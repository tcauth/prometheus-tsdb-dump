package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math"
	"net/url"
	"os"
	"path"
	"strings"

	"github.com/ryotarai/prometheus-tsdb-dump/pkg/writer"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	gokitlog "github.com/go-kit/kit/log"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/index"
)

func main() {
	blockPath := flag.String("block", "", "Path to block directory")
	labelKey := flag.String("label-key", "", "")
	labelValue := flag.String("label-value", "", "")
	externalLabels := flag.String("external-labels", "{}", "Labels to be added to dumped result in JSON")
	minTimestamp := flag.Int64("min-timestamp", 0, "min of timestamp of datapoints to be dumped; unix time in msec")
	maxTimestamp := flag.Int64("max-timestamp", math.MaxInt64, "min of timestamp of datapoints to be dumped; unix time in msec")
	format := flag.String("format", "victoriametrics", "")
	dumpIndex := flag.Bool("dump-index", false, "Dump index information in JSON and exit")
	awsProfile := flag.String("aws-profile", "", "AWS profile to use when accessing S3")
	flag.Parse()

	if *blockPath == "" {
		log.Fatal("-block argument is required")
	}

	if *dumpIndex {
		if err := runDumpIndex(*blockPath, *labelKey, *labelValue, *awsProfile); err != nil {
			log.Fatalf("error: %s", err)
		}
		return
	}

	if err := run(*blockPath, *labelKey, *labelValue, *format, *minTimestamp, *maxTimestamp, *externalLabels); err != nil {
		log.Fatalf("error: %s", err)
	}
}

func run(blockPath string, labelKey string, labelValue string, outFormat string, minTimestamp int64, maxTimestamp int64, externalLabelsJSON string) error {
	externalLabelsMap := map[string]string{}
	if err := json.NewDecoder(strings.NewReader(externalLabelsJSON)).Decode(&externalLabelsMap); err != nil {
		return errors.Wrap(err, "decode external labels")
	}
	var externalLabels labels.Labels
	for k, v := range externalLabelsMap {
		externalLabels = append(externalLabels, labels.Label{Name: k, Value: v})
	}

	wr, err := writer.NewWriter(outFormat)

	logger := gokitlog.NewLogfmtLogger(os.Stderr)

	block, err := tsdb.OpenBlock(logger, blockPath, chunkenc.NewPool())
	if err != nil {
		return errors.Wrap(err, "tsdb.OpenBlock")
	}

	indexr, err := block.Index()
	if err != nil {
		return errors.Wrap(err, "block.Index")
	}
	defer indexr.Close()

	chunkr, err := block.Chunks()
	if err != nil {
		return errors.Wrap(err, "block.Chunks")
	}
	defer chunkr.Close()

	postings, err := indexr.Postings(labelKey, labelValue)
	if err != nil {
		return errors.Wrap(err, "indexr.Postings")
	}

	var it chunkenc.Iterator
	for postings.Next() {
		ref := postings.At()
		lset := labels.Labels{}
		chks := []chunks.Meta{}
		if err := indexr.Series(ref, &lset, &chks); err != nil {
			return errors.Wrap(err, "indexr.Series")
		}
		if len(externalLabels) > 0 {
			lset = append(lset, externalLabels...)
		}

		for _, meta := range chks {
			chunk, err := chunkr.Chunk(meta.Ref)
			if err != nil {
				return errors.Wrap(err, "chunkr.Chunk")
			}

			var timestamps []int64
			var values []float64

			it := chunk.Iterator(it)
			for it.Next() {
				t, v := it.At()
				if math.IsNaN(v) {
					continue
				}
				if math.IsInf(v, -1) || math.IsInf(v, 1) {
					continue
				}
				if t < minTimestamp || maxTimestamp < t {
					continue
				}
				timestamps = append(timestamps, t)
				values = append(values, v)
			}
			if it.Err() != nil {
				return errors.Wrap(err, "iterator.Err")
			}

			if len(timestamps) == 0 {
				continue
			}

			if err := wr.Write(&lset, timestamps, values); err != nil {
				return errors.Wrap(err, fmt.Sprintf("Writer.Write(%v, %v, %v)", lset, timestamps, values))
			}
		}
	}

	if postings.Err() != nil {
		return errors.Wrap(err, "postings.Err")
	}

	return nil
}

func runDumpIndex(blockPath string, labelKey string, labelValue string, awsProfile string) error {
	indexr, err := openIndexReader(blockPath, awsProfile)
	if err != nil {
		return err
	}
	defer indexr.Close()

	postings, err := indexr.Postings(labelKey, labelValue)
	if err != nil {
		return errors.Wrap(err, "indexr.Postings")
	}

	enc := json.NewEncoder(os.Stdout)
	for postings.Next() {
		ref := postings.At()
		lset := labels.Labels{}
		chks := []chunks.Meta{}
		if err := indexr.Series(ref, &lset, &chks); err != nil {
			return errors.Wrap(err, "indexr.Series")
		}

		metric := map[string]string{}
		for _, l := range lset {
			metric[l.Name] = l.Value
		}

		type meta struct {
			Ref     uint64 `json:"ref"`
			MinTime int64  `json:"minTime"`
			MaxTime int64  `json:"maxTime"`
		}

		metas := make([]meta, 0, len(chks))
		for _, m := range chks {
			metas = append(metas, meta{Ref: m.Ref, MinTime: m.MinTime, MaxTime: m.MaxTime})
		}

		line := struct {
			Labels map[string]string `json:"labels"`
			Chunks []meta            `json:"chunks"`
		}{Labels: metric, Chunks: metas}

		if err := enc.Encode(line); err != nil {
			return errors.Wrap(err, "encode")
		}
	}

	if postings.Err() != nil {
		return errors.Wrap(postings.Err(), "postings.Err")
	}

	return nil
}

func openIndexReader(blockPath string, awsProfile string) (*index.Reader, error) {
	if strings.HasPrefix(blockPath, "s3://") {
		bucket, key, err := parseS3Path(blockPath)
		if err != nil {
			return nil, err
		}
		var sess *session.Session
		if awsProfile != "" {
			sess, err = session.NewSessionWithOptions(session.Options{
				Profile:           awsProfile,
				SharedConfigState: session.SharedConfigEnable,
			})
		} else {
			sess, err = session.NewSession()
		}
		if err != nil {
			return nil, errors.Wrap(err, "new aws session")
		}
		downloader := s3manager.NewDownloader(sess)
		buf := aws.NewWriteAtBuffer([]byte{})
		_, err = downloader.Download(buf, &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(path.Join(key, "index")),
		})
		if err != nil {
			return nil, errors.Wrap(err, "download index")
		}
		return index.NewReader(byteSlice(buf.Bytes()))
	}
	return index.NewFileReader(path.Join(blockPath, "index"))
}

type byteSlice []byte

func (b byteSlice) Len() int                    { return len(b) }
func (b byteSlice) Range(start, end int) []byte { return b[start:end] }

func parseS3Path(p string) (bucket, key string, err error) {
	u, err := url.Parse(p)
	if err != nil {
		return "", "", err
	}
	if u.Scheme != "s3" {
		return "", "", fmt.Errorf("invalid s3 url: %s", p)
	}
	bucket = u.Host
	key = strings.TrimPrefix(u.Path, "/")
	return bucket, key, nil
}
