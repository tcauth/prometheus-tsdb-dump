package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math"
	"net/url"
	"os"
	"path"
	"path/filepath"
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
	output := flag.String("output", "", "File to write output to instead of stdout")
	flag.Parse()

	labelValues := parseLabelValues(*labelValue)

	if *blockPath == "" {
		log.Fatal("-block argument is required")
	}

	var out io.Writer = os.Stdout
	if *output != "" {
		f, err := os.Create(*output)
		if err != nil {
			log.Fatalf("error: %s", err)
		}
		defer f.Close()
		out = f
	}

	if *dumpIndex {
		if err := runDumpIndex(*blockPath, *labelKey, labelValues, *awsProfile, out); err != nil {
			log.Fatalf("error: %s", err)
		}
		return
	}

	if err := run(*blockPath, *labelKey, labelValues, *format, *minTimestamp, *maxTimestamp, *externalLabels, *awsProfile, out); err != nil {
		log.Fatalf("error: %s", err)
	}
}

func run(blockPath string, labelKey string, labelValues []string, outFormat string, minTimestamp int64, maxTimestamp int64, externalLabelsJSON string, awsProfile string, out io.Writer) error {
	externalLabelsMap := map[string]string{}
	if err := json.NewDecoder(strings.NewReader(externalLabelsJSON)).Decode(&externalLabelsMap); err != nil {
		return errors.Wrap(err, "decode external labels")
	}
	var externalLabels labels.Labels
	for k, v := range externalLabelsMap {
		externalLabels = append(externalLabels, labels.Label{Name: k, Value: v})
	}

	wr, err := writer.NewWriter(outFormat, out)

	logger := gokitlog.NewLogfmtLogger(os.Stderr)

	block, cleanup, err := openBlock(blockPath, awsProfile, logger)
	if err != nil {
		return errors.Wrap(err, "open block")
	}
	defer cleanup()

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

	var it chunkenc.Iterator
	for _, val := range labelValues {
		postings, err := indexr.Postings(labelKey, val)
		if err != nil {
			return errors.Wrap(err, "indexr.Postings")
		}

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
			return errors.Wrap(postings.Err(), "postings.Err")
		}
	}

	return nil
}

func runDumpIndex(blockPath string, labelKey string, labelValues []string, awsProfile string, out io.Writer) error {
	indexr, err := openIndexReader(blockPath, awsProfile)
	if err != nil {
		return err
	}
	defer indexr.Close()

	enc := json.NewEncoder(out)

	for _, val := range labelValues {
		postings, err := indexr.Postings(labelKey, val)
		if err != nil {
			return errors.Wrap(err, "indexr.Postings")
		}

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
	}

	return nil
}

func openBlock(blockPath string, awsProfile string, logger gokitlog.Logger) (*tsdb.Block, func(), error) {
	if strings.HasPrefix(blockPath, "s3://") {
		bucket, key, err := parseS3Path(blockPath)
		if err != nil {
			return nil, nil, err
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
			return nil, nil, errors.Wrap(err, "new aws session")
		}

		tmpDir, err := ioutil.TempDir("", "tsdb-block-")
		if err != nil {
			return nil, nil, errors.Wrap(err, "create temp dir")
		}

		if err := downloadS3Block(sess, bucket, key, tmpDir); err != nil {
			os.RemoveAll(tmpDir)
			return nil, nil, err
		}

		b, err := tsdb.OpenBlock(logger, tmpDir, chunkenc.NewPool())
		if err != nil {
			os.RemoveAll(tmpDir)
			return nil, nil, err
		}

		cleanup := func() {
			b.Close()
			os.RemoveAll(tmpDir)
		}

		return b, cleanup, nil
	}

	b, err := tsdb.OpenBlock(logger, blockPath, chunkenc.NewPool())
	if err != nil {
		return nil, nil, err
	}
	cleanup := func() { b.Close() }
	return b, cleanup, nil
}

func downloadS3Block(sess *session.Session, bucket, key, dest string) error {
	cli := s3.New(sess)
	downloader := s3manager.NewDownloader(sess)

	prefix := path.Clean(key) + "/"
	token := (*string)(nil)
	for {
		out, err := cli.ListObjectsV2(&s3.ListObjectsV2Input{
			Bucket:            aws.String(bucket),
			Prefix:            aws.String(prefix),
			ContinuationToken: token,
		})
		if err != nil {
			return errors.Wrap(err, "list objects")
		}
		for _, obj := range out.Contents {
			if strings.HasSuffix(*obj.Key, "/") {
				continue
			}
			rel := strings.TrimPrefix(*obj.Key, prefix)
			localPath := filepath.Join(dest, rel)
			if err := os.MkdirAll(filepath.Dir(localPath), 0755); err != nil {
				return err
			}
			f, err := os.Create(localPath)
			if err != nil {
				return err
			}
			if _, err := downloader.Download(f, &s3.GetObjectInput{
				Bucket: aws.String(bucket),
				Key:    obj.Key,
			}); err != nil {
				f.Close()
				return errors.Wrap(err, "download object")
			}
			f.Close()
		}
		if out.NextContinuationToken == nil {
			break
		}
		token = out.NextContinuationToken
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

func parseLabelValues(v string) []string {
	if v == "" {
		return nil
	}
	parts := strings.Split(v, ",")
	res := make([]string, 0, len(parts))
	for _, p := range parts {
		if s := strings.TrimSpace(p); s != "" {
			res = append(res, s)
		}
	}
	return res
}
