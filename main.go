package main

import (
	"context"
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
	"time"

	"github.com/ryotarai/prometheus-tsdb-dump/pkg/chunkreader"
	"github.com/ryotarai/prometheus-tsdb-dump/pkg/writer"

	"errors"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	gokitlog "github.com/go-kit/kit/log"
	pkgerrors "github.com/pkg/errors"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/index"
)

const s3DownloadTimeout = 5 * time.Minute

func main() {
	blockPath := flag.String("block", "", "Path to block directory")
	labelKey := flag.String("label-key", "", "")
	labelValue := flag.String("label-value", "", "")
	externalLabels := flag.String("external-labels", "{}", "Labels to be added to dumped result in JSON")
	metricName := flag.String("metric-name", "", "Only dump series for this metric (__name__)")
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
		if err := runDumpIndex(*blockPath, *labelKey, labelValues, *metricName, *awsProfile, out); err != nil {
			log.Fatalf("error: %s", err)
		}
		return
	}

	if err := run(*blockPath, *labelKey, labelValues, *metricName, *format, *minTimestamp, *maxTimestamp, *externalLabels, *awsProfile, out); err != nil {
		log.Fatalf("error: %s", err)
	}
}

func run(blockPath string, labelKey string, labelValues []string, metricName string, outFormat string, minTimestamp int64, maxTimestamp int64, externalLabelsJSON string, awsProfile string, out io.Writer) error {
	externalLabelsMap := map[string]string{}
	if err := json.NewDecoder(strings.NewReader(externalLabelsJSON)).Decode(&externalLabelsMap); err != nil {
		return pkgerrors.Wrap(err, "decode external labels")
	}
	var externalLabels labels.Labels
	for k, v := range externalLabelsMap {
		externalLabels = append(externalLabels, labels.Label{Name: k, Value: v})
	}

	wr, err := writer.NewWriter(outFormat, out)

	indexr, err := openIndexReader(blockPath, awsProfile)
	if err != nil {
		return pkgerrors.Wrap(err, "open index")
	}
	defer indexr.Close()

	var chunkr tsdb.ChunkReader
	if strings.HasPrefix(blockPath, "s3://") {
		bucket, key, err := parseS3Path(blockPath)
		if err != nil {
			return pkgerrors.Wrap(err, "parse s3 path")
		}
		sess, err := newAWSSession(bucket, awsProfile)
		if err != nil {
			return pkgerrors.Wrap(err, "new aws session")
		}
		chunkr = chunkreader.NewS3ChunkReader(sess, bucket, key)
	} else {
		chunkr = chunkreader.NewLocalChunkReader(path.Join(blockPath, "chunks"))
	}
	defer chunkr.Close()

	// default to all postings if no label key provided
	if labelKey == "" {
		allKey, allValue := index.AllPostingsKey()
		labelKey = allKey
		labelValues = []string{allValue}
	}

	var metricPostings index.Postings
	if metricName != "" {
		var err error
		metricPostings, err = indexr.Postings(labels.MetricName, metricName)
		if err != nil {
			return pkgerrors.Wrap(err, "indexr.Postings metric")
		}
	}

	var it chunkenc.Iterator
	for _, val := range labelValues {
		postings, err := indexr.Postings(labelKey, val)
		if err != nil {
			return pkgerrors.Wrap(err, "indexr.Postings")
		}
		if metricPostings != nil {
			postings = index.Intersect(postings, metricPostings)
		}

		for postings.Next() {
			ref := postings.At()
			lset := labels.Labels{}
			chks := []chunks.Meta{}
			if err := indexr.Series(ref, &lset, &chks); err != nil {
				return pkgerrors.Wrap(err, "indexr.Series")
			}
			if len(externalLabels) > 0 {
				lset = append(lset, externalLabels...)
			}

			for _, meta := range chks {
				chunk, err := chunkr.Chunk(meta.Ref)
				if err != nil {
					return pkgerrors.Wrap(err, "chunkr.Chunk")
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
					return pkgerrors.Wrap(err, "iterator.Err")
				}

				if len(timestamps) == 0 {
					continue
				}

				if err := wr.Write(&lset, timestamps, values); err != nil {
					return pkgerrors.Wrap(err, fmt.Sprintf("Writer.Write(%v, %v, %v)", lset, timestamps, values))
				}
			}
		}

		if postings.Err() != nil {
			return pkgerrors.Wrap(postings.Err(), "postings.Err")
		}
	}

	return nil
}

func runDumpIndex(blockPath string, labelKey string, labelValues []string, metricName string, awsProfile string, out io.Writer) error {
	indexr, err := openIndexReader(blockPath, awsProfile)
	if err != nil {
		return err
	}
	defer indexr.Close()

	enc := json.NewEncoder(out)

	// default to all postings if no label key provided
	if labelKey == "" {
		allKey, allValue := index.AllPostingsKey()
		labelKey = allKey
		labelValues = []string{allValue}
	}

	var metricPostings index.Postings
	if metricName != "" {
		metricPostings, err = indexr.Postings(labels.MetricName, metricName)
		if err != nil {
			return pkgerrors.Wrap(err, "indexr.Postings metric")
		}
	}

	for _, val := range labelValues {
		postings, err := indexr.Postings(labelKey, val)
		if err != nil {
			return pkgerrors.Wrap(err, "indexr.Postings")
		}
		if metricPostings != nil {
			postings = index.Intersect(postings, metricPostings)
		}

		for postings.Next() {
			ref := postings.At()
			lset := labels.Labels{}
			chks := []chunks.Meta{}
			if err := indexr.Series(ref, &lset, &chks); err != nil {
				return pkgerrors.Wrap(err, "indexr.Series")
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
				return pkgerrors.Wrap(err, "encode")
			}
		}

		if postings.Err() != nil {
			return pkgerrors.Wrap(postings.Err(), "postings.Err")
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
		sess, err := newAWSSession(bucket, awsProfile)
		if err != nil {
			return nil, nil, pkgerrors.Wrap(err, "new aws session")
		}

		tmpDir, err := ioutil.TempDir("", "tsdb-block-")
		if err != nil {
			return nil, nil, pkgerrors.Wrap(err, "create temp dir")
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

	ctx, cancel := context.WithTimeout(context.Background(), s3DownloadTimeout)
	defer cancel()

	prefix := path.Clean(key) + "/"
	token := (*string)(nil)
	for {
		out, err := cli.ListObjectsV2WithContext(ctx, &s3.ListObjectsV2Input{
			Bucket:            aws.String(bucket),
			Prefix:            aws.String(prefix),
			ContinuationToken: token,
		})
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
				return err
			}
			return pkgerrors.Wrap(err, "list objects")
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
			if _, err := downloader.DownloadWithContext(ctx, f, &s3.GetObjectInput{
				Bucket: aws.String(bucket),
				Key:    obj.Key,
			}); err != nil {
				f.Close()
				if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
					return err
				}
				return pkgerrors.Wrap(err, "download object")
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
		sess, err := newAWSSession(bucket, awsProfile)
		if err != nil {
			return nil, pkgerrors.Wrap(err, "new aws session")
		}
		cli := s3.New(sess)
		bs, err := chunkreader.NewS3ByteSlice(cli, bucket, path.Join(key, "index"))
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
				return nil, err
			}
			return nil, pkgerrors.Wrap(err, "prepare index slice")
		}
		return index.NewReader(bs)
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

func newAWSSession(bucket, profile string) (*session.Session, error) {
	var sess *session.Session
	var err error
	if profile != "" {
		sess, err = session.NewSessionWithOptions(session.Options{
			Profile:           profile,
			SharedConfigState: session.SharedConfigEnable,
		})
	} else {
		sess, err = session.NewSession()
	}
	if err != nil {
		return nil, err
	}
	if aws.StringValue(sess.Config.Region) == "" {
		region, err := s3manager.GetBucketRegion(aws.BackgroundContext(), sess, bucket, "us-east-1")
		if err != nil {
			return nil, err
		}
		sess.Config.Region = aws.String(region)
	}
	return sess, nil
}
