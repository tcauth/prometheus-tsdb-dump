package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"

	"github.com/ryotarai/prometheus-tsdb-dump/pkg/writer"

	gokitlog "github.com/go-kit/kit/log"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
)

func main() {
	blockPath := flag.String("block", "", "Path to block directory")
	labelKey := flag.String("label-key", "", "")
	labelValue := flag.String("label-value", "", "")
	externalLabels := flag.String("external-labels", "{}", "Labels to be added to dumped result in JSON")
	minTimestamp := flag.Int64("min-timestamp", 0, "min of timestamp of datapoints to be dumped; unix time in msec")
	maxTimestamp := flag.Int64("max-timestamp", math.MaxInt64, "min of timestamp of datapoints to be dumped; unix time in msec")
	format := flag.String("format", "victoriametrics", "")
	awsProfile := flag.String("aws-profile", "", "AWS profile for S3 access")
	flag.Parse()

	if *blockPath == "" {
		log.Fatal("-block argument is required")
	}

	if err := run(*blockPath, *labelKey, *labelValue, *format, *minTimestamp, *maxTimestamp, *externalLabels, *awsProfile); err != nil {
		log.Fatalf("error: %s", err)
	}
}

func run(blockPath string, labelKey string, labelValue string, outFormat string, minTimestamp int64, maxTimestamp int64, externalLabelsJSON string, awsProfile string) error {
	externalLabelsMap := map[string]string{}
	if err := json.NewDecoder(strings.NewReader(externalLabelsJSON)).Decode(&externalLabelsMap); err != nil {
		return errors.Wrap(err, "decode external labels")
	}
	var externalLabels labels.Labels
	for k, v := range externalLabelsMap {
		externalLabels = append(externalLabels, labels.Label{Name: k, Value: v})
	}

	if strings.HasPrefix(blockPath, "s3://") {
		tmpDir, err := downloadS3Block(blockPath, awsProfile)
		if err != nil {
			return errors.Wrap(err, "download S3 block")
		}
		defer os.RemoveAll(tmpDir)
		blockPath = tmpDir
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

func downloadS3Block(s3URL string, profile string) (string, error) {
	u, err := url.Parse(s3URL)
	if err != nil {
		return "", err
	}
	bucket := u.Host
	prefix := strings.TrimPrefix(u.Path, "/")

	sess, err := session.NewSessionWithOptions(session.Options{
		Profile:           profile,
		SharedConfigState: session.SharedConfigEnable,
	})
	if err != nil {
		return "", err
	}
	s3svc := s3.New(sess)
	downloader := s3manager.NewDownloader(sess)

	tmpDir, err := ioutil.TempDir("", "tsdb-block-")
	if err != nil {
		return "", err
	}

	input := &s3.ListObjectsV2Input{Bucket: aws.String(bucket), Prefix: aws.String(prefix)}
	err = s3svc.ListObjectsV2Pages(input, func(page *s3.ListObjectsV2Output, last bool) bool {
		for _, obj := range page.Contents {
			key := *obj.Key
			rel := strings.TrimPrefix(key, prefix)
			rel = strings.TrimLeft(rel, "/")
			localPath := filepath.Join(tmpDir, rel)
			if err := os.MkdirAll(filepath.Dir(localPath), 0755); err != nil {
				return false
			}
			f, err := os.Create(localPath)
			if err != nil {
				return false
			}
			_, err = downloader.Download(f, &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
			f.Close()
			if err != nil {
				return false
			}
		}
		return true
	})
	if err != nil {
		os.RemoveAll(tmpDir)
		return "", err
	}
	return tmpDir, nil
}
