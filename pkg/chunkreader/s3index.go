package chunkreader

import (
	"context"
	"fmt"
	"io/ioutil"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

const indexDownloadTimeout = 5 * time.Minute

type s3API interface {
	HeadObject(ctx context.Context, params *s3.HeadObjectInput, optFns ...func(*s3.Options)) (*s3.HeadObjectOutput, error)
	GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error)
}

// s3ByteSlice allows lazy ranged reads of an index file stored in S3.
type s3ByteSlice struct {
	cli    s3API
	bucket string
	key    string
	size   int
}

// NewS3ByteSlice creates a byte slice backed by an S3 object.
// It performs a HEAD request to determine the object's size.
func NewS3ByteSlice(cli s3API, bucket, key string) (*s3ByteSlice, error) {
	ctx, cancel := context.WithTimeout(context.Background(), indexDownloadTimeout)
	defer cancel()

	out, err := cli.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, err
	}
	if out.ContentLength == nil {
		return nil, fmt.Errorf("content length missing for %s/%s", bucket, key)
	}
	return &s3ByteSlice{
		cli:    cli,
		bucket: bucket,
		key:    key,
		size:   int(*out.ContentLength),
	}, nil
}

func (b *s3ByteSlice) Len() int { return b.size }

func (b *s3ByteSlice) Range(start, end int) []byte {
	ctx, cancel := context.WithTimeout(context.Background(), indexDownloadTimeout)
	defer cancel()

	rng := fmt.Sprintf("bytes=%d-%d", start, end-1)
	out, err := b.cli.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(b.bucket),
		Key:    aws.String(b.key),
		Range:  aws.String(rng),
	})
	if err != nil {
		panic(err)
	}
	defer out.Body.Close()

	data, err := ioutil.ReadAll(out.Body)
	if err != nil {
		panic(err)
	}
	return data
}
