package chunkreader

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/s3"
)

type mockS3 struct {
	lastRange string
	data      []byte
}

func (m *mockS3) HeadObjectWithContext(ctx aws.Context, in *s3.HeadObjectInput, opts ...request.Option) (*s3.HeadObjectOutput, error) {
	return &s3.HeadObjectOutput{ContentLength: aws.Int64(int64(len(m.data)))}, nil
}

func (m *mockS3) GetObjectWithContext(ctx aws.Context, in *s3.GetObjectInput, opts ...request.Option) (*s3.GetObjectOutput, error) {
	m.lastRange = aws.StringValue(in.Range)
	start, end := 0, len(m.data)
	if rng := aws.StringValue(in.Range); rng != "" {
		fmt.Sscanf(rng, "bytes=%d-%d", &start, &end)
		end++
	}
	return &s3.GetObjectOutput{Body: ioutil.NopCloser(bytes.NewReader(m.data[start:end]))}, nil
}

func TestS3ByteSliceRange(t *testing.T) {
	data := []byte("abcdefghijklmnopqrstuvwxyz")
	mock := &mockS3{data: data}
	bs := &s3ByteSlice{cli: mock, bucket: "b", key: "k", size: len(data)}

	got := bs.Range(3, 8)
	if string(got) != string(data[3:8]) {
		t.Fatalf("expected %s, got %s", data[3:8], got)
	}
	if mock.lastRange != "bytes=3-7" {
		t.Fatalf("unexpected range header %s", mock.lastRange)
	}
}
