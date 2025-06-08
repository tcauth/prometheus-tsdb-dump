package chunkreader

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"path"

	"github.com/aws/aws-sdk-go-v2/aws"
	manager "github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
)

// s3ChunkReader implements tsdb.ChunkReader for blocks stored in S3.
type S3ChunkReader struct {
	downloader *manager.Downloader
	bucket     string
	prefix     string
}

func NewS3ChunkReader(cli *s3.Client, bucket, prefix string) *S3ChunkReader {
	return &S3ChunkReader{
		downloader: manager.NewDownloader(cli),
		bucket:     bucket,
		prefix:     prefix,
	}
}

func (r *S3ChunkReader) Close() error { return nil }

func (r *S3ChunkReader) Chunk(ref uint64) (chunkenc.Chunk, error) {
	segment := int(ref >> 32)
	offset := int((ref << 32) >> 32)
	objKey := path.Join(r.prefix, "chunks", fmt.Sprintf("%06d", segment))

	// First fetch header to determine chunk length.
	headerRange := fmt.Sprintf("bytes=%d-%d", offset, offset+chunks.MaxChunkLengthFieldSize+chunks.ChunkEncodingSize-1)
	buf := manager.NewWriteAtBuffer([]byte{})
	_, err := r.downloader.Download(context.Background(), buf, &s3.GetObjectInput{
		Bucket: aws.String(r.bucket),
		Key:    aws.String(objKey),
		Range:  aws.String(headerRange),
	})
	if err != nil {
		return nil, err
	}
	header := buf.Bytes()
	if len(header) < chunks.MaxChunkLengthFieldSize {
		return nil, fmt.Errorf("short header")
	}
	chkDataLen, n := binary.Uvarint(header)
	if n <= 0 {
		return nil, fmt.Errorf("invalid header")
	}
	total := n + chunks.ChunkEncodingSize + int(chkDataLen) + crc32.Size
	// Fetch whole chunk
	chunkRange := fmt.Sprintf("bytes=%d-%d", offset, offset+total-1)
	buf = manager.NewWriteAtBuffer([]byte{})
	_, err = r.downloader.Download(context.Background(), buf, &s3.GetObjectInput{
		Bucket: aws.String(r.bucket),
		Key:    aws.String(objKey),
		Range:  aws.String(chunkRange),
	})
	if err != nil {
		return nil, err
	}
	data := buf.Bytes()
	if len(data) < total {
		return nil, fmt.Errorf("short chunk data")
	}
	enc := data[n]
	chkDataStart := n + chunks.ChunkEncodingSize
	chkDataEnd := chkDataStart + int(chkDataLen)
	crcStart := chkDataEnd
	crcEnd := crcStart + crc32.Size
	if crcEnd > len(data) {
		return nil, fmt.Errorf("invalid chunk length")
	}
	sum := data[crcStart:crcEnd]
	crc := crc32.New(crc32.MakeTable(crc32.Castagnoli))
	if _, err := crc.Write(data[n:chkDataEnd]); err != nil {
		return nil, err
	}
	if !bytes.Equal(crc.Sum(nil), sum) {
		return nil, fmt.Errorf("checksum mismatch")
	}
	return chunkenc.FromData(chunkenc.Encoding(enc), data[chkDataStart:chkDataEnd])
}

// LocalChunkReader reads chunks from local directory.
type LocalChunkReader struct {
	dir string
}

func NewLocalChunkReader(dir string) *LocalChunkReader {
	return &LocalChunkReader{dir: dir}
}

func (r *LocalChunkReader) Close() error { return nil }

func (r *LocalChunkReader) Chunk(ref uint64) (chunkenc.Chunk, error) {
	segment := int(ref >> 32)
	offset := int((ref << 32) >> 32)
	filePath := path.Join(r.dir, fmt.Sprintf("%06d", segment))

	f, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	header := make([]byte, chunks.MaxChunkLengthFieldSize)
	if _, err := f.ReadAt(header, int64(offset)); err != nil {
		return nil, err
	}
	chkDataLen, n := binary.Uvarint(header)
	if n <= 0 {
		return nil, fmt.Errorf("invalid header")
	}

	total := n + chunks.ChunkEncodingSize + int(chkDataLen) + crc32.Size
	buf := make([]byte, total)
	if _, err := f.ReadAt(buf, int64(offset)); err != nil {
		return nil, err
	}

	enc := buf[n]
	chkDataStart := n + chunks.ChunkEncodingSize
	chkDataEnd := chkDataStart + int(chkDataLen)
	sum := buf[chkDataEnd : chkDataEnd+crc32.Size]
	crc := crc32.New(crc32.MakeTable(crc32.Castagnoli))
	if _, err := crc.Write(buf[n:chkDataEnd]); err != nil {
		return nil, err
	}
	if !bytes.Equal(crc.Sum(nil), sum) {
		return nil, fmt.Errorf("checksum mismatch")
	}
	return chunkenc.FromData(chunkenc.Encoding(enc), buf[chkDataStart:chkDataEnd])
}
