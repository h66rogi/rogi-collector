package archive

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"regexp"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type S3Client interface {
	PutObject(context.Context, *s3.PutObjectInput, ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	GetObject(context.Context, *s3.GetObjectInput, ...func(*s3.Options)) (*s3.GetObjectOutput, error)
}

// S3ObjectStore works with an S3 API client, including a compatible object
// store configured through the SDK. It verifies uploaded bytes by reading
// the bounded object back before the PostgreSQL index can be committed.
type S3ObjectStore struct {
	client S3Client
	bucket string
}

func (s *S3ObjectStore) Bucket() string { return s.bucket }

// VerifiedSegment can only be issued after an object has been uploaded and
// read back through S3ObjectStore. Its fields are private to this package.
type VerifiedSegment struct {
	segment Segment
	bucket  string
}

func NewS3ObjectStore(client S3Client, bucket string) (*S3ObjectStore, error) {
	if client == nil || bucket == "" {
		return nil, errors.New("S3 client and archive bucket required")
	}
	return &S3ObjectStore{client: client, bucket: bucket}, nil
}

var objectKeyPattern = regexp.MustCompile(`^v1/soop/h66rogi/[0-9a-f-]{36}/[0-9]+-[0-9]+-[0-9a-f]{64}\.jsonl\.gz$`)

func validObject(key, checksum string) bool {
	if !objectKeyPattern.MatchString(key) || !strings.HasSuffix(key, "-"+checksum+".jsonl.gz") || len(checksum) != 64 {
		return false
	}
	decoded, err := hex.DecodeString(checksum)
	return err == nil && len(decoded) == 32
}

func (s *S3ObjectStore) PutVerified(ctx context.Context, segment Segment) (VerifiedSegment, error) {
	if !validObject(segment.ObjectKey(), segment.SHA256) || len(segment.Body) == 0 || len(segment.Body) > MaxSegmentGzipBytes {
		return VerifiedSegment{}, errors.New("invalid archive object")
	}
	sum := sha256.Sum256(segment.Body)
	if hex.EncodeToString(sum[:]) != segment.SHA256 {
		return VerifiedSegment{}, errors.New("archive object checksum mismatch")
	}
	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(s.bucket), Key: aws.String(segment.ObjectKey()), Body: bytes.NewReader(segment.Body),
		ContentLength: aws.Int64(int64(len(segment.Body))), ContentType: aws.String("application/x-ndjson"), ContentEncoding: aws.String("gzip"),
	})
	if err != nil {
		return VerifiedSegment{}, err
	}
	readback, err := s.GetVerified(ctx, segment.ObjectKey(), segment.SHA256)
	if err != nil {
		return VerifiedSegment{}, err
	}
	if !bytes.Equal(readback, segment.Body) {
		return VerifiedSegment{}, errors.New("archive object readback differs")
	}
	segment.Body = append([]byte(nil), segment.Body...)
	return VerifiedSegment{segment: segment, bucket: s.bucket}, nil
}

func (s *S3ObjectStore) GetVerified(ctx context.Context, key, checksum string) ([]byte, error) {
	if !validObject(key, checksum) {
		return nil, errors.New("invalid archive object reference")
	}
	response, err := s.client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(key)})
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, MaxSegmentGzipBytes+1))
	if err != nil {
		return nil, err
	}
	if len(body) == 0 || len(body) > MaxSegmentGzipBytes {
		return nil, errors.New("archive object size limit")
	}
	sum := sha256.Sum256(body)
	if hex.EncodeToString(sum[:]) != checksum {
		return nil, errors.New("archive object checksum mismatch")
	}
	return body, nil
}
