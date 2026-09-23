package archive

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type memoryS3 struct {
	objects map[string][]byte
	corrupt bool
}

func (m *memoryS3) PutObject(_ context.Context, input *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	if m.objects == nil {
		m.objects = map[string][]byte{}
	}
	body, err := io.ReadAll(input.Body)
	if err != nil {
		return nil, err
	}
	m.objects[*input.Key] = body
	return &s3.PutObjectOutput{}, nil
}
func (m *memoryS3) GetObject(_ context.Context, input *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	body, ok := m.objects[*input.Key]
	if !ok {
		return nil, errors.New("object absent")
	}
	copyBody := append([]byte(nil), body...)
	if m.corrupt {
		copyBody[0] ^= 1
	}
	return &s3.GetObjectOutput{Body: io.NopCloser(bytes.NewReader(copyBody))}, nil
}

func TestS3ObjectStoreReadbackAndChecksum(t *testing.T) {
	segment, err := Build([]ChatRow{{SessionID: "00000000-0000-4000-8000-000000000001", Position: 1, EventID: "synthetic", ReceivedAt: time.Now().UTC(), PublicUserID: strings.Repeat("a", 64), Message: "hello"}})
	if err != nil {
		t.Fatal(err)
	}
	client := &memoryS3{}
	store, err := NewS3ObjectStore(client, "disposable-bucket")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.PutVerified(context.Background(), segment); err != nil {
		t.Fatal(err)
	}
	client.corrupt = true
	if _, err := store.GetVerified(context.Background(), segment.ObjectKey(), segment.SHA256); err == nil {
		t.Fatal("corrupt archive object was accepted")
	}
	if _, err := store.PutVerified(context.Background(), segment); err == nil {
		t.Fatal("upload with corrupt readback was accepted")
	}
}
