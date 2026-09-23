package archive

import (
	"bytes"
	"strings"
	"testing"
	"time"
)

func TestSegmentDeterminismAndVerification(t *testing.T) {
	const sessionID = "00000000-0000-4000-8000-000000000001"
	rows := []ChatRow{
		{SessionID: sessionID, Position: 3, EventID: "a", ReceivedAt: time.Unix(1, 0).UTC(), PublicUserID: strings.Repeat("a", 64), Message: "안녕"},
		{SessionID: sessionID, Position: 9, EventID: "b", ReceivedAt: time.Unix(2, 0).UTC(), PublicUserID: strings.Repeat("b", 64), Message: "hello"},
	}
	first, err := Build(rows)
	if err != nil {
		t.Fatal(err)
	}
	second, err := Build(rows)
	if err != nil {
		t.Fatal(err)
	}
	if first.SHA256 != second.SHA256 || !bytes.Equal(first.Body, second.Body) {
		t.Fatal("same rows produced different immutable object")
	}
	decoded, err := Decode(first)
	if err != nil || len(decoded) != 2 || decoded[1].Position != 9 {
		t.Fatalf("decode: rows=%#v err=%v", decoded, err)
	}
	first.Body[5] ^= 1
	if _, err := Decode(first); err == nil {
		t.Fatal("corrupted object was accepted")
	}
}

func TestSegmentRejectsOrderAndMetadataMismatch(t *testing.T) {
	row := ChatRow{SessionID: "00000000-0000-4000-8000-000000000001", Position: 2, EventID: "a", ReceivedAt: time.Now()}
	if _, err := Build([]ChatRow{row, row}); err == nil {
		t.Fatal("duplicate position accepted")
	}
	segment, err := Build([]ChatRow{row})
	if err != nil {
		t.Fatal(err)
	}
	segment.Count = 2
	if _, err := Decode(segment); err == nil {
		t.Fatal("incorrect index count accepted")
	}
}
