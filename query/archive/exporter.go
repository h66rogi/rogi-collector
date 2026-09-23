package archive

import (
	"context"
	"errors"
	"time"
)

type Exporter struct {
	database   *PgArchive
	objects    *S3ObjectStore
	minimumAge time.Duration
}

func NewExporter(database *PgArchive, objects *S3ObjectStore, minimumAge time.Duration) (*Exporter, error) {
	if database == nil || objects == nil || minimumAge < time.Minute {
		return nil, errors.New("archive exporter requires database, object store and minimum age")
	}
	return &Exporter{database: database, objects: objects, minimumAge: minimumAge}, nil
}

// RunOnce moves at most one immutable segment. A failed upload leaves hot rows
// untouched. The object key is deterministic, so an orphan created before a
// failed index commit can be safely overwritten on the next attempt.
func (e *Exporter) RunOnce(ctx context.Context) (bool, error) {
	rows, err := e.database.NextBatch(ctx, time.Now().Add(-e.minimumAge), MaxSegmentRows)
	if err != nil || len(rows) == 0 {
		return false, err
	}
	var segment Segment
	for {
		segment, err = Build(rows)
		if errors.Is(err, ErrSegmentTooLarge) && len(rows) > 1 {
			rows = rows[:len(rows)/2]
			continue
		}
		if err != nil {
			return false, err
		}
		break
	}
	verified, err := e.objects.PutVerified(ctx, segment)
	if err != nil {
		return false, err
	}
	if err := e.database.CommitSegment(ctx, verified); err != nil {
		return false, err
	}
	return true, nil
}
