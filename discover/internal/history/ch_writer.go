package history

import (
	"context"
	"log/slog"
	"time"

	"github.com/h66rogi/rogi-collector/shared/store"
)

// NewBatchWriter creates a BatchWriter for viewer count rows using the shared generic BatchWriter.
func NewBatchWriter(chStore *store.ClickHouseStore, maxBatch int, flushInterval time.Duration, logger *slog.Logger, metrics *Metrics) *store.BatchWriter[store.ViewerCountRow] {
	var bwMetrics *store.BatchWriterMetrics
	if metrics != nil {
		bwMetrics = &store.BatchWriterMetrics{
			FlushTotal: metrics.CHBatchFlushTotal,
			BatchSize:  metrics.CHBatchSize,
		}
	}
	return store.NewBatchWriter[store.ViewerCountRow](
		func(rows []store.ViewerCountRow) error {
			return chStore.BatchInsertViewerCounts(context.Background(), rows)
		},
		maxBatch,
		flushInterval,
		func(r store.ViewerCountRow) time.Time { return r.ValidFrom },
		logger,
		bwMetrics,
	)
}
