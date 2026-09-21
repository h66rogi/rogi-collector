package pipeline

import (
	"context"
	_ "embed"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/h66rogi/rogi-collector/shared/model"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
)

// PublishMetrics holds Prometheus metrics for the publisher.
// It is populated from WorkerMetrics by main to avoid an import cycle
// between the pipeline and internal packages.
type PublishMetrics struct {
	PublishedTotal    *prometheus.CounterVec
	DedupSkippedTotal *prometheus.CounterVec
	ErrorsTotal       *prometheus.CounterVec
	DurationSeconds   prometheus.Histogram
}

const (
	firehoseKey    = "chat:firehose"
	channelMaxLen  = 1000
	firehoseMaxLen = 100000

	// streamTTL applies only to the retained non-SOOP publisher path.
	// SOOP product chat has its own 24-hour/10,000-message policy in product_chat.go.
	streamTTL = 48 * time.Hour
)

func chatStreamKey(platform model.Platform, channelID string) string {
	return fmt.Sprintf("chat:%s:%s", platform, channelID)
}

//go:embed dedup.lua
var dedupLuaScript string

var dedupScript = redis.NewScript(dedupLuaScript)

const dedupTTL = 10 // seconds

func dedupKey(msg model.ChatMessage) string {
	h := xxhash.Sum64String(msg.Raw)
	return fmt.Sprintf("dedup:%s:%s:%016x", msg.Platform, msg.ChannelID, h)
}

// Publisher writes bounded recent chat to Redis Streams. Durable SOOP donations
// are accepted separately by the manager before reaching this chat pipeline.
type Publisher struct {
	rdb     redis.UniversalClient
	metrics *PublishMetrics

	published atomic.Int64
	errors    atomic.Int64
}

// NewPublisher creates a new Publisher backed by the given Redis client.
// metrics is optional; if non-nil, per-platform publish metrics are recorded.
func NewPublisher(rdb redis.UniversalClient, metrics *PublishMetrics) *Publisher {
	return &Publisher{rdb: rdb, metrics: metrics}
}

// Publish routes SOOP chat to the product stream without raw payload or firehose.
// Other platforms retain the original channel/firehose pipeline (MAXLEN ~).
func (p *Publisher) Publish(ctx context.Context, msg model.ChatMessage) error {
	if msg.Platform == model.PlatformSoop {
		return p.publishProductChat(ctx, msg)
	}
	fields := msg.ToStreamFields()

	pipe := p.rdb.Pipeline()

	streamKey := chatStreamKey(msg.Platform, msg.ChannelID)
	pipe.XAdd(ctx, &redis.XAddArgs{
		Stream: streamKey,
		MaxLen: channelMaxLen,
		Approx: true,
		Values: fields,
	})
	pipe.Expire(ctx, streamKey, streamTTL)

	pipe.XAdd(ctx, &redis.XAddArgs{
		Stream: firehoseKey,
		MaxLen: firehoseMaxLen,
		Approx: true,
		Values: fields,
	})

	start := time.Now()
	_, err := pipe.Exec(ctx)
	elapsed := time.Since(start).Seconds()

	if p.metrics != nil {
		p.metrics.DurationSeconds.Observe(elapsed)
	}

	if err != nil {
		p.errors.Add(1)
		if p.metrics != nil {
			p.metrics.ErrorsTotal.WithLabelValues(string(msg.Platform), "redis_exec").Inc()
		}
		return fmt.Errorf("publish message: %w", err)
	}
	p.published.Add(1)
	if p.metrics != nil {
		p.metrics.PublishedTotal.WithLabelValues(string(msg.Platform)).Inc()
	}
	return nil
}

// PublishWithDedup retains raw-hash deduplication for non-SOOP handoff overlap.
// SOOP uses the product chat path without raw-hash deduplication; identical
// observations must not be silently merged.
func (p *Publisher) PublishWithDedup(ctx context.Context, msg model.ChatMessage) error {
	if msg.Platform == model.PlatformSoop {
		return p.publishProductChat(ctx, msg)
	}
	fields := msg.ToStreamFields()

	// Convert fields map to alternating key/value slice for Lua unpack.
	args := make([]interface{}, 0, 4+len(fields)*2)
	args = append(args, dedupTTL, channelMaxLen, firehoseMaxLen, int(streamTTL.Seconds()))
	for k, v := range fields {
		args = append(args, k, v)
	}

	keys := []string{
		dedupKey(msg),
		chatStreamKey(msg.Platform, msg.ChannelID),
		firehoseKey,
	}

	start := time.Now()
	result, err := dedupScript.Run(ctx, p.rdb, keys, args...).Int64()
	elapsed := time.Since(start).Seconds()

	if p.metrics != nil {
		p.metrics.DurationSeconds.Observe(elapsed)
	}

	if err != nil {
		// Dedup script failed — fall back to direct publish (prefer duplicates over loss).
		p.errors.Add(1)
		return p.Publish(ctx, msg)
	}

	if result == 0 {
		// Duplicate detected, skip.
		if p.metrics != nil && p.metrics.DedupSkippedTotal != nil {
			p.metrics.DedupSkippedTotal.WithLabelValues(string(msg.Platform)).Inc()
		}
		return nil
	}

	p.published.Add(1)
	if p.metrics != nil {
		p.metrics.PublishedTotal.WithLabelValues(string(msg.Platform)).Inc()
	}
	return nil
}

// Published returns the total number of successfully published messages.
func (p *Publisher) Published() int64 {
	return p.published.Load()
}

// Errors returns the total number of publish errors.
func (p *Publisher) Errors() int64 {
	return p.errors.Load()
}
