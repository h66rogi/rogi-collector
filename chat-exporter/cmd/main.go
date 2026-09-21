package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress/zstd"
	"golang.org/x/sync/errgroup"
)

// Row mirrors the chat_messages ClickHouse table.
// Default compression is zstd (set on writer); per-column dict tag enables
// dictionary encoding for low-cardinality columns to shrink Parquet output.
type Row struct {
	MessageID    string    `parquet:"message_id"`
	Timestamp    time.Time `parquet:"timestamp,timestamp(millisecond)"`
	Platform     string    `parquet:"platform,dict"`
	ChannelID    string    `parquet:"channel_id,dict"`
	StreamerName string    `parquet:"streamer_name,dict"`
	UserID       string    `parquet:"user_id"`
	Nickname     string    `parquet:"nickname"`
	MessageType  string    `parquet:"message_type,dict"`
	MessageText  string    `parquet:"message_text"`
	Amount       float32   `parquet:"amount"`
	Currency     string    `parquet:"currency,dict"`
	AmountKRW    int64     `parquet:"amount_krw"`
	WorkerID     string    `parquet:"worker_id,dict"`
	IngestedAt   time.Time `parquet:"ingested_at,timestamp(millisecond)"`
}

type config struct {
	Day                  time.Time
	Hour                 int // 0..23; -1 means full day (avoid on large datasets due to GROUP BY memory)
	CHAddr               string
	CHDatabase           string
	CHUser               string
	CHPassword           string
	Bucket               string
	KeyPrefix            string
	AWSRegion            string
	AllowEmpty           bool
	DryRun               bool
	QueryTimeout         time.Duration
	UploadTimeout        time.Duration
	BatchSize            int
	RowGroupRows         int
	PartSizeMB           int
	UploadConcurrent     int
	DefaultDayOffsetDays int
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	cfg, err := loadConfig()
	if err != nil {
		logger.Error("config load failed", "error", err)
		os.Exit(2)
	}

	if err := run(context.Background(), logger, cfg); err != nil {
		logger.Error("export failed", "error", err)
		os.Exit(1)
	}
}

func loadConfig() (config, error) {
	cfg := config{
		AWSRegion:        os.Getenv("AWS_REGION"),
		KeyPrefix:        envOr("S3_KEY_PREFIX", "messages"),
		CHDatabase:       envOr("CLICKHOUSE_DATABASE", "default"),
		QueryTimeout:     1 * time.Hour,
		UploadTimeout:    2 * time.Hour,
		BatchSize:        5000,
		RowGroupRows:     500000,
		PartSizeMB:       64,
		UploadConcurrent: 4,
	}

	dayFlag := flag.String("day", "", "UTC date to export, format YYYY-MM-DD (e.g. 2026-02-27); takes precedence over --default-day-offset-days")
	flag.IntVar(&cfg.DefaultDayOffsetDays, "default-day-offset-days", 0, "if --day is empty, default to today-N days in UTC (used by cron schedules; backfill should always pass --day explicitly)")
	flag.IntVar(&cfg.Hour, "hour", -1, "UTC hour 0..23 to export (single-hour partition). -1 = full day (avoid on large datasets).")
	flag.BoolVar(&cfg.AllowEmpty, "allow-empty", false, "allow zero-row export to succeed (writes empty Parquet)")
	flag.BoolVar(&cfg.DryRun, "dry-run", false, "select rows and count but skip Parquet build / S3 upload")
	flag.DurationVar(&cfg.QueryTimeout, "query-timeout", cfg.QueryTimeout, "ClickHouse SELECT timeout")
	flag.DurationVar(&cfg.UploadTimeout, "upload-timeout", cfg.UploadTimeout, "S3 multipart upload timeout (independent of query timeout)")
	flag.IntVar(&cfg.BatchSize, "batch-size", cfg.BatchSize, "rows per Parquet writer batch")
	flag.IntVar(&cfg.RowGroupRows, "row-group-rows", cfg.RowGroupRows, "max rows per Parquet row group")
	flag.IntVar(&cfg.PartSizeMB, "part-size-mb", cfg.PartSizeMB, "S3 multipart upload part size in MiB")
	flag.IntVar(&cfg.UploadConcurrent, "upload-concurrency", cfg.UploadConcurrent, "S3 multipart upload concurrency")
	flag.Parse()

	dayStr := *dayFlag
	if dayStr == "" {
		if cfg.DefaultDayOffsetDays <= 0 {
			return cfg, errors.New("--day is required (or set --default-day-offset-days > 0 for cron schedules)")
		}
		dayStr = time.Now().UTC().AddDate(0, 0, -cfg.DefaultDayOffsetDays).Format("2006-01-02")
	}
	day, err := time.ParseInLocation("2006-01-02", dayStr, time.UTC)
	if err != nil {
		return cfg, fmt.Errorf("invalid day %q: %w", dayStr, err)
	}
	cfg.Day = day

	if cfg.Hour < -1 || cfg.Hour > 23 {
		return cfg, fmt.Errorf("invalid --hour %d: must be -1..23", cfg.Hour)
	}

	cfg.CHAddr = os.Getenv("CLICKHOUSE_ADDR")
	cfg.CHUser = os.Getenv("CLICKHOUSE_USER")
	cfg.CHPassword = os.Getenv("CLICKHOUSE_PASSWORD")
	cfg.Bucket = os.Getenv("S3_BUCKET")

	missing := []string{}
	if cfg.CHAddr == "" {
		missing = append(missing, "CLICKHOUSE_ADDR")
	}
	if cfg.CHUser == "" {
		missing = append(missing, "CLICKHOUSE_USER")
	}
	if cfg.CHPassword == "" {
		missing = append(missing, "CLICKHOUSE_PASSWORD")
	}
	if cfg.Bucket == "" {
		missing = append(missing, "S3_BUCKET")
	}
	if cfg.AWSRegion == "" {
		missing = append(missing, "AWS_REGION")
	}
	if len(missing) > 0 {
		return cfg, fmt.Errorf("required env missing: %v", missing)
	}
	return cfg, nil
}

func run(parentCtx context.Context, logger *slog.Logger, cfg config) error {
	queryCtx, cancelQuery := context.WithTimeout(parentCtx, cfg.QueryTimeout)
	defer cancelQuery()

	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{cfg.CHAddr},
		Auth: clickhouse.Auth{Database: cfg.CHDatabase, Username: cfg.CHUser, Password: cfg.CHPassword},
		Settings: clickhouse.Settings{
			"max_execution_time":                 int64(cfg.QueryTimeout.Seconds()),
			"max_memory_usage":                   int64(8 * 1024 * 1024 * 1024),
			"max_bytes_before_external_group_by": int64(4 * 1024 * 1024 * 1024), // spill argMax hash table to disk above 4 GiB
			"max_block_size":                     int64(65536),
		},
		DialTimeout:     20 * time.Second,
		MaxOpenConns:    2,
		MaxIdleConns:    1,
		ConnMaxLifetime: time.Hour,
	})
	if err != nil {
		return fmt.Errorf("clickhouse open: %w", err)
	}
	defer conn.Close()
	if err := conn.Ping(queryCtx); err != nil {
		return fmt.Errorf("clickhouse ping: %w", err)
	}

	yyyymmdd, err := strconv.ParseUint(cfg.Day.Format("20060102"), 10, 32)
	if err != nil {
		return fmt.Errorf("encode export day: %w", err)
	}
	windowStart := cfg.Day
	windowEnd := cfg.Day.Add(24 * time.Hour)
	if cfg.Hour >= 0 {
		windowStart = cfg.Day.Add(time.Duration(cfg.Hour) * time.Hour)
		windowEnd = windowStart.Add(time.Hour)
	}

	windowLabel := cfg.Day.Format("2006-01-02")
	if cfg.Hour >= 0 {
		windowLabel = fmt.Sprintf("%s hour=%02d", windowLabel, cfg.Hour)
	}

	// Pre-flight count to abort early on zero rows or to log expected scale.
	const countQuery = `
		SELECT count()
		FROM chat_messages
		WHERE toYYYYMMDD(timestamp) = ?
		  AND timestamp >= ?
		  AND timestamp <  ?`
	var rawCount uint64
	if err := conn.QueryRow(queryCtx, countQuery, yyyymmdd, windowStart, windowEnd).Scan(&rawCount); err != nil {
		return fmt.Errorf("count query: %w", err)
	}
	logger.Info("preflight count", "window", windowLabel, "raw_rows", rawCount, "yyyymmdd", yyyymmdd)
	if rawCount == 0 {
		if !cfg.AllowEmpty {
			return fmt.Errorf("zero rows for %s; refusing to upload empty Parquet (set --allow-empty to override)", windowLabel)
		}
		logger.Warn("zero rows; skipping upload (allow-empty=true)", "window", windowLabel)
		return nil
	}
	if cfg.DryRun {
		logger.Info("dry-run; exiting before Parquet build / upload")
		return nil
	}

	// Dedup ReplacingMergeTree by replacement key (platform, channel_id, message_id).
	// argMax(col, ingested_at) selects the column value of the row with the highest
	// ingested_at, matching ReplacingMergeTree's "winner" semantics without FINAL.
	// Range predicate matches partition expression (toYYYYMMDD) and order key prefix
	// for index-friendly scan; no global ORDER BY (writer row groups define layout).
	//
	// Aliases must NOT collide with raw column names: ClickHouse resolves WHERE
	// references to SELECT aliases first, which would surface aggregates in WHERE
	// (error 184). Suffix _v on aggregated columns sidesteps that.
	const selectQuery = `
		SELECT
			argMax(message_id,    ingested_at) AS message_id_v,
			argMax(timestamp,     ingested_at) AS timestamp_v,
			platform,
			channel_id,
			argMax(streamer_name, ingested_at) AS streamer_name_v,
			argMax(user_id,       ingested_at) AS user_id_v,
			argMax(nickname,      ingested_at) AS nickname_v,
			argMax(message_type,  ingested_at) AS message_type_v,
			argMax(message_text,  ingested_at) AS message_text_v,
			argMax(amount,        ingested_at) AS amount_v,
			argMax(currency,      ingested_at) AS currency_v,
			argMax(amount_krw,    ingested_at) AS amount_krw_v,
			argMax(worker_id,     ingested_at) AS worker_id_v,
			max(ingested_at)                   AS ingested_at_v
		FROM chat_messages
		WHERE toYYYYMMDD(timestamp) = ?
		  AND timestamp >= ?
		  AND timestamp <  ?
		GROUP BY platform, channel_id, message_id`

	// Independent upload context: a stalled SELECT must not cancel an in-flight
	// multipart upload (which needs a clean AbortMultipartUpload on failure).
	uploadCtx, cancelUpload := context.WithTimeout(parentCtx, cfg.UploadTimeout)
	defer cancelUpload()

	awsCfg, err := awsconfig.LoadDefaultConfig(uploadCtx, awsconfig.WithRegion(cfg.AWSRegion))
	if err != nil {
		return fmt.Errorf("aws config: %w", err)
	}
	uploader := manager.NewUploader(s3.NewFromConfig(awsCfg), func(u *manager.Uploader) {
		u.PartSize = int64(cfg.PartSizeMB) * 1024 * 1024
		u.Concurrency = cfg.UploadConcurrent
	})

	var key string
	if cfg.Hour >= 0 {
		key = fmt.Sprintf("%s/year=%04d/month=%02d/day=%02d/hour=%02d/data.parquet",
			cfg.KeyPrefix, cfg.Day.Year(), cfg.Day.Month(), cfg.Day.Day(), cfg.Hour)
	} else {
		key = fmt.Sprintf("%s/year=%04d/month=%02d/day=%02d/data.parquet",
			cfg.KeyPrefix, cfg.Day.Year(), cfg.Day.Month(), cfg.Day.Day())
	}

	logger.Info("export starting",
		"window", windowLabel,
		"raw_rows", rawCount,
		"bucket", cfg.Bucket,
		"key", key,
	)
	start := time.Now()

	pr, pw := io.Pipe()

	var dedupRows atomic.Int64

	// Use queryCtx for SELECT goroutine; uploadCtx for S3 goroutine.
	// errgroup propagates cancellation if either side fails.
	g := new(errgroup.Group)

	g.Go(func() error {
		defer pw.Close()

		writer := parquet.NewGenericWriter[Row](pw,
			parquet.Compression(&zstd.Codec{Level: zstd.DefaultLevel}),
			parquet.MaxRowsPerRowGroup(int64(cfg.RowGroupRows)),
			parquet.PageBufferSize(1<<20),
		)

		rows, err := conn.Query(queryCtx, selectQuery, yyyymmdd, windowStart, windowEnd)
		if err != nil {
			return fmt.Errorf("ch query: %w", err)
		}
		defer rows.Close()

		batch := make([]Row, 0, cfg.BatchSize)
		flush := func() error {
			if len(batch) == 0 {
				return nil
			}
			if _, err := writer.Write(batch); err != nil {
				return fmt.Errorf("parquet write: %w", err)
			}
			dedupRows.Add(int64(len(batch)))
			batch = batch[:0]
			return nil
		}

		for rows.Next() {
			var r Row
			if err := rows.Scan(
				&r.MessageID, &r.Timestamp, &r.Platform, &r.ChannelID, &r.StreamerName,
				&r.UserID, &r.Nickname, &r.MessageType, &r.MessageText,
				&r.Amount, &r.Currency, &r.AmountKRW, &r.WorkerID, &r.IngestedAt,
			); err != nil {
				return fmt.Errorf("row scan: %w", err)
			}
			batch = append(batch, r)
			if len(batch) >= cfg.BatchSize {
				if err := flush(); err != nil {
					return err
				}
				if n := dedupRows.Load(); n%500000 == 0 {
					logger.Info("export progress", "rows", n, "elapsed", time.Since(start).Round(time.Second).String())
				}
			}
		}
		if err := rows.Err(); err != nil {
			return fmt.Errorf("rows iter: %w", err)
		}
		if err := flush(); err != nil {
			return err
		}
		if err := writer.Close(); err != nil {
			return fmt.Errorf("parquet close: %w", err)
		}
		return nil
	})

	var uploadOut *manager.UploadOutput
	g.Go(func() error {
		out, err := uploader.Upload(uploadCtx, &s3.PutObjectInput{
			Bucket:      aws.String(cfg.Bucket),
			Key:         aws.String(key),
			Body:        pr,
			ContentType: aws.String("application/vnd.apache.parquet"),
			Metadata: map[string]string{
				"export-day":    cfg.Day.Format("2006-01-02"),
				"export-window": windowLabel,
			},
		})
		if err != nil {
			// Reader side close propagates error to writer goroutine, which then exits.
			if closeErr := pr.CloseWithError(err); closeErr != nil {
				logger.Warn("close upload reader after failure", "error", closeErr)
			}
			return fmt.Errorf("s3 upload: %w", err)
		}
		uploadOut = out
		return nil
	})

	if err := g.Wait(); err != nil {
		return err
	}

	totalRows := dedupRows.Load()
	if totalRows == 0 {
		// preflight count > 0 but dedup result is 0: surface as failure (not allow-empty path).
		return fmt.Errorf("dedup yielded zero rows despite preflight count=%d (data integrity concern)", rawCount)
	}

	logger.Info("export complete",
		"window", windowLabel,
		"raw_rows", rawCount,
		"dedup_rows", totalRows,
		"raw_minus_dedup", int64(rawCount)-totalRows,
		"bucket", cfg.Bucket,
		"key", key,
		"etag", aws.ToString(uploadOut.ETag),
		"location", uploadOut.Location,
		"elapsed", time.Since(start).Round(time.Second).String(),
	)
	return nil
}

func envOr(key, def string) string {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	return v
}
