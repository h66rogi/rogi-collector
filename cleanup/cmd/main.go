package main

import (
	"context"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/h66rogi/rogi-collector/shared/store"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	redisAddr := os.Getenv("REDIS_ADDR")
	databaseURL := os.Getenv("DATABASE_URL")

	if redisAddr == "" || databaseURL == "" {
		logger.Error("REDIS_ADDR and DATABASE_URL are required")
		os.Exit(1)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	rdb := store.NewRedisClient(redisAddr)
	defer rdb.Close()
	redisStore := store.NewRedisStore(rdb)

	pgPool, err := store.NewPgPool(ctx, databaseURL)
	if err != nil {
		logger.Error("failed to connect to postgres", "error", err)
		os.Exit(1)
	}
	defer pgPool.Close()
	pgStore := store.NewPgStore(pgPool)

	logger.Info("starting stream cleanup job")
	var hadError bool

	// Phase 1: PG-driven cleanup — delete streams for channels ended > 1 hour ago
	cutoff := time.Now().Add(-1 * time.Hour)
	ended, err := pgStore.ListEndedChannelsBefore(ctx, cutoff, 50000)
	if err != nil {
		logger.Error("failed to list ended channels", "error", err)
		hadError = true
	} else if len(ended) > 0 {
		keys := make([]string, len(ended))
		for i, ch := range ended {
			keys[i] = store.ChatStreamKey(string(ch.Platform), ch.ChannelID)
		}
		deleted, err := redisStore.DeleteChatStreams(ctx, keys)
		if err != nil {
			logger.Error("failed to delete ended streams", "error", err)
			hadError = true
		} else {
			logger.Info("phase1: deleted ended channel streams",
				"candidates", len(keys), "deleted", deleted)
		}
	} else {
		logger.Info("phase1: no ended channels to clean up")
	}

	// Phase 2: Scan Redis for truly orphaned streams (not in PG at all).
	// Respects Phase 1's 1-hour grace: ended channels are included in the known set.
	redisKeys, err := redisStore.ScanChatStreamKeys(ctx)
	if err != nil {
		logger.Error("failed to scan redis keys", "error", err)
		os.Exit(1)
	}
	logger.Info("phase2: scanned redis", "total_stream_keys", len(redisKeys))

	if len(redisKeys) > 0 {
		// Build known channel set from ALL PG channels (any status).
		// Only keys completely absent from PG are truly orphaned.
		allKeys, err := pgStore.ListAllKnownChannelKeys(ctx)
		if err != nil {
			logger.Error("failed to list all known channel keys", "error", err)
			os.Exit(1)
		}

		knownSet := make(map[string]struct{}, len(allKeys))
		for _, key := range allKeys {
			knownSet[key] = struct{}{}
		}
		logger.Info("phase2: known channels loaded", "count", len(allKeys))

		// Find truly orphaned keys (in Redis but not in PostgreSQL at all).
		var orphans []string
		for _, key := range redisKeys {
			if _, ok := knownSet[key]; !ok {
				orphans = append(orphans, key)
			}
		}

		if len(orphans) == 0 {
			logger.Info("phase2: no orphan streams found")
		} else {
			deleted, err := redisStore.DeleteChatStreams(ctx, orphans)
			if err != nil {
				logger.Error("failed to delete orphan streams", "error", err)
				hadError = true
			} else {
				logger.Info("phase2: deleted orphan streams",
					"orphans", len(orphans), "deleted", deleted)
			}
		}
	}

	// Phase 3: Memory report
	info, err := rdb.Info(ctx, "memory").Result()
	if err == nil {
		for _, line := range strings.Split(info, "\r\n") {
			if strings.HasPrefix(line, "used_memory_human:") ||
				strings.HasPrefix(line, "maxmemory_human:") {
				logger.Info("redis-memory", "stat", line)
			}
		}
	}

	if hadError {
		logger.Error("stream cleanup job completed with errors")
		os.Exit(1)
	}
	logger.Info("stream cleanup job completed successfully")
}
