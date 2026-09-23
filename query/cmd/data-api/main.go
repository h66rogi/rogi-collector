package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/h66rogi/rogi-collector/query/archive"
	"github.com/h66rogi/rogi-collector/query/internal"
	"github.com/h66rogi/rogi-collector/query/publicapi"
	"github.com/h66rogi/rogi-collector/shared/collection"
	"github.com/h66rogi/rogi-collector/shared/runtimeenv"
	"github.com/h66rogi/rogi-collector/shared/store"
	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	if err := run(); err != nil {
		slog.Error("data API stopped", "error", err)
		os.Exit(1)
	}
}

func run() error {
	if err := runtimeenv.Load(); err != nil {
		return err
	}
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)
	channel, err := collection.Channel(os.Getenv("CHANNEL_ALLOWLIST"))
	if err != nil {
		return err
	}
	check := internal.DiscoverBroadcastChecker(os.Getenv("SOOP_DIAGNOSTIC_TOKEN"))
	if check == nil {
		return errors.New("broadcast diagnostics configuration required")
	}
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		return errors.New("DATABASE_URL required")
	}
	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		return errors.New("REDIS_ADDR required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool, err := store.NewPgPoolWithRetry(ctx, databaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()
	if err := verifyPublicDatabaseRole(ctx, pool); err != nil {
		return err
	}
	pg := store.NewPgStore(pool)
	pg.SetCollectionChannel(channel)
	redisClient := store.NewRedisClient(redisAddr)
	defer redisClient.Close()
	redis := store.NewRedisStore(redisClient)
	maxConnections := 32
	if raw := os.Getenv("MAX_PUBLIC_WEBSOCKETS"); raw != "" {
		value, err := strconv.Atoi(raw)
		if err != nil || value < 1 || value > 1000 {
			return errors.New("invalid MAX_PUBLIC_WEBSOCKETS")
		}
		maxConnections = value
	}
	api, err := publicapi.New(channel, pg, redis, check, logger, maxConnections)
	if err != nil {
		return err
	}
	if os.Getenv("PUBLIC_ARCHIVE_HISTORY_ENABLED") == "true" {
		archiveDatabase, err := archive.NewPgArchive(pool, channel)
		if err != nil {
			return err
		}
		objects, err := archive.NewR2ObjectStore(
			os.Getenv("R2_ACCOUNT_ID"), os.Getenv("R2_ARCHIVE_BUCKET"),
			os.Getenv("R2_ACCESS_KEY_ID"), os.Getenv("R2_SECRET_ACCESS_KEY"),
		)
		if err != nil {
			return err
		}
		probeCtx, probeCancel := context.WithTimeout(context.Background(), 5*time.Second)
		err = objects.Probe(probeCtx)
		probeCancel()
		if err != nil {
			return err
		}
		if err := api.EnableHistory(archiveDatabase, objects, []byte(os.Getenv("HISTORY_CURSOR_HMAC_KEY"))); err != nil {
			return err
		}
	}
	server := &http.Server{
		Addr: ":8080", Handler: api.Handler(), ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout: 60 * time.Second, MaxHeaderBytes: 16 << 10,
	}
	server.RegisterOnShutdown(func() { logger.Info("data API shutdown") })
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(stop)
	go func() {
		<-stop
		shutdownCtx, done := context.WithTimeout(context.Background(), 5*time.Second)
		defer done()
		_ = server.Shutdown(shutdownCtx)
	}()
	logger.Info("data API listening", "channel", channel)
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

// A public origin must never inherit the private query service's write grants.
// The deployment provisions a distinct role with only the seven table reads
// needed by status and history handlers.
func verifyPublicDatabaseRole(ctx context.Context, pool *pgxpool.Pool) error {
	var isolated, writable bool
	err := pool.QueryRow(ctx, `SELECT current_user='collector_public_api'
		AND NOT (r.rolsuper OR r.rolcreatedb OR r.rolcreaterole OR r.rolreplication OR r.rolbypassrls)
		AND NOT has_schema_privilege(current_user,'public','CREATE')
		AND NOT has_database_privilege(current_user,current_database(),'CREATE'),
		EXISTS(SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
			WHERE n.nspname='public' AND c.relkind IN ('r','p','v','f')
			AND (has_table_privilege(current_user,c.oid,'INSERT')
				OR has_table_privilege(current_user,c.oid,'UPDATE')
				OR has_table_privilege(current_user,c.oid,'DELETE')
				OR has_table_privilege(current_user,c.oid,'TRUNCATE')))
		FROM pg_roles r WHERE r.rolname=current_user`).Scan(&isolated, &writable)
	if err != nil {
		return err
	}
	if !isolated || writable {
		return errors.New("public API database role has unsafe permissions")
	}
	return nil
}
