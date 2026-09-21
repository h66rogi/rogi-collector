// Package testutil creates isolated schemas only when an explicit test DSN is set.
package testutil

import (
	"context"
	"github.com/google/uuid"
	"github.com/h66rogi/rogi-collector/shared/migrations"
	"github.com/h66rogi/rogi-collector/shared/store"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"os"
	"testing"
)

func Collector(t testing.TB) (*store.PgStore, *pgxpool.Pool, store.OwnerGrant) {
	t.Helper()
	dsn := os.Getenv("COLLECTOR_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("COLLECTOR_TEST_DATABASE_URL not set")
	}
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	schema := "collector_test_" + uuid.NewString()[:8]
	quoted := pgx.Identifier{schema}.Sanitize()
	if _, err = admin.Exec(ctx, "CREATE SCHEMA "+quoted); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = admin.Exec(context.Background(), "DROP SCHEMA "+quoted+" CASCADE"); admin.Close() })
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	config.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if err = migrations.Apply(ctx, pool); err != nil {
		t.Fatal(err)
	}
	if err = migrations.Apply(ctx, pool); err != nil {
		t.Fatal(err)
	}
	pg := store.NewPgStore(pool)
	pg.SetCollectionChannel("fixture_channel")
	if err = pg.EnsureCollection(ctx, "fixture_channel"); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `INSERT INTO workers(id) VALUES('fixture_worker'); INSERT INTO live_channels(platform,channel_id,status,worker_id) VALUES('soop','fixture_channel','live','fixture_worker')`); err != nil {
		t.Fatal(err)
	}
	grant, err := pg.AcquireCollection(ctx, "fixture_channel", "fixture_worker")
	if err != nil {
		t.Fatal(err)
	}
	return pg, pool, grant
}
