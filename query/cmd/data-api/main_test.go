package main

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func TestPublicDatabaseRoleRejectsWritableLogin(t *testing.T) {
	readonlyDSN := os.Getenv("PUBLIC_API_TEST_DATABASE_URL")
	writableDSN := os.Getenv("COLLECTOR_TEST_DATABASE_URL")
	if readonlyDSN == "" || writableDSN == "" {
		t.Skip("PostgreSQL integration URLs not configured")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, test := range []struct {
		name string
		dsn  string
		ok   bool
	}{
		{"dedicated read-only login", readonlyDSN, true},
		{"existing writable login", writableDSN, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			pool, err := pgxpool.New(ctx, test.dsn)
			if err != nil {
				t.Fatal(err)
			}
			defer pool.Close()
			err = verifyPublicDatabaseRole(ctx, pool)
			if (err == nil) != test.ok {
				t.Fatalf("unexpected role verification result: %v", err)
			}
		})
	}
}
