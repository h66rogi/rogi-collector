package main

import (
	"context"
	"github.com/h66rogi/rogi-collector/shared/migrations"
	"github.com/h66rogi/rogi-collector/shared/runtimeenv"
	"github.com/jackc/pgx/v5/pgxpool"
	"log"
	"os"
	"time"
)

func main() {
	if err := runtimeenv.Load(); err != nil {
		log.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		log.Fatal("DATABASE_URL required")
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		log.Fatal("database configuration invalid")
	}
	defer pool.Close()
	if err = migrations.Apply(ctx, pool); err != nil {
		log.Fatal(err)
	}
	log.Print("collector application migrations applied")
}
