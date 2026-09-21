package main

import (
	"context"
	"fmt"
	"github.com/h66rogi/rogi-collector/shared/store"
	"github.com/jackc/pgx/v5/pgxpool"
	"log"
	"os"
	"time"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if os.Getenv("DATABASE_URL") == "" {
		log.Fatal("DATABASE_URL required")
	}
	pool, err := pgxpool.New(ctx, os.Getenv("DATABASE_URL"))
	if err != nil {
		log.Fatal("database configuration invalid")
	}
	defer pool.Close()
	next, err := store.NewPgStore(pool).RotateJournal(ctx, os.Getenv("RESTORE_CHANNEL_ID"), os.Getenv("RESTORE_EXPECTED_GENERATION"), os.Getenv("RESTORE_REASON"))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(next)
}
