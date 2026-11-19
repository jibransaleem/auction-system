package main

import (
	"context"
	"errors"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"github.com/jibransaleem/auction-system/engine/configuration"
	"github.com/jibransaleem/auction-system/store"
)

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

type row struct {
	bidID, bidder   string
	amount, version int64
	at              time.Time
}

func str(v any) string { s, _ := v.(string); return s }

// parse turns one Redis Stream entry into a row.
func parse(m redis.XMessage) (row, error) {
	amount, err1 := strconv.ParseInt(str(m.Values["amount"]), 10, 64)
	version, err2 := strconv.ParseInt(str(m.Values["version"]), 10, 64)
	ms, err3 := strconv.ParseInt(str(m.Values["at_ms"]), 10, 64)
	if err := errors.Join(err1, err2, err3); err != nil || str(m.Values["bid_id"]) == "" {
		return row{}, errors.New("bad stream entry " + m.ID)
	}
	return row{
		bidID:   str(m.Values["bid_id"]),
		bidder:  str(m.Values["bidder_id"]),
		amount:  amount,
		version: version,
		at:      time.UnixMilli(ms),
	}, nil
}

// insertBatch saves many bids in ONE round trip. Duplicates are ignored.
func insertBatch(ctx context.Context, pool *pgxpool.Pool, auctionID string, rows []row) error {
	if len(rows) == 0 {
		return nil
	}
	n := len(rows)
	ids, aucs, bidders := make([]string, n), make([]string, n), make([]string, n)
	amounts, versions := make([]int64, n), make([]int64, n)
	ats := make([]time.Time, n)
	for i, r := range rows {
		ids[i], aucs[i], bidders[i] = r.bidID, auctionID, r.bidder
		amounts[i], versions[i], ats[i] = r.amount, r.version, r.at
	}
	_, err := pool.Exec(ctx, `
		INSERT INTO bids (bid_id, auction_id, bidder_id, amount, version, at)
		SELECT * FROM unnest($1::text[], $2::text[], $3::text[], $4::bigint[], $5::bigint[], $6::timestamptz[])
		ON CONFLICT (bid_id) DO NOTHING`,
		ids, aucs, bidders, amounts, versions, ats)
	return err
}

// writeWinners: each bidder's best bid, highest first, earlier time wins ties, top STOCK.
func writeWinners(ctx context.Context, pool *pgxpool.Pool, auctionID string, stock int) error {
	_, err := pool.Exec(ctx, `
		INSERT INTO winners (auction_id, rank, bidder_id, amount, bid_at)
		SELECT $1::text, (ROW_NUMBER() OVER (ORDER BY amount DESC, at ASC))::int, bidder_id, amount, at
		FROM (
			SELECT DISTINCT ON (bidder_id) bidder_id, amount, at
			FROM bids WHERE auction_id = $1::text
			ORDER BY bidder_id, amount DESC, at ASC
		) best
		ORDER BY amount DESC, at ASC
		LIMIT $2
		ON CONFLICT (auction_id, rank) DO UPDATE SET
			bidder_id = EXCLUDED.bidder_id,
			amount = EXCLUDED.amount,
			bid_at = EXCLUDED.bid_at`, auctionID, stock)
	return err
}

func claimStale(ctx context.Context, rdb *redis.Client, stream, group, consumer string) ([]redis.XMessage, error) {
	msgs, _, err := rdb.XAutoClaim(ctx, &redis.XAutoClaimArgs{
		Stream:   stream,
		Group:    group,
		Consumer: consumer,
		MinIdle:  30 * time.Second,
		Start:    "0-0",
		Count:    200,
	}).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return nil, err
	}
	return msgs, nil
}

func main() {
	store.LoadDotEnv()
	auctionID := env("AUCTION_ID", "a1")
	stock, _ := strconv.Atoi(env("STOCK", "3")) // must match the engine's STOCK
	exitOnClose := env("EXIT_ON_CLOSE", "true") == "true"

	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		log.Fatal("DATABASE_URL is not set (.env or environment)")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		log.Fatalf("postgres: %v", err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		log.Fatalf("postgres ping: %v", err)
	}

	rdb, err := store.NewRedisClient()
	if err != nil {
		log.Fatalf("redis: %v", err)
	}
	defer rdb.Close()

	stream := "auction:" + auctionID + ":bids"
	stateKey := "auction:" + auctionID + ":state"
	group, consumer := "persister", env("CONSUMER", "p1")

	err = rdb.XGroupCreateMkStream(ctx, stream, group, "0").Err()
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		log.Fatalf("create group: %v", err)
	}
	log.Printf("persister: auction %q, stock %d, consumer %q", auctionID, stock, consumer)

	pendingDone := false
	winnersWritten := false
	idle, total := 0, 0
	lastClaim := time.Now().Add(-time.Minute)

	for ctx.Err() == nil {
		var msgs []redis.XMessage

		if time.Since(lastClaim) >= 15*time.Second {
			lastClaim = time.Now()
			if claimed, err := claimStale(ctx, rdb, stream, group, consumer); err == nil && len(claimed) > 0 {
				msgs = claimed
			}
		}

		if len(msgs) == 0 {
			id := ">"
			if !pendingDone {
				id = "0"
			}

			res, err := rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
				Group:    group,
				Consumer: consumer,
				Streams:  []string{stream, id},
				Count:    500,
				Block:    time.Second,
			}).Result()

			if err == nil && len(res) > 0 {
				msgs = res[0].Messages
			} else if err != nil && !errors.Is(err, redis.Nil) {
				if ctx.Err() != nil {
					break
				}
				log.Printf("read: %v", err)
				time.Sleep(time.Second)
				continue
			}
		}

		if len(msgs) == 0 {
			if !pendingDone {
				pendingDone = true
				continue
			}
			idle++
			if idle >= 3 {
				status, err := rdb.HGet(ctx, stateKey, "status").Result()
				if err == nil && status == string(configuration.StatusClosed) && !winnersWritten {
					wctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
					werr := writeWinners(wctx, pool, auctionID, stock)
					cancel()
					if werr != nil {
						log.Printf("winners: %v", werr)
						continue
					}
					winnersWritten = true
					log.Printf("auction closed, winners written (%d bids saved)", total)
					if exitOnClose {
						return
					}
				}
			}
			continue
		}
		idle = 0

		rows := make([]row, 0, len(msgs))
		ackIDs := make([]string, 0, len(msgs))
		for _, m := range msgs {
			r, err := parse(m)
			ackIDs = append(ackIDs, m.ID)
			if err != nil {
				log.Printf("skipping: %v", err)
				continue
			}
			rows = append(rows, r)
		}

		if len(rows) > 0 {
			ictx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			err := insertBatch(ictx, pool, auctionID, rows)
			cancel()
			if err != nil {
				log.Printf("insert failed, will retry: %v", err)
				pendingDone = false
				time.Sleep(time.Second)
				continue
			}
		}

		actx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err = rdb.XAck(actx, stream, group, ackIDs...).Err()
		cancel()
		if err != nil {
			log.Printf("ack: %v", err)
		}

		total += len(rows)
		log.Printf("saved %d bids (total %d)", len(rows), total)
	}
	log.Println("persister stopped")
}