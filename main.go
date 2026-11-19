package main

import (
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	cfg "github.com/jibransaleem/auction-system/engine/configuration"
	"github.com/jibransaleem/auction-system/engine/manager"
	"github.com/jibransaleem/auction-system/grpcserver"
	"github.com/jibransaleem/auction-system/pb"
	"github.com/jibransaleem/auction-system/store"
)

func envStr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func envInt(k string, def int64) int64 {
	if v := os.Getenv(k); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			return n
		}
	}
	return def
}

func main() {
	rdb, err := store.NewRedisClient() // loads .env, reads REDIS_URL, pings
	if err != nil {
		log.Fatalf("redis: %v", err)
	}
	defer rdb.Close()
	st := store.NewRedisStore(rdb)

	startDelay, err := time.ParseDuration(envStr("AUCTION_START_DELAY", "0s"))
	if err != nil {
		log.Fatalf("bad AUCTION_START_DELAY: %v", err)
	}

	dur, err := time.ParseDuration(envStr("AUCTION_DURATION", "5m"))
	if err != nil {
		log.Fatalf("bad AUCTION_DURATION: %v", err)
	}

	start := time.Now().Add(startDelay)

	// field names must match your configuration.AuctionConfig
	conf := cfg.AuctionConfig{
		AuctionID:    envStr("AUCTION_ID", "a1"),
		Title:        envStr("AUCTION_TITLE", "Flash auction"),
		MinimumPrice: envInt("MIN_PRICE", 100),
		MinIncrement: envInt("MIN_INCREMENT", 10),
		Stock:        int(envInt("STOCK", 3)),
		StartAt:      start,
		EndAt:        start.Add(dur), // duration now counts from the start time
		FinalWindow:  10 * time.Second,
	}

	mgr, err := manager.New(conf, st, store.NewRedisPublisher(rdb), store.NewRedisBidLog(rdb))
	if err != nil {
		log.Fatalf("manager: %v", err)
	}

	addr := envStr("GRPC_ADDR", ":50051")
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("listen %s: %v", addr, err)
	}

	g := grpc.NewServer()
	pb.RegisterBidEngineServer(g, grpcserver.New(mgr, st))
	reflection.Register(g) // lets grpcurl work without the .proto file

	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
		<-sig
		log.Println("shutting down")
		g.GracefulStop()
	}()

	log.Printf("engine: auction %q starts at %s, ends at %s, gRPC on %s",
		conf.AuctionID, conf.StartAt.Format(time.RFC3339), conf.EndAt.Format(time.RFC3339), addr)
	if err := g.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}
}