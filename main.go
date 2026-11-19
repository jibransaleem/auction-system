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
	st := store.NewRedisStore(rdb)

	startDelay, err := time.ParseDuration(envStr("AUCTION_START_DELAY", "0s"))
	if err != nil {
		_ = rdb.Close()
		log.Fatalf("bad AUCTION_START_DELAY: %v", err)
	}

	dur, err := time.ParseDuration(envStr("AUCTION_DURATION", "5m"))
	if err != nil {
		_ = rdb.Close()
		log.Fatalf("bad AUCTION_DURATION: %v", err)
	}
	if dur <= 0 {
		_ = rdb.Close()
		log.Fatalf("AUCTION_DURATION must be > 0, got %s", dur)
	}

	minPrice := envInt("MIN_PRICE", 100)
	if minPrice <= 0 {
		_ = rdb.Close()
		log.Fatalf("MIN_PRICE must be > 0, got %d", minPrice)
	}

	minInc := envInt("MIN_INCREMENT", 10)
	if minInc <= 0 {
		_ = rdb.Close()
		log.Fatalf("MIN_INCREMENT must be > 0, got %d", minInc)
	}

	stock := int(envInt("STOCK", 3))
	if stock < 1 {
		_ = rdb.Close()
		log.Fatalf("STOCK must be >= 1, got %d", stock)
	}

	now := time.Now()
	start := now.Add(startDelay)

	conf := cfg.AuctionConfig{
		AuctionID:    envStr("AUCTION_ID", "a1"),
		Title:        envStr("AUCTION_TITLE", "Flash auction"),
		MinimumPrice: minPrice,
		MinIncrement: minInc,
		Stock:        stock,
		StartAt:      start,
		EndAt:        start.Add(dur),
		FinalWindow:  10 * time.Second,
	}

	mgr, err := manager.New(conf, st, store.NewRedisPublisher(rdb), store.NewRedisBidLog(rdb))
	if err != nil {
		_ = rdb.Close()
		log.Fatalf("manager: %v", err)
	}

	addr := envStr("GRPC_ADDR", ":50051")
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		_ = rdb.Close()
		log.Fatalf("listen %s: %v", addr, err)
	}

	g := grpc.NewServer()
	pb.RegisterBidEngineServer(g, grpcserver.New(mgr, st))
	reflection.Register(g) // lets grpcurl work without the .proto file

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sigCh)

	go func() {
		<-sigCh
		log.Println("shutting down")
		g.GracefulStop()
	}()

	log.Printf("engine: auction %q starts at %s, ends at %s, gRPC on %s",
		conf.AuctionID, conf.StartAt.Format(time.RFC3339), conf.EndAt.Format(time.RFC3339), addr)
	if err := g.Serve(lis); err != nil {
		_ = rdb.Close()
		log.Fatalf("serve: %v", err)
	}

	if err := rdb.Close(); err != nil {
		log.Printf("close redis: %v", err)
	}
}