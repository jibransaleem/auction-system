package main

import (
	"context"
	_ "embed"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/jibransaleem/auction-system/pb" // same path as in your engine main.go
)

//go:embed index.html
var page []byte

type inMsg struct {
	Type   string `json:"type"`
	BidID  string `json:"bid_id"`
	Amount int64  `json:"amount"`
}

type outResult struct {
	Type     string `json:"type"` // "result"
	BidID    string `json:"bid_id"`
	Accepted bool   `json:"accepted"`
	Reason   string `json:"reason"`
	Price    int64  `json:"price"`
	Version  uint64 `json:"version"`
	Rank     int32  `json:"rank"`
}

type outError struct {
	Type    string `json:"type"` // "error"
	Message string `json:"message"`
}

type Gateway struct {
	engine    pb.BidEngineClient
	auctionID string
}

func (g *Gateway) ws(w http.ResponseWriter, r *http.Request) {
	user := r.URL.Query().Get("user")
	if user == "" {
		http.Error(w, "user is required", http.StatusBadRequest)
		return
	}

	c, err := websocket.Accept(w, r, nil)
	if err != nil {
		return
	}
	defer c.CloseNow()
	c.SetReadLimit(4096)

	ctx := r.Context()
	log.Printf("%s connected", user)

	for {
		_, data, err := c.Read(ctx)
		if err != nil {
			log.Printf("%s left", user)
			return
		}

		var m inMsg
		if json.Unmarshal(data, &m) != nil || m.Type != "bid" || m.BidID == "" || m.Amount <= 0 {
			wsjson.Write(ctx, c, outError{"error", "invalid bid message"})
			continue
		}

		cctx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
		resp, err := g.engine.PlaceBid(cctx, &pb.PlaceBidRequest{
			BidId:     m.BidID,
			AuctionId: g.auctionID,
			BidderId:  user,
			Amount:    m.Amount,
		})
		cancel()

		if err != nil {
			wsjson.Write(ctx, c, outError{"error", status.Convert(err).Message()})
			continue
		}
		wsjson.Write(ctx, c, outResult{
			Type: "result", BidID: resp.BidId, Accepted: resp.Accepted,
			Reason: resp.Reason, Price: resp.Price, Version: resp.Version, Rank: resp.Rank,
		})
	}
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func main() {
	conn, err := grpc.NewClient(env("ENGINE_ADDR", "localhost:50051"),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("grpc: %v", err)
	}
	defer conn.Close()

	g := &Gateway{
		engine:    pb.NewBidEngineClient(conn),
		auctionID: env("AUCTION_ID", "a1"),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/ws", g.ws)
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write(page)
	})

	addr := env("GATEWAY_ADDR", ":8080")
	log.Printf("gateway on %s, auction %q, engine %s", addr, g.auctionID, env("ENGINE_ADDR", "localhost:50051"))
	log.Fatal(http.ListenAndServe(addr, mux))
}