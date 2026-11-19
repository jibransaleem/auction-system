package main

import (
	"github.com/redis/go-redis/v9"
	"context"
	_ "embed"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"sync"
	"time"
	"golang.org/x/time/rate"
	"github.com/coder/websocket"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/jibransaleem/auction-system/pb"
	"github.com/jibransaleem/auction-system/store"
)

//go:embed index.html
var page []byte

// ---------- messages ----------
// fanout sends Redis price updates to all clients.
// In the final window it keeps only the newest update and flushes every 50 ms.
func (g *Gateway) fanout(ch <-chan *redis.Message, endsAt time.Time) {
	const window = 10 * time.Second

	tick := time.NewTicker(50 * time.Millisecond)
	defer tick.Stop()

	var pending []byte
	skipped := 0

	for {
		select {
		case m, ok := <-ch:
			if !ok {
				return
			}
			var ev struct {
				Kind string `json:"kind"`
			}
			_ = json.Unmarshal([]byte(m.Payload), &ev)

			if ev.Kind == "closed" { // never delayed, replaces anything pending
				pending = nil
				g.hub.broadcast([]byte(m.Payload))
				continue
			}

			if time.Now().Before(endsAt.Add(-window)) {
				g.hub.broadcast([]byte(m.Payload)) // normal time: send now
			} else {
				if pending != nil {
					skipped++
				}
				pending = []byte(m.Payload) // final window: latest wins
			}

		case <-tick.C:
			if pending != nil {
				g.hub.broadcast(pending)
				if skipped > 0 {
					log.Printf("coalesced: skipped %d older updates", skipped)
				}
				pending, skipped = nil, 0
			}
		}
	}
}
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

type outState struct {
	Type      string `json:"type"` // "state"
	Price     int64  `json:"price"`
	Version   uint64 `json:"version"`
	StockLeft int32  `json:"stock_left"`
	EndsAtMs  int64  `json:"ends_at_ms"`
	Status    string `json:"status"`
}

// ---------- client + hub ----------

type client struct {
	user   string
	send   chan []byte // buffer of 32
	cancel context.CancelFunc
}

// push never blocks. If the buffer is full, the client is too slow: drop it.
func (c *client) push(msg []byte) {
	select {
	case c.send <- msg:
	default:
		log.Printf("%s too slow, dropping", c.user)
		c.cancel()
	}
}

func (c *client) pushJSON(v any) {
	if b, err := json.Marshal(v); err == nil {
		c.push(b)
	}
}

type Hub struct {
	mu      sync.RWMutex
	clients map[*client]struct{}
}

func NewHub() *Hub { return &Hub{clients: make(map[*client]struct{})} }

func (h *Hub) add(c *client) {
	h.mu.Lock()
	h.clients[c] = struct{}{}
	h.mu.Unlock()
}

func (h *Hub) remove(c *client) {
	h.mu.Lock()
	delete(h.clients, c)
	h.mu.Unlock()
}

func (h *Hub) broadcast(msg []byte) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	for c := range h.clients {
		c.push(msg)
	}
}

func (h *Hub) count() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.clients)
}

// ---------- gateway ----------

type Gateway struct {
	engine    pb.BidEngineClient
	auctionID string
	hub       *Hub
	limiters  sync.Map // user -> *rate.Limiter
}

// one limiter per USER, so opening many tabs doesn't bypass it
func (g *Gateway) limiter(user string) *rate.Limiter {
	if l, ok := g.limiters.Load(user); ok {
		return l.(*rate.Limiter)
	}
	l, _ := g.limiters.LoadOrStore(user, rate.NewLimiter(5, 5)) // 5/s, burst 5
	return l.(*rate.Limiter)
}
func writePump(ctx context.Context, cancel context.CancelFunc, c *websocket.Conn, send <-chan []byte) {
	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-send:
			wctx, wcancel := context.WithTimeout(ctx, 5*time.Second)
			err := c.Write(wctx, websocket.MessageText, msg)
			wcancel()
			if err != nil {
				cancel()
				return
			}
		}
	}
}
func (g *Gateway) ws(w http.ResponseWriter, r *http.Request) {
	user := r.URL.Query().Get("user")
	if user == "" {
		http.Error(w, "user is required", http.StatusBadRequest)
		return
	}

	conn, err := websocket.Accept(w, r, nil)
	if err != nil {
		return
	}
	defer conn.CloseNow()
	conn.SetReadLimit(4096)

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	cl := &client{user: user, send: make(chan []byte, 32), cancel: cancel}
	g.hub.add(cl)
	defer g.hub.remove(cl)
	log.Printf("%s connected (%d online)", user, g.hub.count())

	go writePump(ctx, cancel, conn, cl.send)
	go func() {
		t := time.NewTicker(20 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				pctx, pcancel := context.WithTimeout(ctx, 10*time.Second)
				err := conn.Ping(pctx) // waits for the pong
				pcancel()
				if err != nil {
					log.Printf("%s ping failed, dropping", user)
					cancel()
					return
				}
			}
		}
	}()

	// send the current state so a late joiner sees the price right away
	sctx, scancel := context.WithTimeout(ctx, 500*time.Millisecond)
	if st, err := g.engine.GetState(sctx, &pb.GetStateRequest{AuctionId: g.auctionID}); err == nil {
		cl.pushJSON(outState{"state", st.Price, st.Version, st.StockLeft, st.EndsAtUnixMs, st.Status})
	}
	scancel()

	// read loop
	for {
		_, data, err := conn.Read(ctx)
		if err != nil {
			log.Printf("%s left", user)
			return
		}

		var m inMsg
		if json.Unmarshal(data, &m) != nil || m.Type != "bid" || m.BidID == "" || m.Amount <= 0 {
			cl.pushJSON(outError{"error", "invalid bid message"})
			continue
		}
		if !g.limiter(user).Allow() {
			cl.pushJSON(outError{"error", "rate_limited: max 5 bids per second"})
			continue
		}

		bctx, bcancel := context.WithTimeout(ctx, 500*time.Millisecond)
		resp, err := g.engine.PlaceBid(bctx, &pb.PlaceBidRequest{
			BidId: m.BidID, AuctionId: g.auctionID, BidderId: user, Amount: m.Amount,
		})
		bcancel()

		if err != nil {
			cl.pushJSON(outError{"error", status.Convert(err).Message()})
			continue
		}
		cl.pushJSON(outResult{"result", resp.BidId, resp.Accepted, resp.Reason, resp.Price, resp.Version, resp.Rank})
	}
}

// ---------- main ----------

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func main() {
	auctionID := env("AUCTION_ID", "a1")

	// gRPC to the engine
	conn, err := grpc.NewClient(env("ENGINE_ADDR", "localhost:50051"),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("grpc: %v", err)
	}
	defer conn.Close()

	// Redis (same helper the engine uses: REDIS_URL from .env)
	rdb, err := store.NewRedisClient()
	if err != nil {
		log.Fatalf("redis: %v", err)
	}
	defer rdb.Close()

	hub := NewHub()
	engineClient := pb.NewBidEngineClient(conn)

	var endsAt time.Time
	for i := 0; i < 10; i++ {
		gctx, gcancel := context.WithTimeout(context.Background(), time.Second)
		st, err := engineClient.GetState(gctx, &pb.GetStateRequest{AuctionId: auctionID})
		gcancel()
		if err == nil {
			endsAt = time.UnixMilli(st.EndsAtUnixMs)
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if endsAt.IsZero() {
		log.Fatal("could not read auction end time from engine")
	}
	log.Printf("auction ends at %s", endsAt.Format(time.RFC3339))

	// ONE subscription per gateway, shared by all its clients
	sub := rdb.Subscribe(context.Background(), store.UpdatesChannel(auctionID))
	defer sub.Close()
	rctx, rcancel := context.WithTimeout(context.Background(), 5*time.Second)
	if _, err := sub.Receive(rctx); err != nil { // wait for the subscribe confirmation
		log.Fatalf("subscribe: %v", err)
	}
	rcancel()
	go func() {
		for m := range sub.Channel() {
			hub.broadcast([]byte(m.Payload))
		}
	}()

	g := &Gateway{engine: engineClient, auctionID: auctionID, hub: hub}
	go g.fanout(sub.Channel(), endsAt)

	mux := http.NewServeMux()
	mux.HandleFunc("/ws", g.ws)
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write(page)
	})

	addr := env("GATEWAY_ADDR", ":8080")
	log.Printf("gateway on %s, auction %q", addr, auctionID)
	log.Fatal(http.ListenAndServe(addr, mux))
}