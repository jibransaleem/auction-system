package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/redis/go-redis/v9"
	"golang.org/x/time/rate"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/jibransaleem/auction-system/engine/configuration"
	"github.com/jibransaleem/auction-system/pb"
	"github.com/jibransaleem/auction-system/store"
)

// ---------- fanout ----------

// fanout sends Redis price/state/closed updates to all clients.
// In the final window it keeps only the newest update and flushes every 50 ms.
func (g *Gateway) fanout(ch <-chan *redis.Message) {
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
			var ev configuration.Event
			_ = json.Unmarshal([]byte(m.Payload), &ev)

			if ev.Title != "" {
				g.setTitle(ev.Title)
			}
			if ev.EndsAtMs > 0 {
				g.setEndsAt(time.UnixMilli(ev.EndsAtMs))
			}

			if ev.Kind == configuration.EventClosed { // never delayed, replaces anything pending
				pending = nil
				skipped = 0
				g.hub.broadcast([]byte(m.Payload))
				continue
			}

			endsAt := g.getEndsAt()
			if endsAt.IsZero() || time.Now().Before(endsAt.Add(-window)) {
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

// ---------- messages ----------

type inMsg struct {
	Type        string `json:"type"`
	BidID       string `json:"bid_id"`
	Amount      int64  `json:"amount"`
	Name        string `json:"name"`
	StartPrice  int64  `json:"start_price"`
	MinRaise    int64  `json:"min_raise"`
	WinnerCount int32  `json:"winner_count"`
	StartsAtMs  int64  `json:"starts_at_ms"`
	EndsAtMs    int64  `json:"ends_at_ms"`
	DurationSec int64  `json:"duration_sec"`
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
	Title     string `json:"title,omitempty"`
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

func (h *Hub) broadcastJSON(v any) {
	if b, err := json.Marshal(v); err == nil {
		h.broadcast(b)
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

	mu     sync.RWMutex
	title  string
	endsAt time.Time
}

func (g *Gateway) setTitle(t string) {
	if t == "" {
		return
	}
	g.mu.Lock()
	g.title = t
	g.mu.Unlock()
}

func (g *Gateway) getTitle() string {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.title
}

func (g *Gateway) setEndsAt(t time.Time) {
	g.mu.Lock()
	g.endsAt = t
	g.mu.Unlock()
}

func (g *Gateway) getEndsAt() time.Time {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.endsAt
}

func (g *Gateway) limiter(user string) *rate.Limiter {
	if l, ok := g.limiters.Load(user); ok {
		return l.(*rate.Limiter)
	}
	l, _ := g.limiters.LoadOrStore(user, rate.NewLimiter(5, 5))
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
				_ = c.CloseNow()
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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		<-ctx.Done()
		_ = conn.CloseNow()
	}()

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
				err := conn.Ping(pctx)
				pcancel()
				if err != nil {
					log.Printf("%s ping failed, dropping", user)
					cancel()
					return
				}
			}
		}
	}()

	sctx, scancel := context.WithTimeout(ctx, 500*time.Millisecond)
	st, err := g.engine.GetState(sctx, &pb.GetStateRequest{AuctionId: g.auctionID})
	scancel()
	if err != nil {
		cl.pushJSON(outError{Type: "error", Message: "failed to load initial state: " + status.Convert(err).Message()})
	} else {
		if st.EndsAtUnixMs > 0 {
			g.setEndsAt(time.UnixMilli(st.EndsAtUnixMs))
		}
		cl.pushJSON(outState{
			Type:      "state",
			Title:     g.getTitle(),
			Price:     st.Price,
			Version:   st.Version,
			StockLeft: st.StockLeft,
			EndsAtMs:  st.EndsAtUnixMs,
			Status:    st.Status,
		})
	}

	for {
		_, data, err := conn.Read(ctx)
		if err != nil {
			log.Printf("%s left", user)
			return
		}

		var m inMsg
		if json.Unmarshal(data, &m) != nil || m.Type == "" {
			cl.pushJSON(outError{Type: "error", Message: "invalid message"})
			continue
		}

		switch m.Type {
		case "bid":
			if m.BidID == "" || m.Amount <= 0 {
				cl.pushJSON(outError{Type: "error", Message: "invalid bid message"})
				continue
			}
			if !g.limiter(user).Allow() {
				cl.pushJSON(outError{Type: "error", Message: "rate_limited: max 5 bids per second"})
				continue
			}

			bctx, bcancel := context.WithTimeout(ctx, 500*time.Millisecond)
			resp, err := g.engine.PlaceBid(bctx, &pb.PlaceBidRequest{
				BidId: m.BidID, AuctionId: g.auctionID, BidderId: user, Amount: m.Amount,
			})
			bcancel()

			if err != nil {
				cl.pushJSON(outError{Type: "error", Message: status.Convert(err).Message()})
				continue
			}
			cl.pushJSON(outResult{
				Type:     "result",
				BidID:    resp.BidId,
				Accepted: resp.Accepted,
				Reason:   resp.Reason,
				Price:    resp.Price,
				Version:  resp.Version,
				Rank:     resp.Rank,
			})

		case "configure_auction":
			cctx, ccancel := context.WithTimeout(ctx, 2*time.Second)
			st, err := g.engine.ConfigureAuction(cctx, &pb.ConfigureAuctionRequest{
				AuctionId:   g.auctionID,
				Title:       m.Name,
				StartPrice:  m.StartPrice,
				MinRaise:    m.MinRaise,
				WinnerCount: m.WinnerCount,
				StartsAtMs:  m.StartsAtMs,
				EndsAtMs:    m.EndsAtMs,
				DurationSec: m.DurationSec,
			})
			ccancel()
			if err != nil {
				cl.pushJSON(outError{Type: "error", Message: status.Convert(err).Message()})
				continue
			}
			if m.Name != "" {
				g.setTitle(m.Name)
			}
			if st.EndsAtUnixMs > 0 {
				g.setEndsAt(time.UnixMilli(st.EndsAtUnixMs))
			} else {
				g.setEndsAt(time.Time{})
			}
			g.hub.broadcastJSON(outState{
				Type:      "state",
				Title:     g.getTitle(),
				Price:     st.Price,
				Version:   st.Version,
				StockLeft: st.StockLeft,
				EndsAtMs:  st.EndsAtUnixMs,
				Status:    st.Status,
			})
			log.Printf("admin %s configured auction (title=%q, start_price=%d)", user, m.Name, m.StartPrice)

		case "close_auction":
			cctx, ccancel := context.WithTimeout(ctx, 2*time.Second)
			st, err := g.engine.CloseAuction(cctx, &pb.CloseAuctionRequest{
				AuctionId: g.auctionID,
			})
			ccancel()
			if err != nil {
				cl.pushJSON(outError{Type: "error", Message: status.Convert(err).Message()})
				continue
			}
			g.setEndsAt(time.Now())
			g.hub.broadcastJSON(outState{
				Type:      "state",
				Title:     g.getTitle(),
				Price:     st.Price,
				Version:   st.Version,
				StockLeft: st.StockLeft,
				EndsAtMs:  st.EndsAtUnixMs,
				Status:    st.Status,
			})
			log.Printf("admin %s closed auction", user)

		default:
			cl.pushJSON(outError{Type: "error", Message: "unsupported message type"})
		}
	}
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func main() {
	auctionID := env("AUCTION_ID", "a1")
	auctionTitle := env("AUCTION_TITLE", "Flash auction")

	conn, err := grpc.NewClient(env("ENGINE_ADDR", "localhost:50051"),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("grpc: %v", err)
	}
	defer conn.Close()

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
			if st.EndsAtUnixMs > 0 {
				endsAt = time.UnixMilli(st.EndsAtUnixMs)
			}
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if endsAt.IsZero() {
		log.Printf("auction %q has no end time yet (will update on state/configure events)", auctionID)
	} else {
		log.Printf("auction ends at %s", endsAt.Format(time.RFC3339))
	}

	sub := rdb.Subscribe(context.Background(), store.UpdatesChannel(auctionID))
	defer sub.Close()
	rctx, rcancel := context.WithTimeout(context.Background(), 5*time.Second)
	if _, err := sub.Receive(rctx); err != nil {
		log.Fatalf("subscribe: %v", err)
	}
	rcancel()

	g := &Gateway{
		engine:    engineClient,
		auctionID: auctionID,
		hub:       hub,
		title:     auctionTitle,
		endsAt:    endsAt,
	}
	go g.fanout(sub.Channel())

	mux := http.NewServeMux()
	mux.HandleFunc("/ws", g.ws)

	addr := env("GATEWAY_ADDR", ":8080")
	log.Printf("gateway CLI/WS mode on %s, auction %q", addr, auctionID)
	log.Fatal(http.ListenAndServe(addr, mux))
}