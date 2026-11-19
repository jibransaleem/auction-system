package engine

import (
	"context"
	"time"

	cfg "github.com/jibransaleem/auction-system/engine/configuration"
	"github.com/jibransaleem/auction-system/engine/top_k"
)

const leaderboardSize = 10

// request wraps a single incoming Bid together with a private channel (reply)
// used by the engine to send the answer back to the exact thread that asked for it.
type request struct {
	bid   cfg.Bid
	reply chan cfg.Result
}

// Engine holds all live data for a single auction.
type Engine struct {
	conf    cfg.AuctionConfig
	now     func() time.Time
	topk    *top_k.TopK
	bids    chan cfg.AcceptedBid

	state   cfg.AuctionState
	seen    map[string]struct{} // accepted BidIDs
	winners []cfg.Bid

	in     chan request
	events chan cfg.Event
	done   chan struct{}
}

// New creates and initializes a new auction engine.
func New(conf cfg.AuctionConfig) *Engine {
	nowFn := time.Now
	initialStatus := cfg.StatusOpen
	if !conf.StartAt.IsZero() && nowFn().Before(conf.StartAt) {
		initialStatus = cfg.StatusScheduled
	}

	e := &Engine{
		conf: conf,
		now:  nowFn,
		topk: top_k.New(conf),
		state: cfg.AuctionState{
			AuctionID: conf.AuctionID,
			Title:     conf.Title,
			StartAt:   conf.StartAt,
			Price:     conf.MinimumPrice,
			StockLeft: conf.Stock,
			EndAt:     conf.EndAt,
			Status:    initialStatus,
		},
		bids:   make(chan cfg.AcceptedBid, 65536),
		seen:   make(map[string]struct{}),
		in:     make(chan request, 4096), // absorbs bursts
		events: make(chan cfg.Event, 4096),
		done:   make(chan struct{}),
	}
	go e.run()
	return e
}

func (e *Engine) Bids() <-chan cfg.AcceptedBid { return e.bids }
func (e *Engine) Events() <-chan cfg.Event       { return e.events }
func (e *Engine) Done() <-chan struct{}          { return e.done }
func (e *Engine) Winners() []cfg.Bid             { <-e.done; return e.winners } // safe only after close

// Submit is the public entry point for incoming bids.
func (e *Engine) Submit(ctx context.Context, b cfg.Bid) (cfg.Result, error) {
	req := request{bid: b, reply: make(chan cfg.Result, 1)}

	select {
	case e.in <- req:
	case <-e.done:
		return e.closedResult(b), nil
	case <-ctx.Done():
		return cfg.Result{}, ctx.Err()
	}

	select {
	case r := <-req.reply:
		return r, nil
	case <-e.done:
		select {
		case r := <-req.reply:
			return r, nil
		default:
			return e.closedResult(b), nil
		}
	case <-ctx.Done():
		return cfg.Result{}, ctx.Err()
	}
}

// ---------- the owner goroutine ----------

func (e *Engine) run() {
	var startCh <-chan time.Time
	var startTimer *time.Timer
	if !e.conf.StartAt.IsZero() && e.now().Before(e.conf.StartAt) {
		startTimer = time.NewTimer(e.conf.StartAt.Sub(e.now()))
		startCh = startTimer.C
		defer startTimer.Stop()
	} else {
		e.state.Status = cfg.StatusOpen
	}

	endTimer := time.NewTimer(e.conf.EndAt.Sub(e.now()))
	defer endTimer.Stop()

	for {
		select {
		case <-startCh:
			e.state.Status = cfg.StatusOpen
			startCh = nil
		case req := <-e.in:
			req.reply <- e.handle(req.bid)
		case <-endTimer.C:
			e.close()
			return
		}
	}
}

func (e *Engine) handle(b cfg.Bid) cfg.Result {
	now := e.now()
	b.ReceivedAt = now // server time only

	if !e.conf.StartAt.IsZero() && now.Before(e.conf.StartAt) {
		return e.reject(b, cfg.ReasonNotStarted)
	}
	if !now.Before(e.conf.EndAt) {
		return e.reject(b, cfg.ReasonClosed)
	}
	if _, dup := e.seen[b.BidID]; dup {
		return e.reject(b, cfg.ReasonDuplicate)
	}

	out := e.topk.Offer(b)
	if !out.Accepted {
		return e.reject(b, out.Reason)
	}

	e.seen[b.BidID] = struct{}{}
	e.state.Version++
	e.state.Price = e.topk.MinToBeat()
	e.state.StockLeft = e.conf.Stock - e.topk.Len()

	top, rank := e.leaderboard(b.BidderID)
	e.state.Top = top

	e.bids <- cfg.AcceptedBid{
		BidID:     b.BidID,
		AuctionID: e.conf.AuctionID,
		BidderID:  b.BidderID,
		Amount:    b.Amount,
		Version:   e.state.Version,
		At:        now,
	}
	e.emit(cfg.Event{
		Kind:      cfg.EventPriceUpdate,
		AuctionID: e.conf.AuctionID,
		Price:     e.state.Price,
		StockLeft: e.state.StockLeft,
		Version:   e.state.Version,
		At:        now,
		Top:       top,
	}, false)

	return cfg.Result{
		BidID:    b.BidID,
		Accepted: true,
		Price:    e.state.Price,
		Version:  e.state.Version,
		Rank:     rank,
	}
}

// leaderboard returns the top entries (best first) and the rank of `who`.
func (e *Engine) leaderboard(who string) ([]cfg.LeaderEntry, int) {
	snap := e.topk.Snapshot()
	rank := 0
	out := make([]cfg.LeaderEntry, 0, len(snap))
	for i, b := range snap {
		if b.BidderID == who {
			rank = i + 1
		}
		if i < leaderboardSize {
			out = append(out, cfg.LeaderEntry{Rank: i + 1, BidderID: b.BidderID, Amount: b.Amount})
		}
	}
	return out, rank
}

func (e *Engine) reject(b cfg.Bid, why cfg.RejectReason) cfg.Result {
	return cfg.Result{
		BidID:   b.BidID,
		Reason:  why,
		Price:   e.state.Price,
		Version: e.state.Version, // lets the client refresh its UI
	}
}

func (e *Engine) closedResult(b cfg.Bid) cfg.Result {
	return cfg.Result{BidID: b.BidID, Reason: cfg.ReasonClosed}
}

func (e *Engine) close() {
	e.state.Status = cfg.StatusClosed
	e.winners = e.topk.Snapshot()
	top, _ := e.leaderboard("")
	e.state.Top = top

	e.emit(cfg.Event{
		Kind:      cfg.EventClosed,
		AuctionID: e.conf.AuctionID,
		Price:     e.state.Price,
		StockLeft: e.state.StockLeft,
		Version:   e.state.Version + 1,
		At:        e.now(),
		Top:       top,
	}, true)

	// answer anything still waiting in the queue
	for {
		select {
		case req := <-e.in:
			req.reply <- e.closedResult(req.bid)
		default:
			close(e.bids)
			close(e.done)
			return
		}
	}
}

func (e *Engine) emit(ev cfg.Event, mustDeliver bool) {
	if mustDeliver {
		e.events <- ev // closed event is never dropped
		return
	}
	select {
	case e.events <- ev:
	default: // slow consumer; next update supersedes this one
	}
}