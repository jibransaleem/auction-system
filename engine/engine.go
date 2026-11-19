package engine

import (
	"context"
	"time"

	"github.com/jibransaleem/auction-system/engine/top_k"

	cfg "github.com/jibransaleem/auction-system/engine/configuration"
)

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
	bids chan cfg.AcceptedBid   // in the Engine struct

	 
	state   cfg.AuctionState
	seen    map[string]struct{} // accepted BidIDs
	winners []cfg.Bid

	in     chan request
	events chan cfg.Event
	done   chan struct{}
}

// New creates and initializes a new auction engine.
func New(conf cfg.AuctionConfig) *Engine {
	e := &Engine{
		conf: conf,
		now:  time.Now,
		topk: top_k.New(conf),
		state: cfg.AuctionState{
			AuctionID: conf.AuctionID,
			Price:     conf.MinimumPrice,
			StockLeft: conf.Stock,
			EndAt:    conf.EndAt,
			Status:    cfg.StatusOpen,
		},
		bids: make(chan cfg.AcceptedBid, 65536),
		seen:   make(map[string]struct{}),
		in:     make(chan request, 4096), // absorbs bursts
		events: make(chan cfg.Event, 4096),
		done:   make(chan struct{}),
	}
	go e.run()
	return e
}
func (e *Engine) Bids() <-chan cfg.AcceptedBid { return e.bids }
func (e *Engine) Events() <-chan cfg.Event { return e.events }
func (e *Engine) Done() <-chan struct{}    { return e.done }
func (e *Engine) Winners() []cfg.Bid       { <-e.done; return e.winners } // safe only after close

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
	timer := time.NewTimer(e.conf.EndAt.Sub(e.now()))
	defer timer.Stop()

	for {
		select {
		case req := <-e.in:
			req.reply <- e.handle(req.bid)
		case <-timer.C:
			e.close()
			return
		}
	}
}

func (e *Engine) handle(b cfg.Bid) cfg.Result {
	now := e.now()
	b.ReceivedAt = now // server time only

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
	e.bids <- cfg.AcceptedBid{
	BidID: b.BidID, AuctionID: e.conf.AuctionID, BidderID: b.BidderID,
	Amount: b.Amount, Version: e.state.Version, At: now,
	}
	e.emit(cfg.Event{
		Kind:      cfg.EventPriceUpdate,
		AuctionID: e.conf.AuctionID,
		Price:     e.state.Price,
		Version:   e.state.Version,
		StockLeft: e.state.StockLeft, // Added StockLeft
		At:        now,
	}, false)

	return cfg.Result{
		BidID: b.BidID, Accepted: true,
		Price: e.state.Price, Version: e.state.Version,
		Rank: e.topk.Rank(b.BidderID),
	}
}

func (e *Engine) reject(b cfg.Bid, why cfg.RejectReason) cfg.Result {
	return cfg.Result{
		BidID: b.BidID, Reason: why,
		Price: e.state.Price, Version: e.state.Version, // lets the client refresh its UI
	}
}

func (e *Engine) closedResult(b cfg.Bid) cfg.Result {
	return cfg.Result{BidID: b.BidID, Reason: cfg.ReasonClosed}
}

func (e *Engine) close() {
	e.state.Status = cfg.StatusClosed
	e.winners = e.topk.Snapshot()
	e.emit(cfg.Event{
		Kind:      cfg.EventClosed,
		AuctionID: e.conf.AuctionID,
		Price:     e.state.Price,
		Version:   e.state.Version + 1,
		StockLeft: e.state.StockLeft, // Added StockLeft
		At:        e.now(),
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