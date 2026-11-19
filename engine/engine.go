package engine

import (
	"context"
	"time"

	cfg "github.com/jibransaleem/auction-system/engine/configuration"
	"github.com/jibransaleem/auction-system/engine/top_k"
)

const leaderboardSize = 10

type request struct {
	bid   cfg.Bid
	reply chan cfg.Result
}

type ctrlCmd struct {
	op    string // "configure" or "close"
	conf  cfg.AuctionConfig
	reply chan cfg.AuctionState
}

// Engine holds all live data for a single auction.
type Engine struct {
	conf cfg.AuctionConfig
	now  func() time.Time
	topk *top_k.TopK
	bids chan cfg.AcceptedBid

	state   cfg.AuctionState
	seen    map[string]struct{}
	winners []cfg.Bid

	in     chan request
	ctrl   chan ctrlCmd
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
		in:     make(chan request, 4096),
		ctrl:   make(chan ctrlCmd, 64),
		events: make(chan cfg.Event, 4096),
		done:   make(chan struct{}),
	}
	go e.run()
	return e
}

func (e *Engine) Bids() <-chan cfg.AcceptedBid { return e.bids }
func (e *Engine) Events() <-chan cfg.Event       { return e.events }
func (e *Engine) Done() <-chan struct{}          { return e.done }
func (e *Engine) Winners() []cfg.Bid             { <-e.done; return e.winners }

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

// Configure updates the auction settings and resets live auction state.
func (e *Engine) Configure(ctx context.Context, newConf cfg.AuctionConfig) (cfg.AuctionState, error) {
	cmd := ctrlCmd{op: "configure", conf: newConf, reply: make(chan cfg.AuctionState, 1)}
	select {
	case e.ctrl <- cmd:
	case <-e.done:
		return e.state, nil
	case <-ctx.Done():
		return cfg.AuctionState{}, ctx.Err()
	}

	select {
	case st := <-cmd.reply:
		return st, nil
	case <-e.done:
		return e.state, nil
	case <-ctx.Done():
		return cfg.AuctionState{}, ctx.Err()
	}
}

// CloseNow closes the auction immediately.
func (e *Engine) CloseNow(ctx context.Context) (cfg.AuctionState, error) {
	cmd := ctrlCmd{op: "close", reply: make(chan cfg.AuctionState, 1)}
	select {
	case e.ctrl <- cmd:
	case <-e.done:
		return e.state, nil
	case <-ctx.Done():
		return cfg.AuctionState{}, ctx.Err()
	}

	select {
	case st := <-cmd.reply:
		return st, nil
	case <-e.done:
		return e.state, nil
	case <-ctx.Done():
		return cfg.AuctionState{}, ctx.Err()
	}
}

func resetTimer(t *time.Timer, d time.Duration) *time.Timer {
	if d < 0 {
		d = 0
	}
	if t == nil {
		return time.NewTimer(d)
	}
	if !t.Stop() {
		select {
		case <-t.C:
		default:
		}
	}
	t.Reset(d)
	return t
}

func armEndTimer(t *time.Timer, endAt time.Time, now time.Time) (*time.Timer, <-chan time.Time) {
	if endAt.IsZero() {
		if t != nil && !t.Stop() {
			select {
			case <-t.C:
			default:
			}
		}
		return t, nil
	}
	t = resetTimer(t, endAt.Sub(now))
	return t, t.C
}

// ---------- the owner goroutine ----------

func (e *Engine) run() {
	now := e.now()
	var startCh <-chan time.Time
	var startTimer *time.Timer
	if !e.conf.StartAt.IsZero() && now.Before(e.conf.StartAt) {
		startTimer = resetTimer(startTimer, e.conf.StartAt.Sub(now))
		startCh = startTimer.C
	} else {
		e.state.Status = cfg.StatusOpen
	}

	var endTimer *time.Timer
	var endCh <-chan time.Time
	endTimer, endCh = armEndTimer(nil, e.conf.EndAt, now)

	defer func() {
		if startTimer != nil {
			startTimer.Stop()
		}
		if endTimer != nil {
			endTimer.Stop()
		}
	}()

	for {
		select {
		case <-startCh:
			e.state.Status = cfg.StatusOpen
			startCh = nil

		case req := <-e.in:
			req.reply <- e.handle(req.bid)

		case cmd := <-e.ctrl:
			switch cmd.op {
			case "configure":
				e.conf = cmd.conf
				e.topk = top_k.New(cmd.conf)
				e.seen = make(map[string]struct{})
				e.winners = nil

				now := e.now()
				status := cfg.StatusOpen
				if !cmd.conf.StartAt.IsZero() && now.Before(cmd.conf.StartAt) {
					status = cfg.StatusScheduled
					startTimer = resetTimer(startTimer, cmd.conf.StartAt.Sub(now))
					startCh = startTimer.C
				} else {
					startCh = nil
				}

				endTimer, endCh = armEndTimer(endTimer, cmd.conf.EndAt, now)

				e.state = cfg.AuctionState{
					AuctionID: cmd.conf.AuctionID,
					Title:     cmd.conf.Title,
					StartAt:   cmd.conf.StartAt,
					Price:     cmd.conf.MinimumPrice,
					Version:   e.state.Version + 1,
					StockLeft: cmd.conf.Stock,
					EndAt:     cmd.conf.EndAt,
					Status:    status,
					Top:       nil,
				}
				e.emit(cfg.Event{
					Kind:      cfg.EventConfigured,
					AuctionID: e.conf.AuctionID,
					Price:     e.state.Price,
					StockLeft: e.state.StockLeft,
					Version:   e.state.Version,
					At:        now,
				}, true)
				cmd.reply <- e.state

			case "close":
				e.close()
				cmd.reply <- e.state
				return
			}

		case <-endCh:
			e.close()
			return
		}
	}
}

func (e *Engine) handle(b cfg.Bid) cfg.Result {
	now := e.now()
	b.ReceivedAt = now

	if !e.conf.StartAt.IsZero() && now.Before(e.conf.StartAt) {
		return e.reject(b, cfg.ReasonNotStarted)
	}
	if !e.conf.EndAt.IsZero() && !now.Before(e.conf.EndAt) {
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
	e.state.StockLeft = e.conf.Stock - int(e.topk.Len())

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
		Version: e.state.Version,
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
		e.events <- ev
		return
	}
	select {
	case e.events <- ev:
	default:
	}
}