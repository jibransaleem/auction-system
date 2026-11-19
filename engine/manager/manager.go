package manager

import (
	"context"
	"errors"
	"log"
	"time"

	cfg "github.com/jibransaleem/auction-system/engine/configuration"
	"github.com/jibransaleem/auction-system/engine"
)

type Store interface {
	SaveState(ctx context.Context, st cfg.AuctionState) error
}

type Publisher interface {
	Publish(ctx context.Context, ev cfg.Event) error
}

// noopPub is used until the real Redis publisher exists (Module 6).
type noopPub struct{}

func (noopPub) Publish(context.Context, cfg.Event) error { return nil }

type BidLog interface {
	Append(ctx context.Context, bids []cfg.AcceptedBid) error
}

type noopLog struct{}

func (noopLog) Append(context.Context, []cfg.AcceptedBid) error { return nil }

type Manager struct {
	bidlog   BidLog
	logDone  chan struct{}
	conf     cfg.AuctionConfig
	engine   *engine.Engine
	store    Store
	pub      Publisher
	pumpDone chan struct{}
}

func New(conf cfg.AuctionConfig, store Store, pub Publisher, bl BidLog) (*Manager, error) {
	if conf.Stock < 1 {
		return nil, errors.New("stock must be >= 1")
	}
	if store == nil {
		return nil, errors.New("store is required")
	}
	if pub == nil {
		pub = noopPub{}
	}
	if bl == nil {
		bl = noopLog{}
	}

	status := cfg.StatusOpen
	if !conf.StartAt.IsZero() && time.Now().Before(conf.StartAt) {
		status = cfg.StatusScheduled
	}

	// Save the initial state BEFORE the engine starts taking bids.
	initial := cfg.AuctionState{
		AuctionID: conf.AuctionID,
		Title:     conf.Title,
		StartAt:   conf.StartAt,
		Price:     conf.MinimumPrice,
		Version:   0,
		StockLeft: conf.Stock,
		EndAt:     conf.EndAt,
		Status:    status,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := store.SaveState(ctx, initial); err != nil {
		return nil, err
	}

	m := &Manager{
		conf:     conf,
		engine:   engine.New(conf),
		store:    store,
		pub:      pub,
		bidlog:   bl,
		pumpDone: make(chan struct{}),
		logDone:  make(chan struct{}),
	}
	go m.pump()
	go m.bidLogger()
	return m, nil
}

func (m *Manager) Submit(ctx context.Context, b cfg.Bid) (cfg.Result, error) {
	if b.AuctionID != m.conf.AuctionID {
		return cfg.Result{BidID: b.BidID, Reason: cfg.ReasonNotFound}, nil
	}
	return m.engine.Submit(ctx, b)
}

func (m *Manager) bidLogger() {
	defer close(m.logDone)
	batch := make([]cfg.AcceptedBid, 0, 256)

	for first := range m.engine.Bids() {
		batch = append(batch[:0], first)
	fill:
		for len(batch) < 256 {
			select {
			case b, ok := <-m.engine.Bids():
				if !ok {
					break fill
				}
				batch = append(batch, b)
			default:
				break fill
			}
		}
		m.appendWithRetry(batch)
	}
}

func (m *Manager) appendWithRetry(batch []cfg.AcceptedBid) {
	wait := 100 * time.Millisecond
	for attempt := 1; attempt <= 6; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err := m.bidlog.Append(ctx, batch)
		cancel()
		if err == nil {
			return
		}
		log.Printf("bid log append failed (try %d): %v", attempt, err)
		time.Sleep(wait)
		wait *= 2
	}
	log.Printf("BID LOG LOST %d bids, first=%s", len(batch), batch[0].BidID)
}

// Done closes when the pump has exited (after the closed event). Useful in tests.
func (m *Manager) Done() <-chan struct{} {
	d := make(chan struct{})
	go func() { <-m.pumpDone; <-m.logDone; close(d) }()
	return d
}

// pump is the ONLY reader of the engine's events.
// For each event: coalesce rapid updates, save state to the store, then publish.
func (m *Manager) pump() {
	defer close(m.pumpDone)

	for ev := range m.engine.Events() {
		// skip ahead to the newest queued update (never skip past "closed")
	drain:
		for ev.Kind != cfg.EventClosed {
			select {
			case next, ok := <-m.engine.Events():
				if !ok {
					m.flush(ev)
					return
				}
				ev = next
			default:
				break drain
			}
		}

		m.flush(ev)

		if ev.Kind == cfg.EventClosed {
			return
		}
	}
}

func (m *Manager) flush(ev cfg.Event) {
	status := cfg.StatusOpen
	if ev.Kind == cfg.EventClosed {
		status = cfg.StatusClosed
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	err := m.store.SaveState(ctx, cfg.AuctionState{
		AuctionID: ev.AuctionID,
		Title:     m.conf.Title,
		StartAt:   m.conf.StartAt,
		Price:     ev.Price,
		Version:   ev.Version,
		StockLeft: ev.StockLeft,
		EndAt:     m.conf.EndAt,
		Status:    status,
		Top:       ev.Top,
	})
	cancel()
	if err != nil {
		log.Printf("save state v%d: %v", ev.Version, err)
	}

	ctx, cancel = context.WithTimeout(context.Background(), 3*time.Second)
	if err := m.pub.Publish(ctx, ev); err != nil {
		log.Printf("publish v%d: %v", ev.Version, err)
	}
	cancel()
}