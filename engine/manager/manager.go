package manager

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"

	"github.com/jibransaleem/auction-system/engine"
	cfg "github.com/jibransaleem/auction-system/engine/configuration"
)

type Store interface {
	SaveState(ctx context.Context, st cfg.AuctionState) error
}

type Publisher interface {
	Publish(ctx context.Context, ev cfg.Event) error
}

type noopPub struct{}

func (noopPub) Publish(context.Context, cfg.Event) error { return nil }

type BidLog interface {
	Append(ctx context.Context, bids []cfg.AcceptedBid) error
}

type noopLog struct{}

func (noopLog) Append(context.Context, []cfg.AcceptedBid) error { return nil }

type Manager struct {
	mu       sync.RWMutex
	bidlog   BidLog
	logDone  chan struct{}
	conf     cfg.AuctionConfig
	engine   *engine.Engine
	store    Store
	pub      Publisher
	pumpDone chan struct{}
	nextVer  uint64
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

	eng := engine.New(conf)
	m := &Manager{
		conf:     conf,
		engine:   eng,
		store:    store,
		pub:      pub,
		bidlog:   bl,
		pumpDone: make(chan struct{}),
		logDone:  make(chan struct{}),
		nextVer:  1,
	}
	go m.pump(eng)
	go m.bidLogger(eng)
	return m, nil
}

func (m *Manager) getConfAndEngine() (cfg.AuctionConfig, *engine.Engine) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.conf, m.engine
}

func (m *Manager) Submit(ctx context.Context, b cfg.Bid) (cfg.Result, error) {
	conf, eng := m.getConfAndEngine()
	if b.AuctionID != conf.AuctionID {
		return cfg.Result{BidID: b.BidID, Reason: cfg.ReasonNotFound}, nil
	}
	return eng.Submit(ctx, b)
}

func (m *Manager) ConfigureAuction(ctx context.Context, newConf cfg.AuctionConfig) (cfg.AuctionState, error) {
	if newConf.Stock < 1 {
		return cfg.AuctionState{}, errors.New("stock must be >= 1")
	}

	m.mu.Lock()
	if newConf.AuctionID == "" {
		newConf.AuctionID = m.conf.AuctionID
	}
	if newConf.Title == "" {
		newConf.Title = m.conf.Title
	}
	if newConf.FinalWindow == 0 {
		newConf.FinalWindow = m.conf.FinalWindow
	}
	m.conf = newConf
	eng := m.engine

	select {
	case <-eng.Done():
		fresh := engine.New(newConf)
		m.engine = fresh
		m.pumpDone = make(chan struct{})
		m.logDone = make(chan struct{})
		ver := m.nextVer
		m.nextVer++
		m.mu.Unlock()

		go m.pump(fresh)
		go m.bidLogger(fresh)

		status := cfg.StatusOpen
		if !newConf.StartAt.IsZero() && time.Now().Before(newConf.StartAt) {
			status = cfg.StatusScheduled
		}
		st := cfg.AuctionState{
			AuctionID: newConf.AuctionID,
			Title:     newConf.Title,
			StartAt:   newConf.StartAt,
			Price:     newConf.MinimumPrice,
			Version:   ver,
			StockLeft: newConf.Stock,
			EndAt:     newConf.EndAt,
			Status:    status,
		}
		m.saveAndPublishConfig(st, newConf)
		return st, nil
	default:
		m.mu.Unlock()
	}

	st, err := eng.Configure(ctx, newConf)
	if err != nil {
		return cfg.AuctionState{}, err
	}

	m.mu.Lock()
	if st.Version < m.nextVer {
		st.Version = m.nextVer
	}
	m.nextVer = st.Version + 1
	m.mu.Unlock()

	m.saveAndPublishConfig(st, newConf)
	return st, nil
}

func (m *Manager) CloseAuction(ctx context.Context, auctionID string) (cfg.AuctionState, error) {
	conf, eng := m.getConfAndEngine()
	if auctionID != "" && auctionID != conf.AuctionID {
		return cfg.AuctionState{}, errors.New("auction not found")
	}
	return eng.CloseNow(ctx)
}

func (m *Manager) saveAndPublishConfig(st cfg.AuctionState, conf cfg.AuctionConfig) {
	sctx, scancel := context.WithTimeout(context.Background(), 3*time.Second)
	if err := m.store.SaveState(sctx, st); err != nil {
		log.Printf("save configured state v%d: %v", st.Version, err)
	}
	scancel()

	var startsMs int64
	if !conf.StartAt.IsZero() {
		startsMs = conf.StartAt.UnixMilli()
	}
	var endsMs int64
	var durSec int64
	if !conf.EndAt.IsZero() {
		endsMs = conf.EndAt.UnixMilli()
		if !conf.StartAt.IsZero() {
			durSec = int64(conf.EndAt.Sub(conf.StartAt).Seconds())
		}
	}

	pctx, pcancel := context.WithTimeout(context.Background(), 3*time.Second)
	err := m.pub.Publish(pctx, cfg.Event{
		Kind:        cfg.EventConfigured,
		AuctionID:   st.AuctionID,
		Title:       conf.Title,
		Status:      string(st.Status),
		Price:       st.Price,
		StartPrice:  conf.MinimumPrice,
		MinRaise:    conf.MinIncrement,
		WinnerCount: conf.Stock,
		StockLeft:   st.StockLeft,
		Version:     st.Version,
		StartsAtMs:  startsMs,
		EndsAtMs:    endsMs,
		DurationSec: durSec,
		At:          time.Now(),
	})
	pcancel()
	if err != nil {
		log.Printf("publish configured state v%d: %v", st.Version, err)
	}
}

func (m *Manager) bidLogger(eng *engine.Engine) {
	defer close(m.logDone)
	batch := make([]cfg.AcceptedBid, 0, 256)

	for first := range eng.Bids() {
		batch = append(batch[:0], first)
	fill:
		for len(batch) < 256 {
			select {
			case b, ok := <-eng.Bids():
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
	if len(batch) == 0 {
		return
	}
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

func (m *Manager) Done() <-chan struct{} {
	m.mu.RLock()
	pumpDone := m.pumpDone
	logDone := m.logDone
	m.mu.RUnlock()

	d := make(chan struct{})
	go func() { <-pumpDone; <-logDone; close(d) }()
	return d
}

func (m *Manager) pump(eng *engine.Engine) {
	defer close(m.pumpDone)

	for ev := range eng.Events() {
	drain:
		for ev.Kind != cfg.EventClosed {
			select {
			case next, ok := <-eng.Events():
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
	m.mu.Lock()
	conf := m.conf
	if ev.Version < m.nextVer {
		ev.Version = m.nextVer
	}
	m.nextVer = ev.Version + 1
	m.mu.Unlock()

	status := cfg.StatusOpen
	if ev.Kind == cfg.EventClosed {
		status = cfg.StatusClosed
	} else if ev.Kind == cfg.EventConfigured {
		status = cfg.AuctionStatus(ev.Status)
		if status == "" {
			status = cfg.StatusOpen
		}
	} else if !conf.StartAt.IsZero() && time.Now().Before(conf.StartAt) {
		status = cfg.StatusScheduled
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	err := m.store.SaveState(ctx, cfg.AuctionState{
		AuctionID: ev.AuctionID,
		Title:     conf.Title,
		StartAt:   conf.StartAt,
		Price:     ev.Price,
		Version:   ev.Version,
		StockLeft: ev.StockLeft,
		EndAt:     conf.EndAt,
		Status:    status,
		Top:       ev.Top,
	})
	cancel()
	if err != nil {
		log.Printf("save state v%d: %v", ev.Version, err)
	}

	if ev.Title == "" {
		ev.Title = conf.Title
	}
	if ev.Status == "" {
		ev.Status = string(status)
	}
	if ev.StartsAtMs == 0 && !conf.StartAt.IsZero() {
		ev.StartsAtMs = conf.StartAt.UnixMilli()
	}
	if ev.EndsAtMs == 0 && !conf.EndAt.IsZero() {
		ev.EndsAtMs = conf.EndAt.UnixMilli()
	}

	ctx, cancel = context.WithTimeout(context.Background(), 3*time.Second)
	if err := m.pub.Publish(ctx, ev); err != nil {
		log.Printf("publish v%d: %v", ev.Version, err)
	}
	cancel()
}