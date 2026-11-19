package store

import (
	"context"
	"sync"

	cfg "github.com/jibransaleem/auction-system/engine/configuration"
)

type MemBidLog struct {
	mu   sync.Mutex
	Bids []cfg.AcceptedBid
}

func NewMemBidLog() *MemBidLog {
	return &MemBidLog{}
}

func (l *MemBidLog) Append(_ context.Context, bids []cfg.AcceptedBid) error {
	if l == nil || len(bids) == 0 {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.Bids = append(l.Bids, bids...)
	return nil
}

func (l *MemBidLog) Snapshot() []cfg.AcceptedBid {
	if l == nil {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]cfg.AcceptedBid(nil), l.Bids...)
}