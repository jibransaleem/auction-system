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

func (l *MemBidLog) Append(_ context.Context, bids []cfg.AcceptedBid) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.Bids = append(l.Bids, bids...)
	return nil
}

func (l *MemBidLog) Snapshot() []cfg.AcceptedBid {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]cfg.AcceptedBid(nil), l.Bids...)
}