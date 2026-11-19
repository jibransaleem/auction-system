package main

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	cfg "github.com/jibransaleem/auction-system/engine/configuration"
	"github.com/jibransaleem/auction-system/engine/manager"
	"github.com/jibransaleem/auction-system/store"
)

type testStateStore struct {
	mu    sync.Mutex
	state cfg.AuctionState
}

func (s *testStateStore) SaveState(_ context.Context, st cfg.AuctionState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state = st
	return nil
}

type testPub struct {
	t *testing.T
}

func (p testPub) Publish(_ context.Context, ev cfg.Event) error {
	p.t.Logf("[EVENT] kind=%-18s price=%-5d stock_left=%-2d ver=%-3d top=%+v",
		ev.Kind, ev.Price, ev.StockLeft, ev.Version, ev.Top)
	return nil
}

func TestThreeUsersBiddingAndClose(t *testing.T) {
	ctx := context.Background()
	stStore := &testStateStore{}
	bidLog := store.NewMemBidLog()

	conf := cfg.AuctionConfig{
		AuctionID:    "a1",
		Title:        "3-User Test Auction",
		MinimumPrice: 100,
		MinIncrement: 10,
		Stock:        2,
		StartAt:      time.Now(),
		EndAt:        time.Now().Add(30 * time.Second),
		FinalWindow:  5 * time.Second,
	}

	mgr, err := manager.New(conf, stStore, testPub{t: t}, bidLog)
	if err != nil {
		t.Fatalf("manager.New failed: %v", err)
	}

	bids := []struct {
		bid     cfg.Bid
		wantAcc bool
	}{
		{cfg.Bid{AuctionID: "a1", BidID: "b-1", BidderID: "u1", Amount: 100}, true},
		{cfg.Bid{AuctionID: "a1", BidID: "b-2", BidderID: "u2", Amount: 110}, true},
		{cfg.Bid{AuctionID: "a1", BidID: "b-3", BidderID: "u3", Amount: 120}, true},
		{cfg.Bid{AuctionID: "a1", BidID: "b-4", BidderID: "u1", Amount: 115}, false},
		{cfg.Bid{AuctionID: "a1", BidID: "b-5", BidderID: "u2", Amount: 105}, false},
		{cfg.Bid{AuctionID: "a1", BidID: "b-6", BidderID: "u2", Amount: 130}, true},
		{cfg.Bid{AuctionID: "a1", BidID: "b-7", BidderID: "u3", Amount: 140}, true},
	}

	for _, tc := range bids {
		res, err := mgr.Submit(ctx, tc.bid)
		if err != nil {
			t.Fatalf("Submit(%s) error: %v", tc.bid.BidID, err)
		}
		t.Logf("[BID]   id=%-4s user=%-2s amt=%-4d -> accepted=%-5v reason=%-10s price=%-4d ver=%-2d rank=%d",
			tc.bid.BidID, tc.bid.BidderID, tc.bid.Amount, res.Accepted, res.Reason, res.Price, res.Version, res.Rank)

		if res.Accepted != tc.wantAcc {
			t.Fatalf("bid %s: got accepted=%v, want %v (reason=%s)", tc.bid.BidID, res.Accepted, tc.wantAcc, res.Reason)
		}
	}

	finalState, err := mgr.CloseAuction(ctx, "a1")
	if err != nil {
		t.Fatalf("CloseAuction failed: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	accepted := bidLog.Snapshot()
	if len(accepted) != 5 {
		t.Fatalf("got %d accepted bids in log, want 5", len(accepted))
	}

	fmt.Println("=== FINAL WINNERS ===")
	for _, w := range finalState.Top {
		fmt.Printf("  Rank #%d: user=%s amount=%d\n", w.Rank, w.BidderID, w.Amount)
	}

	if len(finalState.Top) < 2 {
		t.Fatalf("expected at least 2 winners, got %d", len(finalState.Top))
	}
	if finalState.Top[0].BidderID != "u3" || finalState.Top[0].Amount != 140 {
		t.Fatalf("unexpected rank #1 winner: %+v", finalState.Top[0])
	}
	if finalState.Top[1].BidderID != "u2" || finalState.Top[1].Amount != 130 {
		t.Fatalf("unexpected rank #2 winner: %+v", finalState.Top[1])
	}
}