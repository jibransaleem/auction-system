package top_k

import (
	"container/heap"
	"sort"

	cfg "github.com/jibransaleem/auction-system/engine/configuration"
)

// RULES 
// Tie-break: same amount means the earlier bid wins (compare ReceivedAt).
// One slot per bidder: a bidder who bids again raises their own bid. They never take two slots.
// Raising own bid needs at least old + MinIncrement.
// Not full yet: minimum is StartPrice. Full: minimum is worst + MinIncrement.
// Not thread-safe on purpose. Only the engine goroutine touches it 

type entry struct {
	bid   cfg.Bid
	index int // position in heap, needed for heap.Fix
}

// better: higher amount wins; on tie the earlier bid wins.
func worse(a, b cfg.Bid) bool {
	if a.Amount != b.Amount {
		return a.Amount < b.Amount
	}
	return a.ReceivedAt.After(b.ReceivedAt)
}

type minHeap []*entry

func (h minHeap) Len() int           { return len(h) }
func (h minHeap) Less(i, j int) bool { return worse(h[i].bid, h[j].bid) }
func (h minHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}
func (h *minHeap) Push(x any) {
	e := x.(*entry)
	e.index = len(*h)
	*h = append(*h, e)
}
func (h *minHeap) Pop() any {
	old := *h
	n := len(old)
	e := old[n-1]
	old[n-1] = nil
	*h = old[:n-1]
	return e
}

type TopK struct {
	k        int
	start    int64
	inc      int64
	h        minHeap
	byBidder map[string]*entry
}

func New(c cfg.AuctionConfig) *TopK {
	return &TopK{
		k:        c.Stock, // validate Stock >= 1 where you build the config
		start:    c.MinimumPrice,
		inc:      c.MinIncrement,
		byBidder: make(map[string]*entry, c.Stock),
	}
}

// Len returns the current number of items stored in the top-k heap.
func (t *TopK) Len() int {
	return t.h.Len()
}

type Outcome struct {
	Accepted bool
	Reason   cfg.RejectReason
	Evicted  *cfg.Bid // set when someone got pushed out
}

// MinToBeat is the lowest amount a NEW bidder can bid right now.
func (t *TopK) MinToBeat() int64 {
	if len(t.h) < t.k {
		return t.start
	}
	return t.h[0].bid.Amount + t.inc
}

func (t *TopK) Offer(b cfg.Bid) Outcome {
	// 1. bidder already holds a slot: raise own bid
	if e, ok := t.byBidder[b.BidderID]; ok {
		if b.Amount < e.bid.Amount+t.inc {
			return Outcome{Reason: cfg.ReasonTooLow}
		}
		e.bid = b
		heap.Fix(&t.h, e.index)
		return Outcome{Accepted: true}
	}

	// 2. new bidder must reach the minimum
	if b.Amount < t.MinToBeat() {
		return Outcome{Reason: cfg.ReasonTooLow}
	}

	// 3. free slot
	if len(t.h) < t.k {
		e := &entry{bid: b}
		heap.Push(&t.h, e)
		t.byBidder[b.BidderID] = e
		return Outcome{Accepted: true}
	}

	// 4. full: must strictly beat the worst (covers the tie case)
	if !worse(t.h[0].bid, b) {
		return Outcome{Reason: cfg.ReasonTooLow}
	}
	worst := heap.Pop(&t.h).(*entry)
	delete(t.byBidder, worst.bid.BidderID)

	e := &entry{bid: b}
	heap.Push(&t.h, e)
	t.byBidder[b.BidderID] = e
	return Outcome{Accepted: true, Evicted: &worst.bid}
}

// Snapshot returns winners best-first. Used at close and for Winner rows.
func (t *TopK) Snapshot() []cfg.Bid {
	out := make([]cfg.Bid, 0, len(t.h))
	for _, e := range t.h {
		out = append(out, e.bid)
	}
	sort.Slice(out, func(i, j int) bool { return worse(out[j], out[i]) })
	return out
}

// Rank is 1 = best, 0 = not in top K. O(K), fine for small K.
// Finds a specific bidder's position
// If a user wants to check, "Where do I stand right now in the auction?", this function tells them.
func (t *TopK) Rank(bidderID string) int {
	for i, b := range t.Snapshot() {
		if b.BidderID == bidderID {
			return i + 1
		}
	}
	return 0
}