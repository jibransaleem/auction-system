package configuration

import "time"

// Initial auction configuration.
type AuctionConfig struct {
	AuctionID    string        `json:"auction_id"`
	Title        string        `json:"title"`
	StartAt      time.Time     `json:"start_at"`
	Stock        int           `json:"stock"`
	MinimumPrice int64         `json:"minimum_price"`
	MinIncrement int64         `json:"min_increment"`
	EndAt        time.Time     `json:"end_at"`
	FinalWindow  time.Duration `json:"final_window"`
}

// InFinalWindow reports whether now falls within [EndAt-FinalWindow, EndAt).
func (c AuctionConfig) InFinalWindow(now time.Time) bool {
	if c.EndAt.IsZero() || c.FinalWindow <= 0 {
		return false
	}
	return !now.Before(c.EndAt.Add(-c.FinalWindow)) && now.Before(c.EndAt)
}

// Incoming bid.
type Bid struct {
	AuctionID  string    `json:"auction_id"`
	BidID      string    `json:"bid_id"`
	BidderID   string    `json:"bidder_id"`
	Amount     int64     `json:"bid_amount"`
	ReceivedAt time.Time `json:"received_at"`
}

// AcceptedBid is written to the Redis Stream for the persister.
type AcceptedBid struct {
	BidID     string    `json:"bid_id"`
	AuctionID string    `json:"auction_id"`
	BidderID  string    `json:"bidder_id"`
	Amount    int64     `json:"amount"`
	Version   uint64    `json:"version"`
	At        time.Time `json:"at"`
}

// Auction status.
type AuctionStatus string

const (
	StatusScheduled AuctionStatus = "scheduled"
	StatusOpen      AuctionStatus = "open"
	StatusClosed    AuctionStatus = "closed"
)

// Leaderboard entry.
type LeaderEntry struct {
	Rank     int    `json:"rank"`
	BidderID string `json:"bidder"`
	Amount   int64  `json:"amount"`
}

// Current auction state.
type AuctionState struct {
	AuctionID string        `json:"auction_id"`
	Title     string        `json:"title"`
	StartAt   time.Time     `json:"start_at"`
	Price     int64         `json:"price"`
	Version   uint64        `json:"version"`
	StockLeft int           `json:"stock_left"`
	EndAt     time.Time     `json:"end_at"`
	Status    AuctionStatus `json:"auction_status"`
	Top       []LeaderEntry `json:"top,omitempty"`
}

// Bid rejection reasons.
type RejectReason string

const (
	ReasonNone       RejectReason = ""
	ReasonNotStarted RejectReason = "not_started"
	ReasonTooLow     RejectReason = "too_low"
	ReasonClosed     RejectReason = "closed"
	ReasonDuplicate  RejectReason = "duplicate"
	ReasonNotFound   RejectReason = "auction_not_found"
)

// Result returned after processing a bid.
type Result struct {
	BidID    string       `json:"bid_id"`
	Accepted bool         `json:"accepted"`
	Reason   RejectReason `json:"reason,omitempty"`
	Price    int64        `json:"price"`
	Version  uint64       `json:"version"`
	Rank     int          `json:"rank,omitempty"`
}

// Events sent to connected clients.
type EventKind string

const (
	EventPriceUpdate EventKind = "price_update"
	EventClosed      EventKind = "closed"
	EventConfigured  EventKind = "auction_configured"
)

type Event struct {
	Kind        EventKind     `json:"kind"`
	AuctionID   string        `json:"auction_id"`
	Title       string        `json:"title,omitempty"`
	Status      string        `json:"status,omitempty"`
	Price       int64         `json:"price"`
	StartPrice  int64         `json:"start_price,omitempty"`
	MinRaise    int64         `json:"min_raise,omitempty"`
	WinnerCount int           `json:"winner_count,omitempty"`
	Version     uint64        `json:"version"`
	StockLeft   int           `json:"stock_left"`
	StartsAtMs  int64         `json:"starts_at_ms,omitempty"`
	EndsAtMs    int64         `json:"ends_at_ms,omitempty"`
	DurationSec int64         `json:"duration_sec,omitempty"`
	At          time.Time     `json:"at"`
	Top         []LeaderEntry `json:"top,omitempty"`
}

// Auction winner information.
type Winner struct {
	AuctionID string    `json:"auction_id"`
	BidderID  string    `json:"bidder_id"`
	Amount    int64     `json:"amount"`
	Rank      int       `json:"rank"`
	WonAt     time.Time `json:"won_at"`
}