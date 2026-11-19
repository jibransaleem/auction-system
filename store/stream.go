package store

import (
	"context"

	"github.com/redis/go-redis/v9"

	cfg "github.com/jibransaleem/auction-system/engine/configuration"
)

type RedisBidLog struct{ rdb *redis.Client }

func NewRedisBidLog(rdb *redis.Client) *RedisBidLog { return &RedisBidLog{rdb: rdb} }

func bidStreamKey(id string) string { return "auction:" + id + ":bids" }

func (l *RedisBidLog) Append(ctx context.Context, bids []cfg.AcceptedBid) error {
	pipe := l.rdb.Pipeline()
	for _, b := range bids {
		pipe.XAdd(ctx, &redis.XAddArgs{
			Stream: bidStreamKey(b.AuctionID),
			Values: map[string]interface{}{
				"bid_id":    b.BidID,
				"bidder_id": b.BidderID,
				"amount":    b.Amount,
				"version":   b.Version,
				"at_ms":     b.At.UnixMilli(),
			},
		})
	}
	_, err := pipe.Exec(ctx)
	return err
}