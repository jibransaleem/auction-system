package grpcserver

import (
	"context"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	cfg "github.com/jibransaleem/auction-system/engine/configuration"
	"github.com/jibransaleem/auction-system/pb"
)

// Submitter is implemented by *manager.Manager.
type Submitter interface {
	Submit(ctx context.Context, b cfg.Bid) (cfg.Result, error)
}

// StateReader is implemented by *store.RedisStore.
type StateReader interface {
	LoadState(ctx context.Context, id string) (cfg.AuctionState, error)
}

type Server struct {
	pb.UnimplementedBidEngineServer
	sub   Submitter
	state StateReader
}

func New(sub Submitter, state StateReader) *Server {
	return &Server{sub: sub, state: state}
}

func (s *Server) PlaceBid(ctx context.Context, req *pb.PlaceBidRequest) (*pb.PlaceBidResponse, error) {
	if req.GetBidId() == "" || req.GetAuctionId() == "" || req.GetBidderId() == "" {
		return nil, status.Error(codes.InvalidArgument, "bid_id, auction_id and bidder_id are required")
	}
	if req.GetAmount() <= 0 {
		return nil, status.Error(codes.InvalidArgument, "amount must be > 0")
	}

	// safety net in case the caller forgot to set a deadline
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	res, err := s.sub.Submit(ctx, cfg.Bid{
		BidID:     req.GetBidId(),
		AuctionID: req.GetAuctionId(),
		BidderID:  req.GetBidderId(),
		Amount:    req.GetAmount(),
		// ReceivedAt is stamped by the engine
	})
	if err != nil {
		return nil, status.FromContextError(err).Err()
	}

	// too_low / closed / duplicate are NORMAL answers, not gRPC errors
	return &pb.PlaceBidResponse{
		BidId:    res.BidID,
		Accepted: res.Accepted,
		Reason:   string(res.Reason),
		Price:    res.Price,
		Version:  res.Version,
		Rank:     int32(res.Rank),
	}, nil
}

func (s *Server) GetState(ctx context.Context, req *pb.GetStateRequest) (*pb.GetStateResponse, error) {
	if req.GetAuctionId() == "" {
		return nil, status.Error(codes.InvalidArgument, "auction_id is required")
	}

	st, err := s.state.LoadState(ctx, req.GetAuctionId())
	if err != nil {
		return nil, status.Error(codes.Unavailable, "state store unavailable")
	}
	if st.AuctionID == "" {
		return nil, status.Error(codes.NotFound, "auction not found")
	}

	return &pb.GetStateResponse{
		AuctionId:    st.AuctionID,
		Price:        st.Price,
		Version:      st.Version,
		StockLeft:    int32(st.StockLeft),
		EndsAtUnixMs: st.EndAt.UnixMilli(),
		Status:       string(st.Status),
	}, nil
}