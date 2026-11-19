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

// AdminController is optionally implemented by *manager.Manager.
type AdminController interface {
	ConfigureAuction(ctx context.Context, conf cfg.AuctionConfig) (cfg.AuctionState, error)
	CloseAuction(ctx context.Context, auctionID string) (cfg.AuctionState, error)
}

type Server struct {
	pb.UnimplementedBidEngineServer
	sub   Submitter
	state StateReader
	admin AdminController
}

func New(sub Submitter, state StateReader, admin ...AdminController) *Server {
	var a AdminController
	if len(admin) > 0 {
		a = admin[0]
	} else if ac, ok := sub.(AdminController); ok {
		a = ac
	}
	return &Server{sub: sub, state: state, admin: a}
}

func (s *Server) PlaceBid(ctx context.Context, req *pb.PlaceBidRequest) (*pb.PlaceBidResponse, error) {
	if req.GetBidId() == "" || req.GetAuctionId() == "" || req.GetBidderId() == "" {
		return nil, status.Error(codes.InvalidArgument, "bid_id, auction_id and bidder_id are required")
	}
	if req.GetAmount() <= 0 {
		return nil, status.Error(codes.InvalidArgument, "amount must be > 0")
	}

	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	res, err := s.sub.Submit(ctx, cfg.Bid{
		BidID:     req.GetBidId(),
		AuctionID: req.GetAuctionId(),
		BidderID:  req.GetBidderId(),
		Amount:    req.GetAmount(),
	})
	if err != nil {
		return nil, status.FromContextError(err).Err()
	}

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
		if ctx.Err() != nil {
			return nil, status.FromContextError(err).Err()
		}
		return nil, status.Error(codes.Unavailable, "state store unavailable")
	}
	if st.AuctionID == "" {
		return nil, status.Error(codes.NotFound, "auction not found")
	}

	var endsAtMs int64
	if !st.EndAt.IsZero() {
		endsAtMs = st.EndAt.UnixMilli()
	}

	return &pb.GetStateResponse{
		AuctionId:    st.AuctionID,
		Price:        st.Price,
		Version:      st.Version,
		StockLeft:    int32(st.StockLeft),
		EndsAtUnixMs: endsAtMs,
		Status:       string(st.Status),
	}, nil
}

func (s *Server) ConfigureAuction(ctx context.Context, req *pb.ConfigureAuctionRequest) (*pb.ConfigureAuctionResponse, error) {
	if s.admin == nil {
		return nil, status.Error(codes.Unimplemented, "admin control not configured")
	}
	if req.GetAuctionId() == "" {
		return nil, status.Error(codes.InvalidArgument, "auction_id is required")
	}
	if req.GetStartPrice() <= 0 {
		return nil, status.Error(codes.InvalidArgument, "start_price must be > 0")
	}

	now := time.Now()
	startAt := now
	if req.GetStartsAtMs() > 0 {
		startAt = time.UnixMilli(req.GetStartsAtMs())
	}

	var endAt time.Time
	if req.GetEndsAtMs() > 0 {
		endAt = time.UnixMilli(req.GetEndsAtMs())
	} else if req.GetDurationSec() > 0 {
		endAt = startAt.Add(time.Duration(req.GetDurationSec()) * time.Second)
	} else {
		endAt = startAt.Add(60 * time.Second)
	}

	minInc := req.GetMinRaise()
	if minInc <= 0 {
		minInc = 1
	}
	stock := int(req.GetWinnerCount())
	if stock <= 0 {
		stock = 1
	}

	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	st, err := s.admin.ConfigureAuction(ctx, cfg.AuctionConfig{
		AuctionID:    req.GetAuctionId(),
		Title:        req.GetTitle(),
		StartAt:      startAt,
		Stock:        stock,
		MinimumPrice: req.GetStartPrice(),
		MinIncrement: minInc,
		EndAt:        endAt,
	})
	if err != nil {
		if ctx.Err() != nil {
			return nil, status.FromContextError(err).Err()
		}
		return nil, status.Error(codes.Internal, err.Error())
	}

	var endsAtMs int64
	if !st.EndAt.IsZero() {
		endsAtMs = st.EndAt.UnixMilli()
	}

	return &pb.ConfigureAuctionResponse{
		Ok:           true,
		Status:       string(st.Status),
		Price:        st.Price,
		Version:      st.Version,
		StockLeft:    int32(st.StockLeft),
		EndsAtUnixMs: endsAtMs,
	}, nil
}

func (s *Server) CloseAuction(ctx context.Context, req *pb.CloseAuctionRequest) (*pb.CloseAuctionResponse, error) {
	if s.admin == nil {
		return nil, status.Error(codes.Unimplemented, "admin control not configured")
	}
	if req.GetAuctionId() == "" {
		return nil, status.Error(codes.InvalidArgument, "auction_id is required")
	}

	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	st, err := s.admin.CloseAuction(ctx, req.GetAuctionId())
	if err != nil {
		if ctx.Err() != nil {
			return nil, status.FromContextError(err).Err()
		}
		if err.Error() == "auction not found" {
			return nil, status.Error(codes.NotFound, err.Error())
		}
		return nil, status.Error(codes.Internal, err.Error())
	}

	var endsAtMs int64
	if !st.EndAt.IsZero() {
		endsAtMs = st.EndAt.UnixMilli()
	}

	return &pb.CloseAuctionResponse{
		Ok:           true,
		Status:       string(st.Status),
		Price:        st.Price,
		Version:      st.Version,
		StockLeft:    int32(st.StockLeft),
		EndsAtUnixMs: endsAtMs,
	}, nil
}