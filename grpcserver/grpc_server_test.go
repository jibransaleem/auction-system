package grpcserver

import (
	"context"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	cfg "github.com/jibransaleem/auction-system/engine/configuration"
	"github.com/jibransaleem/auction-system/pb"
)

type fakeSub struct {
	res cfg.Result
	err error
	got cfg.Bid
}

func (f *fakeSub) Submit(_ context.Context, b cfg.Bid) (cfg.Result, error) {
	f.got = b
	return f.res, f.err
}

type fakeState struct {
	st  cfg.AuctionState
	err error
}

func (f *fakeState) LoadState(context.Context, string) (cfg.AuctionState, error) {
	return f.st, f.err
}

func validReq() *pb.PlaceBidRequest {
	return &pb.PlaceBidRequest{BidId: "b1", AuctionId: "a1", BidderId: "u1", Amount: 150}
}

func TestPlaceBid_MapsFields(t *testing.T) {
	sub := &fakeSub{res: cfg.Result{BidID: "b1", Accepted: true, Price: 160, Version: 4, Rank: 2}}
	s := New(sub, &fakeState{})

	resp, err := s.PlaceBid(context.Background(), validReq())
	if err != nil {
		t.Fatal(err)
	}
	if !resp.Accepted || resp.Price != 160 || resp.Version != 4 || resp.Rank != 2 {
		t.Fatalf("bad response: %+v", resp)
	}
	if sub.got.BidderID != "u1" || sub.got.Amount != 150 {
		t.Fatalf("bid not passed correctly: %+v", sub.got)
	}
}

func TestPlaceBid_InvalidInput(t *testing.T) {
	s := New(&fakeSub{}, &fakeState{})

	bad := []*pb.PlaceBidRequest{
		{BidId: "", AuctionId: "a1", BidderId: "u1", Amount: 10},
		{BidId: "b1", AuctionId: "a1", BidderId: "", Amount: 10},
		{BidId: "b1", AuctionId: "a1", BidderId: "u1", Amount: 0},
		{BidId: "b1", AuctionId: "a1", BidderId: "u1", Amount: -5},
	}
	for i, r := range bad {
		_, err := s.PlaceBid(context.Background(), r)
		if status.Code(err) != codes.InvalidArgument {
			t.Fatalf("case %d: want InvalidArgument, got %v", i, err)
		}
	}
}

func TestPlaceBid_RejectionIsNotAnError(t *testing.T) {
	sub := &fakeSub{res: cfg.Result{BidID: "b1", Reason: cfg.ReasonTooLow, Price: 200}}
	s := New(sub, &fakeState{})

	resp, err := s.PlaceBid(context.Background(), validReq())
	if err != nil {
		t.Fatalf("rejection must not be a gRPC error: %v", err)
	}
	if resp.Accepted || resp.Reason != "too_low" {
		t.Fatalf("bad response: %+v", resp)
	}
}

func TestPlaceBid_Timeout(t *testing.T) {
	sub := &fakeSub{err: context.DeadlineExceeded}
	s := New(sub, &fakeState{})

	_, err := s.PlaceBid(context.Background(), validReq())
	if status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("want DeadlineExceeded, got %v", err)
	}
}

func TestGetState_NotFound(t *testing.T) {
	s := New(&fakeSub{}, &fakeState{}) // zero state = missing key
	_, err := s.GetState(context.Background(), &pb.GetStateRequest{AuctionId: "nope"})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("want NotFound, got %v", err)
	}
}