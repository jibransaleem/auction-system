$bids = @(
  '{"bid_id":"b1","auction_id":"a70","bidder_id":"u1","amount":120}',
  '{"bid_id":"b2","auction_id":"a70","bidder_id":"u2","amount":130}',
  '{"bid_id":"b3","auction_id":"a70","bidder_id":"u3","amount":140}',
  '{"bid_id":"b3","auction_id":"a70","bidder_id":"u3","amount":140}',
  '{"bid_id":"b4","auction_id":"a70","bidder_id":"u4","amount":50}'
)

foreach ($b in $bids) {
  $b | Set-Content bid.json
  Write-Host $b
  Get-Content bid.json | grpcurl -plaintext -d '@' localhost:50051 auction.v1.BidEngine/PlaceBid
}