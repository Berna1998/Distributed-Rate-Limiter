package main

import (
	"context"
	"log"
	"time"

	pb "distributed-rate-limiter/proto"
)

type RateLimiterServer struct {
	pb.UnimplementedRateLimiterServer
	manager *BucketManager
	nodeID  string
}

func (s *RateLimiterServer) CheckQuota(
	ctx context.Context,
	req *pb.QuotaRequest,
) (*pb.QuotaResponse, error) {

	log.Printf("[%s] Request received from client: %s", s.nodeID, req.ClientId)

	bucket := s.manager.GetBucket(req.ClientId)

	allowed := bucket.Allow()

	return &pb.QuotaResponse{
		Allowed:         allowed,
		RemainingTokens: int32(bucket.Tokens),
	}, nil
}

func (s *RateLimiterServer) Gossip(
	ctx context.Context,
	req *pb.GossipMessage,
) (*pb.GossipResponse, error) {

	log.Printf("[%s] Received gossip from %s", s.nodeID, req.Sender)

	states := make([]BucketState, 0, len(req.Buckets))

	for _, bucket := range req.Buckets {

		states = append(states, BucketState{
			ClientID:    bucket.ClientId,
			Tokens:      bucket.Tokens,
			LastRefill:  time.UnixMilli(bucket.LastRefill),
			LastUpdated: time.UnixMilli(bucket.LastUpdated),
		})
	}

	stat := s.manager.Merge(states)

	log.Printf(
		"[%s] Gossip merge result from %s | Created=%d Updated=%d Ignored=%d",
		s.nodeID,
		req.Sender,
		stat.Created,
		stat.Updated,
		stat.Ignored,
	)

	return &pb.GossipResponse{
		Success: true,
	}, nil
}
