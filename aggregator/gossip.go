package main

import (
	pb "distributed-rate-limiter/proto"
	"log"
	"time"
)

type GossipService struct {
	nodeID  string
	manager *BucketManager

	peers []*GossipClient
}

func NewGossipService(
	nodeID string,
	manager *BucketManager,
	peers []*GossipClient,
) *GossipService {

	return &GossipService{
		nodeID:  nodeID,
		manager: manager,
		peers:   peers,
	}
}

func (g *GossipService) Start() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		log.Printf("[%s] Starting gossip round...", g.nodeID)
		// 1. snapshot (READ ONLY)
		snapshot := g.manager.SnapshotModified()
		if len(snapshot) == 0 {
			continue
		}
		log.Printf("[%s] Sending %d modified buckets", g.nodeID, len(snapshot))

		// 2. build payload
		pbBuckets := make([]*pb.BucketState, 0, len(snapshot))

		for _, state := range snapshot {
			pbBuckets = append(pbBuckets, &pb.BucketState{
				ClientId:    state.ClientID,
				Tokens:      state.Tokens,
				LastRefill:  state.LastRefill.UnixMilli(),
				LastUpdated: state.LastUpdated.UnixMilli(),
			})
		}

		msg := &pb.GossipMessage{
			Sender:  g.nodeID,
			Buckets: pbBuckets,
		}

		// 3. gossip send
		success := false

		for _, peer := range g.peers {
			err := peer.Gossip(msg)
			if err != nil {
				log.Printf("[%s] Gossip failed: %v", g.nodeID, err)
				continue
			}
			success = true
		}

		// 4. reset dirty SOLO se almeno un gossip è andato a buon fine
		if success {
			for _, state := range snapshot {
				bucket, exists := g.manager.buckets[state.ClientID]
				if !exists {
					continue
				}

				bucket.mu.Lock()
				bucket.Dirty = false
				bucket.mu.Unlock()
			}
		}
	}
}
