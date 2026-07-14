package main

import (
	"flag"
	"fmt"
	"log"
	"net"

	"distributed-rate-limiter/internal/config"
	pb "distributed-rate-limiter/proto"

	"google.golang.org/grpc"
)

func main() {
	port := flag.Int("port", 50051, "Aggregator listening port")
	nodeID := flag.String("id", "node1", "Aggregator node ID")
	flag.Parse()

	address := fmt.Sprintf(":%d", *port)
	lis, err := net.Listen("tcp", address)

	//var peers []pb.RateLimiterClient
	if err != nil {
		log.Fatalf("cannot listen: %v", err)
	}

	peerAddrs := config.GetEnvList("PEER_ADDRESSES", []string{
		":50051",
		":50052",
	})

	selfAddr := config.GetEnv("SELF_ADDRESS", fmt.Sprintf(":%d", *port))

	var peers []*GossipClient

	for _, addr := range peerAddrs {
		if addr == selfAddr {
			continue
		}

		client, err := NewGossipClient(addr)
		if err != nil {
			log.Fatalf("cannot connect to peer %s: %v", addr, err)
		}
		peers = append(peers, client)
	}

	grpcServer := grpc.NewServer()
	manager := NewBucketManager(*nodeID)

	gossip := NewGossipService(*nodeID, manager, peers)
	go gossip.Start()

	pb.RegisterRateLimiterServer(
		grpcServer,
		&RateLimiterServer{
			manager: manager,
			nodeID:  *nodeID,
		},
	)
	//peers = append(peers, client)

	log.Printf("[%s] Aggregator listening on %s", *nodeID, address)

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("server error: %v", err)
	}
}
