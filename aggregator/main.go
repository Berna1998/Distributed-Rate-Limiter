package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"

	"distributed-rate-limiter/internal/config"
	"distributed-rate-limiter/internal/tracing"
	pb "distributed-rate-limiter/proto"

	"google.golang.org/grpc"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
)

func main() {
	port := flag.Int("port", 50051, "Aggregator listening port")
	nodeID := flag.String("id", "node1", "Aggregator node ID")
	flag.Parse()

	shutdown, err := tracing.Init(context.Background(), *nodeID)
	if err != nil {
		log.Fatal(err)
	}
	defer shutdown(context.Background())

	address := fmt.Sprintf(":%d", *port)
	lis, err := net.Listen("tcp", address)

	//var peers []pb.RateLimiterClient
	if err != nil {
		log.Fatalf("cannot listen: %v", err)
	}

	selfAddr := config.GetEnv("SELF_ADDRESS", fmt.Sprintf("localhost:%d", *port))

	var peers []*GossipClient

	for _, addr := range config.AggregatorAddresses {
		if addr == selfAddr {
			continue
		}

		client, err := NewGossipClient(addr)
		if err != nil {
			log.Fatalf("cannot connect to peer %s: %v", addr, err)
		}
		peers = append(peers, client)
	}

	grpcServer := grpc.NewServer(grpc.StatsHandler(otelgrpc.NewServerHandler()))
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
