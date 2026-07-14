package main

import (
	pb "distributed-rate-limiter/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"time"

	"context"
)

type GossipClient struct {
	address string
	client  pb.RateLimiterClient
}

func NewGossipClient(address string) (*GossipClient, error) {

	conn, err := grpc.NewClient(
		address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)

	if err != nil {
		return nil, err
	}

	return &GossipClient{
		address: address,
		client:  pb.NewRateLimiterClient(conn),
	}, nil
}

func (g *GossipClient) Gossip(msg *pb.GossipMessage) error {

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	_, err := g.client.Gossip(ctx, msg)

	return err
}
