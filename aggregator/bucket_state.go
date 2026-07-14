package main

import "time"

type BucketState struct {
	ClientID    string
	Tokens      float64
	LastRefill  time.Time
	LastUpdated time.Time
}
