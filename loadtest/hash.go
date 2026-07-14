package main

import "hash/fnv"

// primaryIndexFor mirrors edge/load_balancer.go's primaryIndex: same
// algorithm, so scenarios can predict which aggregator a given client_id
// would be routed to without going through the edge.
func primaryIndexFor(clientID string, n int) int {
	h := fnv.New32a()
	h.Write([]byte(clientID))
	return int(h.Sum32() % uint32(n))
}
