package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"testing"
	"time"
	"vote/proto"

	"google.golang.org/grpc"
)

const (
	concurrency  = 100
	totalRequest = 100000
)

func BenchmarkVotingService(b *testing.B) {
	conn, err := grpc.Dial("localhost:50051", grpc.WithInsecure())
	if err != nil {
		b.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := proto.NewVotingServiceClient(conn)
	ctx := context.Background()
	var wg sync.WaitGroup
	wg.Add(concurrency)
	requestsPerGoroutine := totalRequest / concurrency
	latencies := make([]time.Duration, totalRequest)
	startTime := time.Now()
	for i := 0; i < concurrency; i++ {
		go func(i int) {
			defer wg.Done()
			for j := 0; j < requestsPerGoroutine; j++ {
				starID := int32(rand.Intn(10) + 1)
				userID := fmt.Sprintf("user_%d", rand.Intn(1000000))
				start := time.Now()
				_, err := c.Vote(ctx, &proto.VoteRequest{StarId: starID, UserId: userID})
				if err != nil {
					log.Printf("could not vote: %v", err)
				}
				latencies[i*requestsPerGoroutine+j] = time.Since(start)
			}
		}(i)
	}
	wg.Wait()
	totalTime := time.Since(startTime)
	qps := float64(totalRequest) / totalTime.Seconds()
	var totalLatency time.Duration
	for _, latency := range latencies {
		totalLatency += latency
	}
	avgLatency := totalLatency / time.Duration(totalRequest)
	fmt.Printf("Total Requests: %d\n", totalRequest)
	fmt.Printf("Concurrency: %d\n", concurrency)
	fmt.Printf("Total Time: %.2fs\n", totalTime.Seconds())
	fmt.Printf("QPS: %.2f\n", qps)
	fmt.Printf("Average Latency: %s\n", avgLatency)
}
func TestMain(m *testing.M) {
	BenchmarkVotingService(nil)
}
