package main

import (
	"context"
	"log"
	"math/rand"
	"sync"
	"testing"
	"time"
	"vote/proto"

	"google.golang.org/grpc"
)

func BenchmarkVotingService(b *testing.B) {
	conn, err := grpc.Dial("localhost:50051", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	client := proto.NewVotingServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	_, err = client.ClearLeaderboard(ctx, &proto.ClearLeaderboardRequest{})
	if err != nil {
		b.Fatalf("could not clear leaderboard: %v", err)
	}
	rand.Seed(time.Now().UnixNano())
	wg := sync.WaitGroup{}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			starID := int32(rand.Intn(10) + 1)
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			_, err := client.Vote(ctx, &proto.VoteRequest{StarId: starID})
			if err != nil {
				b.Errorf("could not vote: %v", err)
			}
		}()
	}
	wg.Wait()
	b.StopTimer()
}
