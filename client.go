package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"
	"vote/proto"

	"google.golang.org/grpc"
)

func main() {
	conn, err := grpc.Dial("localhost:50051", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := proto.NewVotingServiceClient(conn)
	_, err = c.ClearLeaderboard(context.Background(), &proto.ClearLeaderboardRequest{})
	if err != nil {
		log.Fatalf("could not clear leaderboard: %v", err)
	}
	rand.Seed(time.Now().UnixNano())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			simulateVoting(c)
			time.Sleep(1 * time.Second)
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			printLeaderboard(c)
			time.Sleep(5 * time.Second)
		}
	}()
	wg.Wait()
}
func simulateVoting(c proto.VotingServiceClient) {
	var wg sync.WaitGroup
	voteCount := 100000
	for i := 0; i < voteCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			starID := int32(rand.Intn(10) + 1)
			_, err := c.Vote(context.Background(), &proto.VoteRequest{StarId: starID})
			if err != nil {
				log.Printf("could not vote: %v", err)
			}
		}()
	}
	wg.Wait()
}
func printLeaderboard(c proto.VotingServiceClient) {
	resp, err := c.GetLeaderboard(context.Background(), &proto.LeaderboardRequest{})
	if err != nil {
		log.Fatalf("could not get leaderboard: %v", err)
	}
	fmt.Println("Leaderboard:")
	for i, rank := range resp.GetRankings() {
		fmt.Printf("No.%.2d: StarID: %.2d, Votes: %d\n", i+1, rank.GetStarId(), rank.GetVotes())
	}
	fmt.Println("End of Leaderboard")
}