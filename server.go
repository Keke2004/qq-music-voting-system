package main

import (
	"context"
	"log"
	"net"
	"sync"
	"vote/cache"
	"vote/proto"

	"github.com/go-redis/redis/v8"
	"google.golang.org/grpc"
)

type VotingServiceServer struct {
	proto.UnimplementedVotingServiceServer
}

var voteLock sync.Mutex

func (s *VotingServiceServer) Vote(ctx context.Context, req *proto.VoteRequest) (*proto.VoteResponse, error) {
	voteLock.Lock()
	defer voteLock.Unlock()
	starID := req.GetStarId()
	userID := req.GetUserId()
	err := cache.IncrementVote(starID, userID)
	if err != nil {
		return nil, err
	}
	return &proto.VoteResponse{Message: "Vote successful!"}, nil
}
func (s *VotingServiceServer) GetLeaderboard(ctx context.Context, req *proto.LeaderboardRequest) (*proto.LeaderboardResponse, error) {
	allRankings, err := cache.GetAllRankings()
	if err != nil {
		return nil, err
	}
	leaderboard := cache.MergeAndSortRankings(allRankings)
	return &proto.LeaderboardResponse{Rankings: leaderboard}, nil
}
func (s *VotingServiceServer) ClearLeaderboard(ctx context.Context, req *proto.ClearLeaderboardRequest) (*proto.ClearLeaderboardResponse, error) {
	err := cache.ClearLeaderboard()
	if err != nil {
		return nil, err
	}
	return &proto.ClearLeaderboardResponse{Message: "Clear successful!"}, nil
}
func main() {
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	cache.InitRedis(rdb)
	server := grpc.NewServer()
	proto.RegisterVotingServiceServer(server, &VotingServiceServer{})
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	log.Printf("Server listening on %v", lis.Addr())
	if err := server.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
