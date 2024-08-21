package cache

import (
	"context"
	"fmt"
	"log"
	"sort"
	"strconv"
	"time"
	"vote/proto"

	"github.com/go-redis/redis/v8"
)

var redisClient *redis.Client

func InitRedis(client *redis.Client) {
	redisClient = client
}
func GetShardKey(starID int32) string {
	return fmt.Sprintf("shard:%d", starID%10)
}
func IncrementVote(ctx context.Context, starID int32, userID string) error {
	shardKey := GetShardKey(starID)
	voteKey := fmt.Sprintf("%s:votes", shardKey)
	userKey := fmt.Sprintf("%s:user:%s", shardKey, userID)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	pipe := redisClient.TxPipeline()
	userVoteExists := pipe.SetNX(ctx, userKey, 1, 24*time.Hour)
	member := fmt.Sprintf("star:%d", starID)
	pipe.HIncrBy(ctx, voteKey, member, 1)
	_, err := pipe.Exec(ctx)
	if err != nil {
		return err
	}
	if !userVoteExists.Val() {
		log.Printf("User %s has already voted for Star ID %d", userID, starID)
		return nil
	}
	log.Printf("Vote incremented successfully for StarID %d by UserID %s", starID, userID)
	return nil
}
func GetLeaderboard(ctx context.Context, shardKey string) ([]redis.Z, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	key := fmt.Sprintf("%s:votes", shardKey)
	log.Printf("Getting leaderboard from key %s", key)
	results, err := redisClient.HGetAll(ctx, key).Result()
	if err != nil {
		return nil, err
	}
	var leaderboard []redis.Z
	for starIDStr, votesStr := range results {
		votes, err := strconv.ParseFloat(votesStr, 64)
		if err != nil {
			return nil, err
		}
		leaderboard = append(leaderboard, redis.Z{Member: starIDStr, Score: votes})
	}
	sort.Slice(leaderboard, func(i, j int) bool {
		return leaderboard[i].Score > leaderboard[j].Score
	})
	return leaderboard, nil
}
func GetAllRankings(ctx context.Context) ([]redis.Z, error) {
	var allRankings []redis.Z
	for i := 0; i < 10; i++ {
		shardKey := fmt.Sprintf("shard:%d", i)
		rankings, err := GetLeaderboard(ctx, shardKey)
		if err != nil {
			return nil, err
		}
		log.Printf("Shard %d rankings: %v", i, rankings)
		allRankings = append(allRankings, rankings...)
	}
	return allRankings, nil
}
func MergeAndSortRankings(rankings []redis.Z) []*proto.StarRanking {
	voteMap := make(map[int32]int64)
	for _, rank := range rankings {
		starID, _ := parseStarID(rank.Member.(string))
		voteMap[starID] += int64(rank.Score)
	}
	var sortedRankings []*proto.StarRanking
	for starID, votes := range voteMap {
		sortedRankings = append(sortedRankings, &proto.StarRanking{StarId: starID, Votes: votes})
	}
	sort.Slice(sortedRankings, func(i, j int) bool {
		return sortedRankings[i].Votes > sortedRankings[j].Votes
	})
	return sortedRankings
}
func FlushVotesPeriodically() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		ctx := context.Background()
		log.Println("Flushing votes to the database")
		rankings, err := GetAllRankings(ctx)
		if err != nil {
			log.Printf("Error fetching rankings: %v", err)
			continue
		}
		log.Printf("Batch writing %d votes to the database", len(rankings))
	}
}
func parseStarID(member string) (int32, error) {
	var starID int32
	_, err := fmt.Sscanf(member, "star:%d", &starID)
	return starID, err
}
func ClearLeaderboard(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for i := 0; i < 10; i++ {
		shardKey := fmt.Sprintf("shard:%d", i)
		key := fmt.Sprintf("%s:votes", shardKey)
		log.Printf("Clearing leaderboard from key %s", key)
		if err := redisClient.Del(ctx, key).Err(); err != nil {
			return err
		}
	}
	return nil
}
