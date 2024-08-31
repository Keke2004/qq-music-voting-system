package cache

import (
	"context"
	"fmt"
	"log"
	"sort"
	"sync"
	"time"
	"vote/proto"

	"github.com/go-redis/redis/v8"
)

var (
	redisClient *redis.Client
	voteCache   = make(map[string]map[string]int64)
	cacheLock   sync.Mutex
)

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
	cacheLock.Lock()
	defer cacheLock.Unlock()
	userVoteExists, err := redisClient.SetNX(ctx, userKey, 1, 10*time.Second).Result()
	if err != nil {
		return err
	}
	if !userVoteExists {
		log.Printf("User %s has already voted for Star ID %d", userID, starID)
		return nil
	}
	if _, ok := voteCache[voteKey]; !ok {
		voteCache[voteKey] = make(map[string]int64)
	}
	member := fmt.Sprintf("star:%d", starID)
	voteCache[voteKey][member]++
	if len(voteCache[voteKey]) >= 1000 {
		if err := flushVoteCache(ctx); err != nil {
			return err
		}
	}
	log.Printf("Vote incremented successfully for StarID %d by UserID %s", starID, userID)
	return nil
}
func FlushVotesPeriodically() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		ctx := context.Background()
		if err := flushVoteCache(ctx); err != nil {
			log.Printf("Error flushing votes: %v", err)
		}
	}
}
func flushVoteCache(ctx context.Context) error {
	cacheLock.Lock()
	defer cacheLock.Unlock()
	pipe := redisClient.TxPipeline()
	for voteKey, starVotes := range voteCache {
		for member, count := range starVotes {
			pipe.ZIncrBy(ctx, voteKey, float64(count), member)
		}
	}
	_, err := pipe.Exec(ctx)
	if err != nil {
		return err
	}
	voteCache = make(map[string]map[string]int64)
	log.Printf("Flushed votes to Redis")
	return nil
}
func GetLeaderboard(ctx context.Context, shardKey string) ([]redis.Z, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	key := fmt.Sprintf("%s:votes", shardKey)
	log.Printf("Getting leaderboard from key %s", key)
	return redisClient.ZRangeWithScores(ctx, key, 0, -1).Result()
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
		starID, err := parseStarID(rank.Member.(string))
		if err != nil {
			log.Printf("Error parsing star ID: %v", err)
			continue
		}
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
