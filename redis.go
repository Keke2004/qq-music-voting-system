package cache

import (
	"context"
	"fmt"
	"log"
	"sort"
	"vote/proto"

	"github.com/go-redis/redis/v8"
)

var ctx = context.Background()
var redisClient *redis.Client

func InitRedis(client *redis.Client) {
	redisClient = client
}
func GetShardKey(starID int32) string {
	return fmt.Sprintf("shard:%d", starID%10)
}
func IncrementVote(starID int32) error {
	shardKey := GetShardKey(starID)
	key := fmt.Sprintf("%s:votes", shardKey)
	member := fmt.Sprintf("star:%d", starID)
	log.Printf("Incrementing vote for Star ID %d in key %s", starID, key)
	err := redisClient.ZIncrBy(ctx, key, 1, member).Err()
	if err != nil {
		log.Printf("Error incrementing vote: %v", err)
	} else {
		log.Printf("Vote incremented successfully for StarID %d", starID)
	}
	return err
}
func GetLeaderboard(shardKey string) ([]redis.Z, error) {
	key := fmt.Sprintf("%s:votes", shardKey)
	log.Printf("Getting leaderboard from key %s", key)
	return redisClient.ZRangeWithScores(ctx, key, 0, -1).Result()
}
func GetAllRankings() ([]redis.Z, error) {
	var allRankings []redis.Z
	for i := 0; i < 10; i++ {
		shardKey := fmt.Sprintf("shard:%d", i)
		rankings, err := GetLeaderboard(shardKey)
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
func parseStarID(member string) (int32, error) {
	var starID int32
	_, err := fmt.Sscanf(member, "star:%d", &starID)
	return starID, err
}
func ClearLeaderboard() error {
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
