package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

type RedisQueueStore struct {
	rdb *redis.Client
}

func NewRedisQueueStore(rdb *redis.Client) *RedisQueueStore { return &RedisQueueStore{rdb: rdb} }

func (s *RedisQueueStore) queueKey(campaignID string) string      { return "campaign:" + campaignID + ":queue" }
func (s *RedisQueueStore) processingKey(campaignID string) string { return "campaign:" + campaignID + ":processing" }
func (s *RedisQueueStore) progressKey(campaignID string) string   { return "campaign:" + campaignID + ":progress" }
func (s *RedisQueueStore) statusKey(campaignID string) string     { return "campaign:" + campaignID + ":status" }
func (s *RedisQueueStore) rateLimitKey(campaignID string) string  { return "rate_limit:campaign:" + campaignID }
func (s *RedisQueueStore) rateCountKey(campaignID string) string  { return "rate_limit_count:campaign:" + campaignID }
func (s *RedisQueueStore) retryKey(campaignID string) string      { return "campaign:" + campaignID + ":retry" }
func (s *RedisQueueStore) campaignsSet() string                   { return "campaigns:list" }

func (s *RedisQueueStore) Enqueue(ctx context.Context, campaignID string, payload any) error {
	b, _ := json.Marshal(payload)
	return s.rdb.LPush(ctx, s.queueKey(campaignID), b).Err()
}

// BRPOPLPUSH pattern (reliable): pop from main queue, push into processing list
func (s *RedisQueueStore) PopToProcessing(ctx context.Context, campaignID string, timeout time.Duration) (string, error) {
	return s.rdb.BRPopLPush(ctx, s.queueKey(campaignID), s.processingKey(campaignID), timeout).Result()
}

func (s *RedisQueueStore) RemoveFromProcessing(ctx context.Context, campaignID, payload string) error {
	return s.rdb.LRem(ctx, s.processingKey(campaignID), 1, payload).Err()
}

func (s *RedisQueueStore) InitProgress(ctx context.Context, campaignID string, total int64) error {
	key := s.progressKey(campaignID)

	pipe := s.rdb.TxPipeline()
	pipe.HSet(ctx, key, "total", total)
	pipe.HSetNX(ctx, key, "sent", 0)   
	pipe.HSetNX(ctx, key, "failed", 0)
	_, err := pipe.Exec(ctx)
	return err
}

func (s *RedisQueueStore) IncrProgress(ctx context.Context, campaignID, field string, delta int64) (int64, error) {
	return s.rdb.HIncrBy(ctx, s.progressKey(campaignID), field, delta).Result()
}

func (s *RedisQueueStore) GetProgress(ctx context.Context, campaignID string) (map[string]string, error) {
	return s.rdb.HGetAll(ctx, s.progressKey(campaignID)).Result()
}

func (s *RedisQueueStore) SetStatus(ctx context.Context, campaignID, status string) error {
	return s.rdb.Set(ctx, s.statusKey(campaignID), status, 0).Err()
}

func (s *RedisQueueStore) GetStatus(ctx context.Context, campaignID string) (string, error) {
	return s.rdb.Get(ctx, s.statusKey(campaignID)).Result()
}

func (s *RedisQueueStore) SetRateLimit(ctx context.Context, campaignID string, limit int64) error {
	return s.rdb.Set(ctx, s.rateLimitKey(campaignID), limit, 0).Err()
}

func (s *RedisQueueStore) GetRateLimit(ctx context.Context, campaignID string) (int64, error) {
	return s.rdb.Get(ctx, s.rateLimitKey(campaignID)).Int64()
}

func (s *RedisQueueStore) IncrRateCount(ctx context.Context, campaignID string) (int64, error) {
	key := s.rateCountKey(campaignID)
	cnt, err := s.rdb.Incr(ctx, key).Result()
	if err != nil {
		return 0, err
	}
	if cnt == 1 {
		_ = s.rdb.Expire(ctx, key, time.Minute).Err()
	}
	return cnt, nil
}

func (s *RedisQueueStore) GetRateCount(ctx context.Context, campaignID string) (int64, error) {
	return s.rdb.Get(ctx, s.rateCountKey(campaignID)).Int64()
}

// Retry ZSET helpers
func (s *RedisQueueStore) AddRetry(ctx context.Context, campaignID string, unixTs int64, payload string) error {
	return s.rdb.ZAdd(ctx, s.retryKey(campaignID), redis.Z{Score: float64(unixTs), Member: payload}).Err()
}

func (s *RedisQueueStore) PopDueRetries(ctx context.Context, campaignID string, now int64) ([]string, error) {
	items, err := s.rdb.ZRangeByScore(ctx, s.retryKey(campaignID), &redis.ZRangeBy{Min: "-inf", Max: fmt.Sprintf("%d", now)}).Result()
	if err != nil {
		return nil, err
	}
	if len(items) > 0 {
		_ = s.rdb.ZRemRangeByScore(ctx, s.retryKey(campaignID), "-inf", fmt.Sprintf("%d", now)).Err()
	}
	return items, nil
}

// Move everything from processing back to queue (used by reconciler)
func (s *RedisQueueStore) RequeueProcessingAll(ctx context.Context, campaignID string, max int) error {
	for i := 0; i < max; i++ {
		item, err := s.rdb.RPopLPush(ctx, s.processingKey(campaignID), s.queueKey(campaignID)).Result()
		if err == redis.Nil {
			return nil
		}
		if err != nil {
			return err
		}
		_ = item
	}
	return nil
}

func (s *RedisQueueStore) RegisterCampaign(ctx context.Context, campaignID string) { _ = s.rdb.SAdd(ctx, s.campaignsSet(), campaignID).Err() }
func (s *RedisQueueStore) ListCampaigns(ctx context.Context) ([]string, error)     { return s.rdb.SMembers(ctx, s.campaignsSet()).Result() }
