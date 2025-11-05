package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"time"
)

type JobPayload struct {
	Email    string `json:"email"`
	Attempts int    `json:"attempts"`
}

func (c *Controller) workerLoop(campaignID string, wid int) {
	for {
		// 1) check global flag
		if status, _ := c.store.GetStatus(context.Background(), campaignID); status == "paused" {
			time.Sleep(500 * time.Millisecond)
			continue
		}

		// 2) atomically move one job into processing (blocks)
		raw, err := c.store.PopToProcessing(context.Background(), campaignID, 5*time.Second)
		if err != nil {
			fmt.Printf("[w%d] pop: %v\n", wid, err)
			time.Sleep(200 * time.Millisecond)
			continue
		}
		if raw == "" {
			continue // nothing right now
		}

		var job JobPayload
		if err := json.Unmarshal([]byte(raw), &job); err != nil {
			_ = c.store.RemoveFromProcessing(context.Background(), campaignID, raw)
			continue
		}

		// 3) rate limit (configurable per campaign)
		limit, err := c.store.GetRateLimit(context.Background(), campaignID)
		if err != nil || limit <= 0 {
			limit = 120 // safe fallback
		}
		used, err := c.store.IncrRateCount(context.Background(), campaignID)
		if err != nil {
			// be conservative if redis hiccups
			time.Sleep(500 * time.Millisecond)
			// requeue job
			_ = c.store.RemoveFromProcessing(context.Background(), campaignID, raw)
			_ = c.store.Enqueue(context.Background(), campaignID, job)
			continue
		}
		if used > limit {
			// push back and try later in this minute
			_ = c.store.RemoveFromProcessing(context.Background(), campaignID, raw)
			_ = c.store.Enqueue(context.Background(), campaignID, job)
			time.Sleep(500 * time.Millisecond)
			continue
		}

		// 4) send email
		if err := c.provider.Send(job.Email); err != nil {
			// retry with exponential backoff (max 3)
			job.Attempts++
			if job.Attempts <= 3 {
				delay := time.Duration(math.Pow(2, float64(job.Attempts))) * time.Second
				retryAt := time.Now().Add(delay).Unix()
				_ = c.store.AddRetry(context.Background(), campaignID, retryAt, mustJSON(job))
			} else {
				_, _ = c.store.IncrProgress(context.Background(), campaignID, "failed", 1)
			}
			_ = c.store.RemoveFromProcessing(context.Background(), campaignID, raw)
			continue
		}

		// 5) success path
		_, _ = c.store.IncrProgress(context.Background(), campaignID, "sent", 1)
		_ = c.store.RemoveFromProcessing(context.Background(), campaignID, raw)
	}
}

func mustJSON(v any) string { b, _ := json.Marshal(v); return string(b) }
