package main

import (
	"context"
	"fmt"
	"time"
)

// Periodically:
// 1) Requeue any items lingering in the processing list (simple heal).
// 2) Move due retries back to the main queue.
func StartReconciler(c *Controller, every time.Duration) {
	t := time.NewTicker(every)
	for range t.C {
		ctx := context.Background()
		campaigns, _ := c.store.ListCampaigns(ctx)
		for _, id := range campaigns {
			// 1) Requeue stuck processing (bounded loop for safety)
			if err := c.store.RequeueProcessingAll(ctx, id, 1000); err != nil {
				fmt.Println("requeue processing:", err)
			}

			// 2) Move due retries
			now := time.Now().Unix()
			items, err := c.store.PopDueRetries(ctx, id, now)
			if err != nil {
				fmt.Println("retries pop:", err); continue
			}
			for _, raw := range items {
				_ = c.store.Enqueue(ctx, id, jsonRaw(raw))
			}
		}
	}
}

// small wrapper to enqueue already-serialized JSON payloads
type jsonRaw string
func (j jsonRaw) MarshalJSON() ([]byte, error) { return []byte(j), nil }
