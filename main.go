package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/gorilla/mux"
	"github.com/redis/go-redis/v9"
)

func main() {
	var (
		port      = flag.String("port", getenv("PORT", "8080"), "server port")
		redisAddr = flag.String("redis", getenv("REDIS_ADDR", "localhost:6379"), "redis address")
		workers   = flag.Int("workers", getenvInt("WORKERS", 10), "workers per campaign")
	)
	flag.Parse()

	// Redis
	rdb := redis.NewClient(&redis.Options{Addr: *redisAddr})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatalf("redis ping failed: %v", err)
	}

	// AWS config (S3)
	awsCfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		log.Fatalf("aws cfg: %v", err)
	}

	// Build
	store := NewRedisQueueStore(rdb)
	// choose provider (mock or real)
	var provider EmailProvider
	if sk := os.Getenv("SENDGRID_API_KEY"); sk != "" {
		provider = NewSendGridProvider(sk)
	} else {
		provider = NewMockProvider()
	}
	controller := NewController(store, provider, *workers)
	controller.SetAWSConfig(awsCfg)

	// reconciler (heals stuck jobs + triggers retries)
	go StartReconciler(controller, 30*time.Second)

	// HTTP
	r := mux.NewRouter()

	// S3 multipart upload flow
	r.HandleFunc("/campaigns/{id}/upload/init", makeS3InitHandler(controller)).Methods("POST")
	r.HandleFunc("/campaigns/{id}/upload/complete", makeS3CompleteHandler(controller)).Methods("POST")
	r.HandleFunc("/campaigns/{id}/upload/abort", makeS3AbortHandler(controller)).Methods("POST")

	// (optional) small CSV direct upload (for tiny files)
	r.HandleFunc("/campaigns/{id}/upload", makeUploadHandler(controller)).Methods("POST")

	// controls + status
	r.HandleFunc("/campaigns/{id}/start", makeStartHandler(controller)).Methods("POST")
	r.HandleFunc("/campaigns/{id}/pause", makePauseHandler(controller)).Methods("POST")
	r.HandleFunc("/campaigns/{id}/resume", makeResumeHandler(controller)).Methods("POST")
	r.HandleFunc("/campaigns/{id}/status", makeStatusHandler(controller)).Methods("GET")
	r.HandleFunc("/campaigns/{id}/rate-limit", makeSetRateLimitHandler(controller)).Methods("POST")

	srv := &http.Server{
		Addr:         ":" + *port,
		Handler:      r,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
	}
	fmt.Printf("listening on %s\n", srv.Addr)
	log.Fatal(srv.ListenAndServe())
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getenvInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		var out int
		_, _ = fmt.Sscanf(v, "%d", &out)
		if out > 0 {
			return out
		}
	}
	return fallback
}
