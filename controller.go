package main

import (
	"context"
	"encoding/csv"
	"io"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type Controller struct {
	store    *RedisQueueStore
	provider EmailProvider
	workers  int

	awsCfg aws.Config
	s3Cli  *s3.Client
	ps     *s3.PresignClient
}

func NewController(store *RedisQueueStore, provider EmailProvider, workers int) *Controller {
	return &Controller{store: store, provider: provider, workers: workers}
}

func (c *Controller) SetAWSConfig(cfg aws.Config) {
	c.awsCfg = cfg
	c.s3Cli = s3.NewFromConfig(cfg)
	c.ps = s3.NewPresignClient(c.s3Cli)
}

func (c *Controller) StartCampaign(id string) {
	// stateless; safe to call multiple times
	c.store.RegisterCampaign(context.Background(), id)
	for i := 0; i < c.workers; i++ {
		go c.workerLoop(id, i)
	}
}

// ---- S3 multipart presign helpers ----

func (c *Controller) S3CreateMultipartPresigns(ctx context.Context, key string, parts int) (uploadID string, urls []string, err error) {
	bucket := getenv("S3_BUCKET", "my-bucket")

	// 1) init
	initOut, err := c.s3Cli.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return "", nil, err
	}
	uploadID = aws.ToString(initOut.UploadId)

	// 2) presign UploadPart URLs
	urls = make([]string, parts)
	for i := 1; i <= parts; i++ {
		pre, err := c.ps.PresignUploadPart(ctx, &s3.UploadPartInput{
			Bucket:     aws.String(bucket),
			Key:        aws.String(key),
			UploadId:   aws.String(uploadID),
			PartNumber: aws.Int32(int32(i)), // <-- pointer
		}, s3.WithPresignExpires(15*time.Minute))
		if err != nil {
			return "", nil, err
		}
		urls[i-1] = pre.URL
	}
	return uploadID, urls, nil
}

func (c *Controller) S3CompleteMultipart(ctx context.Context, key, uploadID string, parts []struct {
	ETag       string `json:"etag"`
	PartNumber int32  `json:"part_number"`
}) error {
	bucket := getenv("S3_BUCKET", "my-bucket")
	cp := make([]s3types.CompletedPart, 0, len(parts))
	for _, p := range parts {
		cp = append(cp, s3types.CompletedPart{
			ETag:       aws.String(p.ETag),
			PartNumber: aws.Int32(p.PartNumber), // <-- pointer
		})
	}
	_, err := c.s3Cli.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(bucket),
		Key:      aws.String(key),
		UploadId: aws.String(uploadID),
		MultipartUpload: &s3types.CompletedMultipartUpload{
			Parts: cp,
		},
	})
	return err
}

func (c *Controller) S3AbortMultipart(ctx context.Context, key, uploadID string) error {
	bucket := getenv("S3_BUCKET", "my-bucket")
	_, err := c.s3Cli.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
		Bucket:   aws.String(bucket),
		Key:      aws.String(key),
		UploadId: aws.String(uploadID),
	})
	return err
}

// Parse the CSV (from S3) and enqueue jobs (one per email).
// For simplicity & to fix your compile error, this version downloads into memory
// via manager.NewWriteAtBuffer. (You can switch to a streaming approach later.)
func (c *Controller) ParseS3CSVAndEnqueue(s3Key, campaignID string) error {
	bucket := getenv("S3_BUCKET", "my-bucket")
	downloader := manager.NewDownloader(c.s3Cli)

	buf := manager.NewWriteAtBuffer([]byte{})
	_, err := downloader.Download(context.Background(), buf, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(s3Key),
	})
	if err != nil {
		return err
	}

	reader := csv.NewReader(strings.NewReader(string(buf.Bytes())))
	var total int64
	for {
		rec, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil || len(rec) == 0 {
			continue
		}
		email := strings.TrimSpace(rec[0])
		if email == "" {
			continue
		}
		_ = c.store.Enqueue(context.Background(), campaignID, JobPayload{Email: email, Attempts: 0})
		total++
	}
	_ = c.store.InitProgress(context.Background(), campaignID, total)
	return nil
}
