package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/gorilla/mux"
)

// Fallback small-upload endpoint (multipart/form-data). For large files, use S3 multipart.
func makeUploadHandler(c *Controller) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := mux.Vars(r)["id"]
		if err := r.ParseMultipartForm(50 << 20); err != nil {
			http.Error(w, "parse form: "+err.Error(), http.StatusBadRequest); return
		}
		f, _, err := r.FormFile("file")
		if err != nil { http.Error(w, "file: "+err.Error(), http.StatusBadRequest); return }
		defer f.Close()

		reader := csv.NewReader(f)
		var total int64
		for {
			rec, err := reader.Read()
			if err == io.EOF { break }
			if err != nil || len(rec) == 0 { continue }
			email := rec[0]
			if email == "" { continue }
			_ = c.store.Enqueue(context.Background(), id, JobPayload{Email: email, Attempts: 0})
			total++
		}
		_ = c.store.InitProgress(context.Background(), id, total)
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(fmt.Sprintf("enqueued=%d\n", total)))
	}
}

// ---- S3 multipart HTTP endpoints ----

type InitUploadRequest struct {
	Filename string `json:"filename"`
	Parts    int    `json:"parts"` // optional; default 8
}

type InitUploadResponse struct {
	Key      string   `json:"key"`
	UploadID string   `json:"upload_id"`
	URLs     []string `json:"urls"` // presigned uploadPart URLs
	Parts    int      `json:"parts"`
}

func makeS3InitHandler(c *Controller) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		campaignID := mux.Vars(r)["id"]
		var req InitUploadRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "bad json", http.StatusBadRequest); return
		}
		if req.Parts <= 0 { req.Parts = 8 }
		key := fmt.Sprintf("campaigns/%s/%d_%s", campaignID, time.Now().Unix(), req.Filename)

		uploadID, urls, err := c.S3CreateMultipartPresigns(r.Context(), key, req.Parts)
		if err != nil {
			http.Error(w, "init multipart: "+err.Error(), http.StatusInternalServerError); return
		}
		resp := InitUploadResponse{Key: key, UploadID: uploadID, URLs: urls, Parts: req.Parts}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}
}

func makeS3CompleteHandler(c *Controller) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		campaignID := mux.Vars(r)["id"]
		var req struct {
			Key      string `json:"key"`
			UploadID string `json:"upload_id"`
			Parts    []struct {
				ETag       string `json:"etag"`
				PartNumber int32  `json:"part_number"`
			} `json:"parts"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "bad json", http.StatusBadRequest); return
		}
		if err := c.S3CompleteMultipart(r.Context(), req.Key, req.UploadID, req.Parts); err != nil {
			http.Error(w, "complete: "+err.Error(), http.StatusInternalServerError); return
		}
		// kick off CSV parse + enqueue (async)
		go func() {
			if err := c.ParseS3CSVAndEnqueue(req.Key, campaignID); err != nil {
				fmt.Println("CSV parse error:", err)
			}
		}()
		w.WriteHeader(http.StatusOK)
	}
}

func makeS3AbortHandler(c *Controller) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Key      string `json:"key"`
			UploadID string `json:"upload_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "bad json", http.StatusBadRequest); return
		}
		if err := c.S3AbortMultipart(r.Context(), req.Key, req.UploadID); err != nil {
			http.Error(w, "abort: "+err.Error(), http.StatusInternalServerError); return
		}
		w.WriteHeader(http.StatusOK)
	}
}

// ---- Campaign control/status endpoints ----

func makeStartHandler(c *Controller) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := mux.Vars(r)["id"]
		_ = c.store.SetStatus(r.Context(), id, "running")
		c.StartCampaign(id)
		w.WriteHeader(http.StatusOK)
	}
}

func makePauseHandler(c *Controller) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := mux.Vars(r)["id"]
		_ = c.store.SetStatus(r.Context(), id, "paused")
		w.WriteHeader(http.StatusOK)
	}
}

func makeResumeHandler(c *Controller) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := mux.Vars(r)["id"]
		_ = c.store.SetStatus(r.Context(), id, "running")
		w.WriteHeader(http.StatusOK)
	}
}

func makeStatusHandler(c *Controller) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := mux.Vars(r)["id"]
		p, _ := c.store.GetProgress(r.Context(), id)
		status, _ := c.store.GetStatus(r.Context(), id)
		limit, _ := c.store.GetRateLimit(r.Context(), id)
		count, _ := c.store.GetRateCount(r.Context(), id)
		resp := map[string]any{
			"id":      id,
			"status":  status,
			"progress": p,
			"tpm_limit": limit,
			"tpm_used":  count,
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}
}

// POST { "tpm": 200 }
func makeSetRateLimitHandler(c *Controller) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := mux.Vars(r)["id"]
		var req struct{ TPM int64 `json:"tpm"` }
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.TPM <= 0 {
			http.Error(w, "bad tpm", http.StatusBadRequest); return
		}
		if err := c.store.SetRateLimit(r.Context(), id, req.TPM); err != nil {
			http.Error(w, "set limit: "+err.Error(), http.StatusInternalServerError); return
		}
		w.WriteHeader(http.StatusOK)
	}
}
