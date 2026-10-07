package video

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type API struct {
	Queue *Queue
	Store *ObjectStore
}

func (a *API) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeVideoJSON(w, 200, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("POST /v1/uploads", a.createUpload)
	mux.HandleFunc("PATCH /v1/uploads/{id}", a.uploadChunk)
	mux.HandleFunc("POST /v1/uploads/{id}/complete", a.completeUpload)
	mux.HandleFunc("GET /v1/jobs", a.listJobs)
	mux.HandleFunc("GET /v1/jobs/{id}", a.getJob)
	mux.HandleFunc("POST /v1/jobs/{id}/replay", a.replayJob)
	mux.HandleFunc("POST /internal/jobs/lease", a.leaseJob)
	mux.HandleFunc("POST /internal/jobs/{id}/heartbeat", a.heartbeatJob)
	mux.HandleFunc("POST /internal/jobs/{id}/complete", a.completeJob)
	mux.HandleFunc("POST /internal/jobs/{id}/fail", a.failJob)
	return mux
}

func (a *API) createUpload(w http.ResponseWriter, _ *http.Request) {
	bytes := make([]byte, 16)
	if _, err := rand.Read(bytes); err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	id := hex.EncodeToString(bytes)
	w.Header().Set("Location", "/v1/uploads/"+id)
	w.Header().Set("Upload-Offset", "0")
	writeVideoJSON(w, http.StatusCreated, map[string]string{"upload_id": id})
}

func (a *API) uploadChunk(w http.ResponseWriter, r *http.Request) {
	offset, err := strconv.ParseInt(r.Header.Get("Upload-Offset"), 10, 64)
	if err != nil || offset < 0 {
		http.Error(w, "valid Upload-Offset required", 400)
		return
	}
	current, err := a.Store.PutChunk(r.PathValue("id"), offset, r.Body)
	w.Header().Set("Upload-Offset", strconv.FormatInt(current, 10))
	if err != nil {
		http.Error(w, err.Error(), http.StatusConflict)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (a *API) completeUpload(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	input, err := a.Store.Finalize(id)
	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	info, err := os.Stat(input)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	decodedName, err := url.QueryUnescape(r.Header.Get("X-Upload-Name"))
	if err != nil {
		http.Error(w, "invalid upload name", 400)
		return
	}
	name := filepath.Base(strings.ReplaceAll(decodedName, "\\", "/"))
	if name == "." || name == "/" {
		name = id
	}
	if len(name) > 255 {
		http.Error(w, "upload name too long", 400)
		return
	}
	idempotencyKey := r.Header.Get("Idempotency-Key")
	if idempotencyKey == "" {
		idempotencyKey = "upload:" + id
	}
	job, created, err := a.Queue.Create(Job{ID: id, Name: name, SizeBytes: info.Size(), IdempotencyKey: idempotencyKey, InputPath: input, OutputPath: a.Store.OutputPath(id), MaxAttempts: 5})
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	status := http.StatusOK
	if created {
		status = http.StatusAccepted
	}
	writeVideoJSON(w, status, job)
}

func (a *API) listJobs(w http.ResponseWriter, _ *http.Request) {
	type summary struct {
		ID        string    `json:"id"`
		Name      string    `json:"name"`
		SizeBytes int64     `json:"size_bytes"`
		State     JobState  `json:"state"`
		UpdatedAt time.Time `json:"updated_at"`
	}
	jobs := a.Queue.List()
	items := make([]summary, 0, len(jobs))
	for _, job := range jobs {
		items = append(items, summary{ID: job.ID, Name: job.Name, SizeBytes: job.SizeBytes, State: job.State, UpdatedAt: job.UpdatedAt})
	}
	writeVideoJSON(w, http.StatusOK, map[string]any{"jobs": items})
}

func (a *API) getJob(w http.ResponseWriter, r *http.Request) {
	job, ok := a.Queue.Get(r.PathValue("id"))
	if !ok {
		http.Error(w, "not found", 404)
		return
	}
	writeVideoJSON(w, 200, job)
}

func (a *API) replayJob(w http.ResponseWriter, r *http.Request) {
	job, err := a.Queue.ReplayDead(r.PathValue("id"))
	if err != nil {
		http.Error(w, err.Error(), 409)
		return
	}
	writeVideoJSON(w, 200, job)
}

type leaseRequest struct {
	Worker       string `json:"worker"`
	LeaseSeconds int    `json:"lease_seconds"`
}

func (a *API) leaseJob(w http.ResponseWriter, r *http.Request) {
	var input leaseRequest
	if json.NewDecoder(r.Body).Decode(&input) != nil || input.Worker == "" {
		http.Error(w, "worker required", 400)
		return
	}
	if input.LeaseSeconds < 10 || input.LeaseSeconds > 300 {
		input.LeaseSeconds = 30
	}
	job, err := a.Queue.Lease(input.Worker, time.Duration(input.LeaseSeconds)*time.Second)
	if errors.Is(err, ErrNoJob) {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	writeVideoJSON(w, 200, job)
}

func (a *API) heartbeatJob(w http.ResponseWriter, r *http.Request) {
	var input leaseRequest
	if json.NewDecoder(r.Body).Decode(&input) != nil {
		http.Error(w, "invalid JSON", 400)
		return
	}
	if input.LeaseSeconds < 10 || input.LeaseSeconds > 300 {
		input.LeaseSeconds = 30
	}
	job, err := a.Queue.Heartbeat(r.PathValue("id"), input.Worker, time.Duration(input.LeaseSeconds)*time.Second)
	if err != nil {
		http.Error(w, err.Error(), 409)
		return
	}
	writeVideoJSON(w, 200, job)
}

func (a *API) completeJob(w http.ResponseWriter, r *http.Request) {
	var input leaseRequest
	if json.NewDecoder(r.Body).Decode(&input) != nil {
		http.Error(w, "invalid JSON", 400)
		return
	}
	job, err := a.Queue.Complete(r.PathValue("id"), input.Worker)
	if err != nil {
		http.Error(w, err.Error(), 409)
		return
	}
	writeVideoJSON(w, 200, job)
}

func (a *API) failJob(w http.ResponseWriter, r *http.Request) {
	var input struct {
		Worker string `json:"worker"`
		Error  string `json:"error"`
	}
	if json.NewDecoder(r.Body).Decode(&input) != nil {
		http.Error(w, "invalid JSON", 400)
		return
	}
	job, err := a.Queue.Fail(r.PathValue("id"), input.Worker, input.Error, 2*time.Second)
	if err != nil {
		http.Error(w, err.Error(), 409)
		return
	}
	writeVideoJSON(w, 200, job)
}

func writeVideoJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

type QueueClient struct {
	BaseURL string
	Client  *http.Client
}

func NewQueueClient(baseURL string) *QueueClient {
	return &QueueClient{BaseURL: strings.TrimRight(baseURL, "/"), Client: &http.Client{Timeout: 10 * time.Second}}
}

func (c *QueueClient) Lease(ctx context.Context, worker string, duration time.Duration) (Job, error) {
	var job Job
	status, err := c.post(ctx, "/internal/jobs/lease", leaseRequest{Worker: worker, LeaseSeconds: int(duration.Seconds())}, &job)
	if status == http.StatusNoContent {
		return Job{}, ErrNoJob
	}
	return job, err
}

func (c *QueueClient) Heartbeat(ctx context.Context, id, worker string, duration time.Duration) error {
	_, err := c.post(ctx, "/internal/jobs/"+id+"/heartbeat", leaseRequest{Worker: worker, LeaseSeconds: int(duration.Seconds())}, &Job{})
	return err
}

func (c *QueueClient) Complete(ctx context.Context, id, worker string) error {
	_, err := c.post(ctx, "/internal/jobs/"+id+"/complete", leaseRequest{Worker: worker}, &Job{})
	return err
}

func (c *QueueClient) Fail(ctx context.Context, id, worker string, cause error) error {
	_, err := c.post(ctx, "/internal/jobs/"+id+"/fail", map[string]string{"worker": worker, "error": cause.Error()}, &Job{})
	return err
}

func (c *QueueClient) post(ctx context.Context, path string, input, output any) (int, error) {
	payload, _ := json.Marshal(input)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.BaseURL+path, strings.NewReader(string(payload)))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	response, err := c.Client.Do(req)
	if err != nil {
		return 0, err
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusNoContent {
		return response.StatusCode, nil
	}
	if response.StatusCode/100 != 2 {
		return response.StatusCode, fmt.Errorf("queue returned %s", response.Status)
	}
	return response.StatusCode, json.NewDecoder(response.Body).Decode(output)
}
