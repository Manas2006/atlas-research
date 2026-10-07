package atlas

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/Manas2006/atlas-research/internal/collab"
)

func TestServerIndexesAndSearchesEntries(t *testing.T) {
	server, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	handler := server.Handler()

	payload := []byte(`{"title":"Lease recovery","body":"Workers reclaim expired media jobs","type":"Note","tags":["reliability"]}`)
	request := httptest.NewRequest(http.MethodPost, "/api/entries", bytes.NewReader(payload))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusCreated {
		t.Fatalf("create returned %d: %s", response.Code, response.Body.String())
	}

	request = httptest.NewRequest(http.MethodGet, "/api/search?q=expired+workers", nil)
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("search returned %d: %s", response.Code, response.Body.String())
	}
	var body struct {
		Results []SearchResult `json:"results"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if len(body.Results) != 1 || body.Results[0].Entry.Title != "Lease recovery" {
		t.Fatalf("unexpected search results: %+v", body.Results)
	}
}

func TestImpactExperimentAPIValidationLifecycleAndSearchConclusion(t *testing.T) {
	server, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	handler := server.Handler()
	do := func(method, target, body string) *httptest.ResponseRecorder {
		t.Helper()
		request := httptest.NewRequest(method, target, strings.NewReader(body))
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		return response
	}
	invalid := do(http.MethodPost, "/api/impact-experiments", `{"name":"missing fields"}`)
	if invalid.Code != http.StatusBadRequest {
		t.Fatalf("invalid create returned %d", invalid.Code)
	}
	created := do(http.MethodPost, "/api/impact-experiments", `{"name":"Campaign holdout","hypothesis":"campaign causes incremental orders","outcome_name":"order","treatment_allocation":0.5,"observation_window_seconds":3600}`)
	if created.Code != http.StatusCreated {
		t.Fatalf("create: %d %s", created.Code, created.Body.String())
	}
	var body struct {
		Experiment struct {
			ID string `json:"id"`
		} `json:"experiment"`
	}
	if err := json.Unmarshal(created.Body.Bytes(), &body); err != nil || body.Experiment.ID == "" {
		t.Fatalf("create body: %v %s", err, created.Body.String())
	}
	id := body.Experiment.ID
	started := do(http.MethodPost, "/api/impact-experiments/"+id+"/start", "")
	if started.Code != http.StatusOK {
		t.Fatalf("start: %d %s", started.Code, started.Body.String())
	}
	assignment := do(http.MethodPost, "/api/impact-experiments/"+id+"/assignments", `{"subject_id":"synthetic-person-1"}`)
	var assigned struct {
		Arm           string `json:"arm"`
		ConfigVersion int    `json:"config_version"`
	}
	if err := json.Unmarshal(assignment.Body.Bytes(), &assigned); err != nil || assigned.Arm == "" {
		t.Fatalf("assignment: %d %s", assignment.Code, assignment.Body.String())
	}
	now := time.Now().UTC()
	events := fmt.Sprintf(`{"events":[{"event_id":"exposure","type":"exposure","subject_id":"synthetic-person-1","arm":%q,"event_timestamp":%q,"config_version":1},{"event_id":"outcome","type":"outcome","subject_id":"synthetic-person-1","arm":%q,"event_timestamp":%q,"outcome_name":"order","value":42,"config_version":1}]}`, assigned.Arm, now.Format(time.RFC3339Nano), assigned.Arm, now.Add(time.Minute).Format(time.RFC3339Nano))
	ingested := do(http.MethodPost, "/api/impact-experiments/"+id+"/events", events)
	if ingested.Code != http.StatusAccepted || !strings.Contains(ingested.Body.String(), `"accepted":2`) {
		t.Fatalf("ingest: %d %s", ingested.Code, ingested.Body.String())
	}
	stopped := do(http.MethodPost, "/api/impact-experiments/"+id+"/stop", "")
	if stopped.Code != http.StatusOK || !strings.Contains(stopped.Body.String(), `"knowledge_entry"`) {
		t.Fatalf("stop: %d %s", stopped.Code, stopped.Body.String())
	}
	searched := do(http.MethodGet, "/api/search?q=incremental+orders", "")
	if searched.Code != http.StatusOK || !strings.Contains(searched.Body.String(), "Impact conclusion: Campaign holdout") {
		t.Fatalf("conclusion not searchable: %d %s", searched.Code, searched.Body.String())
	}
}

func TestConsoleReadsMeasuredTrafficAndPersistedMediaJobs(t *testing.T) {
	server, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	handler := server.Handler()
	request := func(method, path, body string, headers map[string]string) *httptest.ResponseRecorder {
		t.Helper()
		r := httptest.NewRequest(method, path, strings.NewReader(body))
		for key, value := range headers {
			r.Header.Set(key, value)
		}
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, r)
		return response
	}

	empty := request(http.MethodGet, "/api/video/v1/jobs", "", nil)
	if empty.Code != http.StatusOK || !strings.Contains(empty.Body.String(), `"jobs":[]`) {
		t.Fatalf("expected an empty real queue: %d %s", empty.Code, empty.Body.String())
	}
	created := request(http.MethodPost, "/api/video/v1/uploads", "", nil)
	var upload struct {
		UploadID string `json:"upload_id"`
	}
	if err := json.Unmarshal(created.Body.Bytes(), &upload); err != nil || upload.UploadID == "" {
		t.Fatalf("create upload: %d %s", created.Code, created.Body.String())
	}
	path := "/api/video/v1/uploads/" + upload.UploadID
	chunk := request(http.MethodPatch, path, "real video bytes", map[string]string{"Upload-Offset": "0"})
	if chunk.Code != http.StatusNoContent {
		t.Fatalf("upload chunk: %d %s", chunk.Code, chunk.Body.String())
	}
	completed := request(http.MethodPost, path+"/complete", "", map[string]string{"X-Upload-Name": url.QueryEscape("research clip.mp4")})
	if completed.Code != http.StatusAccepted {
		t.Fatalf("complete upload: %d %s", completed.Code, completed.Body.String())
	}
	jobs := request(http.MethodGet, "/api/video/v1/jobs", "", nil)
	var listed struct {
		Jobs []struct {
			Name      string `json:"name"`
			SizeBytes int64  `json:"size_bytes"`
			State     string `json:"state"`
		} `json:"jobs"`
	}
	if err := json.Unmarshal(jobs.Body.Bytes(), &listed); err != nil || len(listed.Jobs) != 1 || listed.Jobs[0].Name != "research clip.mp4" || listed.Jobs[0].SizeBytes != 16 || listed.Jobs[0].State != "queued" {
		t.Fatalf("real job not listed: %d %s", jobs.Code, jobs.Body.String())
	}
	if strings.Contains(jobs.Body.String(), "input_path") || strings.Contains(jobs.Body.String(), "idempotency_key") {
		t.Fatalf("job list exposed internal paths or keys: %s", jobs.Body.String())
	}
	signals := request(http.MethodGet, "/api/signals", "", nil)
	var measured struct {
		Requests uint64 `json:"requests"`
		History  []struct {
			Count uint64 `json:"count"`
		} `json:"request_history"`
	}
	if err := json.Unmarshal(signals.Body.Bytes(), &measured); err != nil || measured.Requests < 6 || len(measured.History) != 12 || measured.History[11].Count < 6 {
		t.Fatalf("traffic was not measured: %d %s", signals.Code, signals.Body.String())
	}
}

func TestServerServesConsoleAndCORS(t *testing.T) {
	server, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()

	request := httptest.NewRequest(http.MethodGet, "/", nil)
	request.Header.Set("Origin", "https://example.github.io")
	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("console returned %d", response.Code)
	}
	if response.Header().Get("Access-Control-Allow-Origin") != "https://example.github.io" {
		t.Fatalf("missing CORS response: %v", response.Header())
	}
}

func TestLiveDocsAreServedIndexedAndSearchable(t *testing.T) {
	dataDir := t.TempDir()
	server, err := Open(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	handler := server.Handler()
	do := func(method, target, body string) *httptest.ResponseRecorder {
		t.Helper()
		request := httptest.NewRequest(method, target, bytes.NewReader([]byte(body)))
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		return response
	}

	response := do(http.MethodPost, "/api/docs", `{"title":"Saturation notes","template":"factoid","user":{"id":"u1","name":"Manas"}}`)
	if response.Code != http.StatusCreated {
		t.Fatalf("create returned %d: %s", response.Code, response.Body.String())
	}
	var created struct {
		Doc struct {
			ID string `json:"id"`
		} `json:"doc"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &created); err != nil || created.Doc.ID == "" {
		t.Fatalf("unexpected create response: %s", response.Body.String())
	}

	// The doc is searchable next to catalog entries, by title and by text.
	for _, query := range []string{"saturation", "factoids+librarian"} {
		response = do(http.MethodGet, "/api/search?q="+query, "")
		var found struct {
			Results []SearchResult `json:"results"`
		}
		if err := json.Unmarshal(response.Body.Bytes(), &found); err != nil {
			t.Fatal(err)
		}
		if len(found.Results) != 1 || found.Results[0].Entry.ID != "doc:"+created.Doc.ID || found.Results[0].Entry.Type != "Doc" {
			t.Fatalf("query %q: unexpected results %+v", query, found.Results)
		}
	}

	response = do(http.MethodGet, "/api/agents", "")
	if response.Code != http.StatusOK || !bytes.Contains(response.Body.Bytes(), []byte(`"librarian"`)) || bytes.Contains(response.Body.Bytes(), []byte(`"writer"`)) {
		t.Fatalf("expected only the built-in agent without a model configured: %s", response.Body.String())
	}
	response = do(http.MethodGet, "/api/health", "")
	if !bytes.Contains(response.Body.Bytes(), []byte(`"docs":1`)) {
		t.Fatalf("health should count docs: %s", response.Body.String())
	}
	if response = do(http.MethodGet, "/api/docs/"+created.Doc.ID+"/log", ""); response.Code != http.StatusOK || !bytes.Contains(response.Body.Bytes(), []byte(`"k":"meta"`)) {
		t.Fatalf("history export failed: %d %s", response.Code, response.Body.String())
	}
	if response = do(http.MethodGet, "/api/docs/nosuchdoc0000000", ""); response.Code != http.StatusNotFound {
		t.Fatalf("unknown doc returned %d", response.Code)
	}

	// The index is rebuilt from the operation logs on the next start.
	if err := server.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	request := httptest.NewRequest(http.MethodGet, "/api/search?q=saturation", nil)
	recorder := httptest.NewRecorder()
	reopened.Handler().ServeHTTP(recorder, request)
	if !bytes.Contains(recorder.Body.Bytes(), []byte("doc:"+created.Doc.ID)) {
		t.Fatalf("doc was not reindexed after restart: %s", recorder.Body.String())
	}
}

func TestWriterAgentIsOfferedWhenConfigured(t *testing.T) {
	server, err := OpenWithOptions(t.TempDir(), Options{Writer: &collab.LLMConfig{BaseURL: "http://127.0.0.1:1/v1", Model: "lab-model"}})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	request := httptest.NewRequest(http.MethodGet, "/api/agents", nil)
	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, request)
	if !bytes.Contains(response.Body.Bytes(), []byte(`"writer"`)) || !bytes.Contains(response.Body.Bytes(), []byte(`"lab-model"`)) {
		t.Fatalf("writer agent missing: %s", response.Body.String())
	}
	if _, err := OpenWithOptions(t.TempDir(), Options{Writer: &collab.LLMConfig{Model: "no-url"}}); err == nil {
		t.Fatal("a writer without a URL should fail at startup, not at first use")
	}
}

func TestAllowedOriginsRestrictBrowserCallers(t *testing.T) {
	server, err := OpenWithOptions(t.TempDir(), Options{AllowedOrigins: []string{"https://manas2006.github.io/"}})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	status := func(origin string) int {
		request := httptest.NewRequest(http.MethodGet, "http://atlas.lab:8088/api/health", nil)
		if origin != "" {
			request.Header.Set("Origin", origin)
		}
		response := httptest.NewRecorder()
		server.Handler().ServeHTTP(response, request)
		return response.Code
	}
	for origin, want := range map[string]int{
		"":                            http.StatusOK,        // curl, scripts, same-origin GETs
		"http://atlas.lab:8088":       http.StatusOK,        // the console this process serves
		"https://manas2006.github.io": http.StatusOK,        // the hosted console, allowed by flag
		"https://evil.example":        http.StatusForbidden, // any other page in a member's browser
	} {
		if got := status(origin); got != want {
			t.Fatalf("origin %q: got %d, want %d", origin, got, want)
		}
	}
}
