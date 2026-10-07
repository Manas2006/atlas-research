package atlas

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

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
