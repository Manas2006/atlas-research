package atlas

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
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
