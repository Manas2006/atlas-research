package main

import (
	"bytes"
	"context"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Manas2006/atlas-research/internal/atlas"
)

func TestSyncBundleImportsReadyRecordsAndIsIdempotent(t *testing.T) {
	server, err := atlas.Open(filepath.Join(t.TempDir(), "atlas"))
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	httpServer := httptest.NewServer(server.Handler())
	defer httpServer.Close()

	input := bundle{Version: 1, Records: []record{
		{ID: "paper", Title: "Long context", Content: "Rotary embeddings at scale", Type: "Presentation", Tags: []string{"spring-2026"}, Status: "ready", URL: "https://drive.google.com/file/d/paper", Path: "spring 2026/paper", MIMEType: "application/pdf"},
		{ID: "accounts", Title: "Lab accounts", Content: "private", Sensitive: true, Status: "ready"},
		{ID: "shortcut", Title: "Shortcut", Status: "skipped", SkipReason: "shortcut target unavailable"},
		{ID: "empty", Title: "Scanned PDF", Status: "ready"},
	}}
	var output bytes.Buffer
	first := syncBundle(context.Background(), httpServer.Client(), httpServer.URL, input, map[string]atlas.Entry{}, false, false, &output)
	if first.Imported != 1 || first.Skipped != 3 || first.Failed != 0 {
		t.Fatalf("unexpected first result: %+v\n%s", first, output.String())
	}
	entries, err := listEntries(context.Background(), httpServer.Client(), httpServer.URL)
	if err != nil {
		t.Fatal(err)
	}
	imported := entries["gdrive:paper"]
	if imported.Source == nil || imported.Source.Provider != "google_drive" || imported.Source.ContentSHA256 == "" {
		t.Fatalf("missing provenance: %+v", imported)
	}
	if !strings.Contains(strings.Join(imported.Tags, " "), "google-drive") {
		t.Fatalf("missing import tag: %v", imported.Tags)
	}

	output.Reset()
	second := syncBundle(context.Background(), httpServer.Client(), httpServer.URL, input, entries, false, false, &output)
	if second.Unchanged != 1 || second.Imported != 0 || second.Skipped != 3 || second.Failed != 0 {
		t.Fatalf("unexpected second result: %+v\n%s", second, output.String())
	}
}

func TestSyncBundleCanIncludeSensitiveRecordsExplicitly(t *testing.T) {
	server, err := atlas.Open(filepath.Join(t.TempDir(), "atlas"))
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	httpServer := httptest.NewServer(server.Handler())
	defer httpServer.Close()

	input := bundle{Version: 1, Records: []record{{ID: "ops", Title: "Operations", Content: "restricted setup notes", Sensitive: true, Status: "ready"}}}
	got := syncBundle(context.Background(), httpServer.Client(), httpServer.URL, input, map[string]atlas.Entry{}, true, false, &bytes.Buffer{})
	if got.Imported != 1 || got.Skipped != 0 || got.Failed != 0 {
		t.Fatalf("unexpected result: %+v", got)
	}
}

func TestNormalizeImportTagsMatchesCatalogRules(t *testing.T) {
	got := normalizeImportTags([]string{" Drive ", "drive", "SPRING-2026", "", "paper"})
	want := []string{"drive", "spring-2026", "paper"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("got %v, want %v", got, want)
	}
}
