// Command atlas-import synchronizes an extracted source bundle into a running
// Atlas instance. It intentionally does not contain cloud credentials or call a
// provider API: extraction happens in an authenticated, access-controlled
// environment and the resulting bundle can remain outside version control.
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/Manas2006/atlas-research/internal/atlas"
)

type bundle struct {
	Version    int      `json:"version"`
	SourceRoot string   `json:"source_root"`
	Records    []record `json:"records"`
}

type record struct {
	ID         string    `json:"id"`
	Title      string    `json:"title"`
	Content    string    `json:"content"`
	Type       string    `json:"type"`
	Tags       []string  `json:"tags"`
	URL        string    `json:"url"`
	Path       string    `json:"path"`
	MIMEType   string    `json:"mime_type"`
	ModifiedAt time.Time `json:"modified_at"`
	Sensitive  bool      `json:"sensitive"`
	Status     string    `json:"status"`
	SkipReason string    `json:"skip_reason"`
}

type result struct {
	Imported  int
	Unchanged int
	Skipped   int
	Failed    int
}

func main() {
	input := flag.String("input", "", "path to an extracted Atlas import bundle")
	baseURL := flag.String("base-url", "http://localhost:8088", "running Atlas API base URL")
	includeSensitive := flag.Bool("include-sensitive", false, "import records marked sensitive (use only with an access-controlled runtime)")
	dryRun := flag.Bool("dry-run", false, "validate and report without writing entries")
	timeout := flag.Duration("timeout", 30*time.Second, "per-request timeout")
	flag.Parse()

	if *input == "" {
		fatal(errors.New("-input is required"))
	}
	parsedBase, err := url.Parse(strings.TrimRight(*baseURL, "/"))
	if err != nil || parsedBase.Scheme == "" || parsedBase.Host == "" {
		fatal(fmt.Errorf("invalid -base-url %q", *baseURL))
	}
	loaded, err := loadBundle(*input)
	if err != nil {
		fatal(err)
	}
	client := &http.Client{Timeout: *timeout}
	ctx := context.Background()
	existing, err := listEntries(ctx, client, parsedBase.String())
	if err != nil {
		fatal(err)
	}

	got := syncBundle(ctx, client, parsedBase.String(), loaded, existing, *includeSensitive, *dryRun, os.Stdout)
	fmt.Printf("summary: imported=%d unchanged=%d skipped=%d failed=%d\n", got.Imported, got.Unchanged, got.Skipped, got.Failed)
	if got.Failed > 0 {
		os.Exit(1)
	}
}

func loadBundle(path string) (bundle, error) {
	file, err := os.Open(path)
	if err != nil {
		return bundle{}, err
	}
	defer file.Close()
	var value bundle
	decoder := json.NewDecoder(io.LimitReader(file, 128<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&value); err != nil {
		return bundle{}, fmt.Errorf("decode bundle: %w", err)
	}
	if value.Version != 1 {
		return bundle{}, fmt.Errorf("unsupported bundle version %d", value.Version)
	}
	seen := make(map[string]struct{}, len(value.Records))
	for index := range value.Records {
		r := &value.Records[index]
		r.ID = strings.TrimSpace(r.ID)
		r.Title = strings.TrimSpace(r.Title)
		r.Status = strings.ToLower(strings.TrimSpace(r.Status))
		if r.Status == "" {
			r.Status = "ready"
		}
		if r.Status != "ready" && r.Status != "skipped" {
			return bundle{}, fmt.Errorf("record %q has unsupported status %q", r.ID, r.Status)
		}
		if r.ID == "" || r.Title == "" {
			return bundle{}, fmt.Errorf("record %d requires id and title", index)
		}
		if _, duplicate := seen[r.ID]; duplicate {
			return bundle{}, fmt.Errorf("duplicate record id %q", r.ID)
		}
		seen[r.ID] = struct{}{}
	}
	sort.SliceStable(value.Records, func(i, j int) bool { return value.Records[i].Path < value.Records[j].Path })
	return value, nil
}

func syncBundle(ctx context.Context, client *http.Client, baseURL string, loaded bundle, existing map[string]atlas.Entry, includeSensitive, dryRun bool, output io.Writer) result {
	var got result
	for _, source := range loaded.Records {
		entryID := "gdrive:" + source.ID
		if source.Status != "ready" {
			got.Skipped++
			fmt.Fprintf(output, "skip %s: %s\n", entryID, firstNonEmpty(source.SkipReason, source.Status))
			continue
		}
		if source.Sensitive && !includeSensitive {
			got.Skipped++
			fmt.Fprintf(output, "skip %s: sensitive (pass -include-sensitive only for a protected runtime)\n", entryID)
			continue
		}
		content := strings.TrimSpace(source.Content)
		if content == "" {
			got.Skipped++
			fmt.Fprintf(output, "skip %s: no extracted text\n", entryID)
			continue
		}
		hash := sha256.Sum256([]byte(content))
		checksum := hex.EncodeToString(hash[:])
		entry := atlas.Entry{
			ID: entryID, Title: source.Title, Body: content, Type: firstNonEmpty(source.Type, "Research source"),
			Tags:   normalizeImportTags(append([]string{"google-drive", "imported"}, source.Tags...)),
			Source: &atlas.Source{Provider: "google_drive", ID: source.ID, URL: source.URL, Path: source.Path, MIMEType: source.MIMEType, ModifiedAt: source.ModifiedAt, ContentSHA256: checksum},
		}
		if prior, ok := existing[entryID]; ok && sameImportedEntry(prior, entry) {
			got.Unchanged++
			fmt.Fprintf(output, "unchanged %s\n", entryID)
			continue
		}
		if dryRun {
			got.Imported++
			fmt.Fprintf(output, "would import %s\n", entryID)
			continue
		}
		if err := postEntry(ctx, client, baseURL, entry); err != nil {
			got.Failed++
			fmt.Fprintf(output, "failed %s: %v\n", entryID, err)
			continue
		}
		got.Imported++
		fmt.Fprintf(output, "imported %s\n", entryID)
	}
	return got
}

func sameImportedEntry(prior, next atlas.Entry) bool {
	if prior.Source == nil || next.Source == nil {
		return false
	}
	return prior.Title == next.Title && prior.Type == next.Type && slices.Equal(prior.Tags, next.Tags) &&
		prior.Source.Provider == next.Source.Provider && prior.Source.ID == next.Source.ID &&
		prior.Source.URL == next.Source.URL && prior.Source.Path == next.Source.Path &&
		prior.Source.MIMEType == next.Source.MIMEType && prior.Source.ModifiedAt.Equal(next.Source.ModifiedAt) &&
		prior.Source.ContentSHA256 == next.Source.ContentSHA256
}

func normalizeImportTags(tags []string) []string {
	seen := make(map[string]struct{}, len(tags))
	result := make([]string, 0, len(tags))
	for _, tag := range tags {
		tag = strings.ToLower(strings.TrimSpace(tag))
		if tag == "" {
			continue
		}
		if _, duplicate := seen[tag]; duplicate {
			continue
		}
		seen[tag] = struct{}{}
		result = append(result, tag)
		if len(result) == 8 {
			break
		}
	}
	return result
}

func listEntries(ctx context.Context, client *http.Client, baseURL string) (map[string]atlas.Entry, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/api/entries", nil)
	if err != nil {
		return nil, err
	}
	response, err := client.Do(request)
	if err != nil {
		return nil, fmt.Errorf("list Atlas entries: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return nil, responseError("list Atlas entries", response)
	}
	var payload struct {
		Entries []atlas.Entry `json:"entries"`
	}
	if err := json.NewDecoder(io.LimitReader(response.Body, 128<<20)).Decode(&payload); err != nil {
		return nil, fmt.Errorf("decode Atlas entries: %w", err)
	}
	entries := make(map[string]atlas.Entry, len(payload.Entries))
	for _, entry := range payload.Entries {
		entries[entry.ID] = entry
	}
	return entries, nil
}

func postEntry(ctx context.Context, client *http.Client, baseURL string, entry atlas.Entry) error {
	payload, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+"/api/entries", bytes.NewReader(payload))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusCreated {
		return responseError("import entry", response)
	}
	return nil
}

func responseError(operation string, response *http.Response) error {
	body, _ := io.ReadAll(io.LimitReader(response.Body, 8<<10))
	return fmt.Errorf("%s: HTTP %d: %s", operation, response.StatusCode, strings.TrimSpace(string(body)))
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return "unknown"
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "atlas-import:", err)
	os.Exit(2)
}
