package atlas

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

type Entry struct {
	ID        string    `json:"id"`
	Title     string    `json:"title"`
	Body      string    `json:"body"`
	Type      string    `json:"type"`
	Tags      []string  `json:"tags"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

type Run struct {
	ID        string            `json:"id"`
	Name      string            `json:"name"`
	Model     string            `json:"model"`
	Dataset   string            `json:"dataset"`
	Status    string            `json:"status"`
	Score     *float64          `json:"score,omitempty"`
	Parameters map[string]string `json:"parameters,omitempty"`
	Notes     string            `json:"notes,omitempty"`
	CreatedAt time.Time         `json:"created_at"`
	UpdatedAt time.Time         `json:"updated_at"`
}

type catalogFile struct {
	Entries map[string]Entry `json:"entries"`
	Runs    map[string]Run   `json:"runs"`
}

type Catalog struct {
	mu      sync.RWMutex
	path    string
	entries map[string]Entry
	runs    map[string]Run
}

func OpenCatalog(path string) (*Catalog, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, err
	}
	c := &Catalog{path: path, entries: make(map[string]Entry), runs: make(map[string]Run)}
	payload, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return c, nil
	}
	if err != nil {
		return nil, err
	}
	var stored catalogFile
	if err := json.Unmarshal(payload, &stored); err != nil {
		return nil, err
	}
	if stored.Entries != nil {
		c.entries = stored.Entries
	}
	if stored.Runs != nil {
		c.runs = stored.Runs
	}
	return c, nil
}

func (c *Catalog) Entries() []Entry {
	c.mu.RLock()
	defer c.mu.RUnlock()
	items := make([]Entry, 0, len(c.entries))
	for _, entry := range c.entries {
		items = append(items, entry)
	}
	sort.Slice(items, func(i, j int) bool { return items[i].UpdatedAt.After(items[j].UpdatedAt) })
	return items
}

func (c *Catalog) Entry(id string) (Entry, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	entry, ok := c.entries[id]
	return entry, ok
}

func (c *Catalog) SaveEntry(entry Entry) (Entry, error) {
	entry.Title = strings.TrimSpace(entry.Title)
	entry.Body = strings.TrimSpace(entry.Body)
	entry.Type = strings.TrimSpace(entry.Type)
	if entry.Title == "" {
		return Entry{}, errors.New("title is required")
	}
	if entry.Type == "" {
		entry.Type = "Note"
	}
	now := time.Now().UTC()
	if entry.ID == "" {
		entry.ID = newID()
	}
	if entry.CreatedAt.IsZero() {
		entry.CreatedAt = now
	}
	entry.UpdatedAt = now
	entry.Tags = normalizeTags(entry.Tags)

	c.mu.Lock()
	defer c.mu.Unlock()
	previous, existed := c.entries[entry.ID]
	c.entries[entry.ID] = entry
	if err := c.persistLocked(); err != nil {
		if existed {
			c.entries[entry.ID] = previous
		} else {
			delete(c.entries, entry.ID)
		}
		return Entry{}, err
	}
	return entry, nil
}

func (c *Catalog) Runs() []Run {
	c.mu.RLock()
	defer c.mu.RUnlock()
	items := make([]Run, 0, len(c.runs))
	for _, run := range c.runs {
		items = append(items, run)
	}
	sort.Slice(items, func(i, j int) bool { return items[i].UpdatedAt.After(items[j].UpdatedAt) })
	return items
}

func (c *Catalog) SaveRun(run Run) (Run, error) {
	run.Name = strings.TrimSpace(run.Name)
	if run.Name == "" {
		return Run{}, errors.New("name is required")
	}
	if run.Status == "" {
		run.Status = "Queued"
	}
	now := time.Now().UTC()
	if run.ID == "" {
		run.ID = newID()
	}
	if run.CreatedAt.IsZero() {
		run.CreatedAt = now
	}
	run.UpdatedAt = now

	c.mu.Lock()
	defer c.mu.Unlock()
	previous, existed := c.runs[run.ID]
	c.runs[run.ID] = run
	if err := c.persistLocked(); err != nil {
		if existed {
			c.runs[run.ID] = previous
		} else {
			delete(c.runs, run.ID)
		}
		return Run{}, err
	}
	return run, nil
}

func (c *Catalog) Counts() (int, int) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.entries), len(c.runs)
}

func (c *Catalog) persistLocked() error {
	payload, err := json.MarshalIndent(catalogFile{Entries: c.entries, Runs: c.runs}, "", "  ")
	if err != nil {
		return err
	}
	temporary := c.path + ".tmp"
	file, err := os.OpenFile(temporary, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	if _, err := file.Write(payload); err != nil {
		file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	return os.Rename(temporary, c.path)
}

func normalizeTags(tags []string) []string {
	seen := make(map[string]struct{})
	result := make([]string, 0, len(tags))
	for _, tag := range tags {
		tag = strings.ToLower(strings.TrimSpace(tag))
		if tag == "" {
			continue
		}
		if _, exists := seen[tag]; exists {
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

func newID() string {
	value := make([]byte, 12)
	if _, err := rand.Read(value); err != nil {
		return hex.EncodeToString([]byte(time.Now().UTC().Format(time.RFC3339Nano)))
	}
	return hex.EncodeToString(value)
}
