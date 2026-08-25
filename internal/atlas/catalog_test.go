package atlas

import (
	"path/filepath"
	"reflect"
	"testing"
)

func TestCatalogPersistsEntriesAndRuns(t *testing.T) {
	path := filepath.Join(t.TempDir(), "catalog.json")
	catalog, err := OpenCatalog(path)
	if err != nil {
		t.Fatal(err)
	}
	entry, err := catalog.SaveEntry(Entry{Title: "Recovery notes", Body: "Replay the WAL", Type: "Note", Tags: []string{" Search ", "search", "WAL"}})
	if err != nil {
		t.Fatal(err)
	}
	run, err := catalog.SaveRun(Run{Name: "recovery-canary", Model: "atlas", Dataset: "failures", Status: "Running"})
	if err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenCatalog(path)
	if err != nil {
		t.Fatal(err)
	}
	gotEntry, ok := reopened.Entry(entry.ID)
	if !ok {
		t.Fatal("entry was not recovered")
	}
	if !reflect.DeepEqual(gotEntry.Tags, []string{"search", "wal"}) {
		t.Fatalf("unexpected normalized tags: %v", gotEntry.Tags)
	}
	if runs := reopened.Runs(); len(runs) != 1 || runs[0].ID != run.ID {
		t.Fatalf("run was not recovered: %+v", runs)
	}
}

func TestCatalogRejectsMissingNames(t *testing.T) {
	catalog, err := OpenCatalog(filepath.Join(t.TempDir(), "catalog.json"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := catalog.SaveEntry(Entry{}); err == nil {
		t.Fatal("expected missing entry title to fail")
	}
	if _, err := catalog.SaveRun(Run{}); err == nil {
		t.Fatal("expected missing run name to fail")
	}
}
