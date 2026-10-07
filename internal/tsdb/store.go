package tsdb

import (
	"bufio"
	"compress/gzip"
	"encoding/gob"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

type Series struct {
	Name   string            `json:"name"`
	Labels map[string]string `json:"labels,omitempty"`
}

type PointBatch struct {
	Series  Series   `json:"series"`
	Samples []Sample `json:"samples"`
}

type block struct {
	Version int
	Series  Series
	Start   int64
	End     int64
	Encoded []byte
}

type walRecord struct {
	Fingerprint string `json:"fingerprint"`
	Series      Series `json:"series"`
	Sample      Sample `json:"sample"`
}

type Store struct {
	mu        sync.RWMutex
	dir       string
	blockDir  string
	wal       *os.File
	head      map[string][]Sample
	series    map[string]Series
	blockSize int
}

func OpenStore(dir string, blockSize int) (*Store, error) {
	if blockSize <= 0 {
		blockSize = 4096
	}
	blockDir := filepath.Join(dir, "blocks")
	if err := os.MkdirAll(blockDir, 0o700); err != nil {
		return nil, err
	}
	wal, err := os.OpenFile(filepath.Join(dir, "samples.wal"), os.O_CREATE|os.O_APPEND|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	store := &Store{dir: dir, blockDir: blockDir, wal: wal, head: make(map[string][]Sample), series: make(map[string]Series), blockSize: blockSize}
	if err := store.loadBlockMetadata(); err != nil {
		wal.Close()
		return nil, err
	}
	if err := store.replayWAL(); err != nil {
		wal.Close()
		return nil, err
	}
	return store, nil
}

func (s *Store) Close() error { return s.wal.Close() }

func fingerprint(series Series) string {
	hash := fnv.New64a()
	_, _ = hash.Write([]byte(series.Name))
	keys := make([]string, 0, len(series.Labels))
	for key := range series.Labels {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		_, _ = hash.Write([]byte("\x00" + key + "=" + series.Labels[key]))
	}
	return fmt.Sprintf("%016x", hash.Sum64())
}

func (s *Store) Write(batch PointBatch) error {
	if strings.TrimSpace(batch.Series.Name) == "" {
		return errors.New("metric name is required")
	}
	key := fingerprint(batch.Series)
	s.mu.Lock()
	defer s.mu.Unlock()
	encoder := json.NewEncoder(s.wal)
	for _, sample := range batch.Samples {
		if err := encoder.Encode(walRecord{Fingerprint: key, Series: batch.Series, Sample: sample}); err != nil {
			return err
		}
		s.head[key] = append(s.head[key], sample)
	}
	if err := s.wal.Sync(); err != nil {
		return err
	}
	s.series[key] = batch.Series
	if len(s.head[key]) >= s.blockSize {
		return s.flushSeriesLocked(key)
	}
	return nil
}

func (s *Store) Query(series Series, start, end int64) ([]Sample, error) {
	key := fingerprint(series)
	s.mu.RLock()
	head := append([]Sample(nil), s.head[key]...)
	s.mu.RUnlock()
	entries, err := os.ReadDir(s.blockDir)
	if err != nil {
		return nil, err
	}
	result := make([]Sample, 0, len(head))
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasPrefix(entry.Name(), key+"-") {
			continue
		}
		stored, err := readBlock(filepath.Join(s.blockDir, entry.Name()))
		if err != nil {
			return nil, err
		}
		if stored.End < start || (end != 0 && stored.Start >= end) {
			continue
		}
		samples, err := decodeSamples(stored.Encoded)
		if err != nil {
			return nil, err
		}
		result = append(result, samples...)
	}
	result = append(result, head...)
	sort.SliceStable(result, func(i, j int) bool { return result[i].Timestamp < result[j].Timestamp })
	filtered := result[:0]
	for _, sample := range result {
		if sample.Timestamp < start || (end != 0 && sample.Timestamp >= end) {
			continue
		}
		if len(filtered) > 0 && filtered[len(filtered)-1].Timestamp == sample.Timestamp {
			filtered[len(filtered)-1] = sample
		} else {
			filtered = append(filtered, sample)
		}
	}
	return filtered, nil
}

func (s *Store) Flush() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for key := range s.head {
		if err := s.flushSeriesLocked(key); err != nil {
			return err
		}
	}
	if err := s.wal.Truncate(0); err != nil {
		return err
	}
	_, err := s.wal.Seek(0, 0)
	return err
}

func (s *Store) DeleteExpired(cutoff time.Time) (int, error) {
	entries, err := os.ReadDir(s.blockDir)
	if err != nil {
		return 0, err
	}
	deleted := 0
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".block") {
			continue
		}
		stored, err := readBlock(filepath.Join(s.blockDir, entry.Name()))
		if err != nil {
			return deleted, err
		}
		if stored.End < cutoff.UnixMilli() {
			if err := os.Remove(filepath.Join(s.blockDir, entry.Name())); err != nil {
				return deleted, err
			}
			deleted++
		}
	}
	return deleted, nil
}

func (s *Store) SeriesCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.series)
}

func (s *Store) flushSeriesLocked(key string) error {
	samples := s.head[key]
	if len(samples) == 0 {
		return nil
	}
	sort.SliceStable(samples, func(i, j int) bool { return samples[i].Timestamp < samples[j].Timestamp })
	stored := block{Version: 1, Series: s.series[key], Start: samples[0].Timestamp, End: samples[len(samples)-1].Timestamp, Encoded: encodeSamples(samples)}
	path := filepath.Join(s.blockDir, fmt.Sprintf("%s-%d-%d.block", key, stored.Start, stored.End))
	temporary := path + ".tmp"
	file, err := os.OpenFile(temporary, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	compressed := gzip.NewWriter(file)
	if err := gob.NewEncoder(compressed).Encode(stored); err != nil {
		compressed.Close()
		file.Close()
		return err
	}
	if err := compressed.Close(); err != nil {
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
	if err := os.Rename(temporary, path); err != nil {
		return err
	}
	delete(s.head, key)
	return nil
}

func readBlock(path string) (block, error) {
	file, err := os.Open(path)
	if err != nil {
		return block{}, err
	}
	defer file.Close()
	compressed, err := gzip.NewReader(file)
	if err != nil {
		return block{}, err
	}
	defer compressed.Close()
	var stored block
	err = gob.NewDecoder(compressed).Decode(&stored)
	if err == nil && stored.Version != 1 {
		err = fmt.Errorf("unsupported block version %d", stored.Version)
	}
	return stored, err
}

func (s *Store) loadBlockMetadata() error {
	entries, err := os.ReadDir(s.blockDir)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".block") {
			continue
		}
		stored, err := readBlock(filepath.Join(s.blockDir, entry.Name()))
		if err != nil {
			return err
		}
		s.series[fingerprint(stored.Series)] = stored.Series
	}
	return nil
}

func (s *Store) replayWAL() error {
	if _, err := s.wal.Seek(0, 0); err != nil {
		return err
	}
	scanner := bufio.NewScanner(s.wal)
	for scanner.Scan() {
		var record walRecord
		if err := json.Unmarshal(scanner.Bytes(), &record); err != nil {
			return err
		}
		s.series[record.Fingerprint] = record.Series
		s.head[record.Fingerprint] = append(s.head[record.Fingerprint], record.Sample)
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	_, err := s.wal.Seek(0, 2)
	return err
}
