package search

import (
	"compress/gzip"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"io"
	"sort"
)

const snapshotVersion = 1

type diskSnapshot struct {
	Version  int
	Docs     []Document
	Lengths  []int
	Postings map[string][]byte
}

// WriteSnapshot writes a gzip-compressed term dictionary. Each posting list is
// independently encoded using delta document ordinals and unsigned varints.
func (i *Index) WriteSnapshot(destination io.Writer) error {
	i.mu.RLock()
	defer i.mu.RUnlock()

	ids := make([]string, 0, len(i.docs))
	for id := range i.docs {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	ordinals := make(map[string]int, len(ids))
	snapshot := diskSnapshot{
		Version:  snapshotVersion,
		Docs:     make([]Document, len(ids)),
		Lengths:  make([]int, len(ids)),
		Postings: make(map[string][]byte, len(i.postings)),
	}
	for ordinal, id := range ids {
		ordinals[id] = ordinal
		snapshot.Docs[ordinal] = i.docs[id]
		snapshot.Lengths[ordinal] = i.lengths[id]
	}

	var scratch [binary.MaxVarintLen64]byte
	for term, posting := range i.postings {
		postingIDs := make([]string, 0, len(posting))
		for id := range posting {
			postingIDs = append(postingIDs, id)
		}
		sort.Slice(postingIDs, func(a, b int) bool {
			return ordinals[postingIDs[a]] < ordinals[postingIDs[b]]
		})
		encoded := make([]byte, 0, len(postingIDs)*2)
		previous := -1
		for _, id := range postingIDs {
			ordinal := ordinals[id]
			gap := ordinal - previous
			n := binary.PutUvarint(scratch[:], uint64(gap))
			encoded = append(encoded, scratch[:n]...)
			n = binary.PutUvarint(scratch[:], uint64(posting[id]))
			encoded = append(encoded, scratch[:n]...)
			previous = ordinal
		}
		snapshot.Postings[term] = encoded
	}

	compressed := gzip.NewWriter(destination)
	if err := gob.NewEncoder(compressed).Encode(snapshot); err != nil {
		_ = compressed.Close()
		return err
	}
	return compressed.Close()
}

func ReadSnapshot(source io.Reader) (*Index, error) {
	compressed, err := gzip.NewReader(source)
	if err != nil {
		return nil, err
	}
	defer compressed.Close()
	var snapshot diskSnapshot
	if err := gob.NewDecoder(compressed).Decode(&snapshot); err != nil {
		return nil, err
	}
	if snapshot.Version != snapshotVersion {
		return nil, fmt.Errorf("unsupported snapshot version %d", snapshot.Version)
	}
	if len(snapshot.Docs) != len(snapshot.Lengths) {
		return nil, fmt.Errorf("corrupt snapshot: document length table mismatch")
	}

	index := NewIndex()
	for ordinal, doc := range snapshot.Docs {
		index.docs[doc.ID] = doc
		index.lengths[doc.ID] = snapshot.Lengths[ordinal]
		index.tokens += int64(snapshot.Lengths[ordinal])
	}
	for term, encoded := range snapshot.Postings {
		posting := make(map[string]int)
		previous := -1
		for len(encoded) > 0 {
			gap, n := binary.Uvarint(encoded)
			if n <= 0 {
				return nil, fmt.Errorf("corrupt posting for %q", term)
			}
			encoded = encoded[n:]
			frequency, n := binary.Uvarint(encoded)
			if n <= 0 {
				return nil, fmt.Errorf("corrupt frequency for %q", term)
			}
			encoded = encoded[n:]
			ordinal := previous + int(gap)
			if ordinal < 0 || ordinal >= len(snapshot.Docs) {
				return nil, fmt.Errorf("invalid document ordinal %d", ordinal)
			}
			posting[snapshot.Docs[ordinal].ID] = int(frequency)
			previous = ordinal
		}
		index.postings[term] = posting
	}
	return index, nil
}
