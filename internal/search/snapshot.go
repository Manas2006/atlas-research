package search

import (
	"compress/gzip"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"io"
	"math"
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
// Ordinals follow ascending document ID order, so the format does not depend
// on internal slot numbers and is unchanged from the map-based index.
func (i *Index) WriteSnapshot(destination io.Writer) error {
	i.mu.RLock()
	defer i.mu.RUnlock()

	ids := make([]string, 0, len(i.slots))
	for id := range i.slots {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	ordinals := make([]uint32, len(i.docs)) // by slot; unset for tombstones
	snapshot := diskSnapshot{
		Version:  snapshotVersion,
		Docs:     make([]Document, len(ids)),
		Lengths:  make([]int, len(ids)),
		Postings: make(map[string][]byte, len(i.terms)),
	}
	for ordinal, id := range ids {
		slot := i.slots[id]
		ordinals[slot] = uint32(ordinal)
		snapshot.Docs[ordinal] = i.docs[slot]
		snapshot.Lengths[ordinal] = int(i.lengths[slot])
	}

	var scratch [binary.MaxVarintLen64]byte
	var live []posting
	for term, entry := range i.terms {
		live = live[:0]
		for _, p := range entry.postings {
			if i.live[p.doc] {
				live = append(live, posting{doc: ordinals[p.doc], tf: p.tf})
			}
		}
		sort.Slice(live, func(a, b int) bool { return live[a].doc < live[b].doc })
		encoded := make([]byte, 0, len(live)*2)
		previous := -1
		for _, p := range live {
			ordinal := int(p.doc)
			gap := ordinal - previous
			n := binary.PutUvarint(scratch[:], uint64(gap))
			encoded = append(encoded, scratch[:n]...)
			n = binary.PutUvarint(scratch[:], uint64(p.tf))
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
	if uint64(len(snapshot.Docs)) >= noDoc {
		return nil, fmt.Errorf("snapshot has too many documents: %d", len(snapshot.Docs))
	}

	// Snapshot ordinals become slots directly: they are dense and every
	// posting list is stored in ascending ordinal order.
	index := NewIndex()
	index.docs = snapshot.Docs
	index.lengths = make([]uint32, len(snapshot.Docs))
	index.live = make([]bool, len(snapshot.Docs))
	for ordinal, doc := range snapshot.Docs {
		if _, duplicate := index.slots[doc.ID]; duplicate {
			return nil, fmt.Errorf("corrupt snapshot: duplicate document %q", doc.ID)
		}
		length := snapshot.Lengths[ordinal]
		if length < 0 || uint64(length) > math.MaxUint32 {
			return nil, fmt.Errorf("corrupt snapshot: invalid length %d for %q", length, doc.ID)
		}
		index.slots[doc.ID] = uint32(ordinal)
		index.lengths[ordinal] = uint32(length)
		index.live[ordinal] = true
		index.tokens += int64(length)
	}
	for term, encoded := range snapshot.Postings {
		entry := &termPostings{}
		previous := -1
		for len(encoded) > 0 {
			gap, n := binary.Uvarint(encoded)
			if n <= 0 {
				return nil, fmt.Errorf("corrupt posting for %q", term)
			}
			encoded = encoded[n:]
			frequency, n := binary.Uvarint(encoded)
			if n <= 0 || frequency > math.MaxUint32 {
				return nil, fmt.Errorf("corrupt frequency for %q", term)
			}
			encoded = encoded[n:]
			if gap == 0 || gap > uint64(len(snapshot.Docs)) {
				return nil, fmt.Errorf("invalid document gap %d for %q", gap, term)
			}
			ordinal := previous + int(gap)
			if ordinal < 0 || ordinal >= len(snapshot.Docs) {
				return nil, fmt.Errorf("invalid document ordinal %d", ordinal)
			}
			entry.postings = append(entry.postings, posting{doc: uint32(ordinal), tf: uint32(frequency)})
			entry.addImpact(uint32(frequency), index.lengths[ordinal])
			previous = ordinal
		}
		if entry.df = len(entry.postings); entry.df > 0 {
			index.terms[term] = entry
		}
	}
	return index, nil
}
