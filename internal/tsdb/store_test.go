package tsdb

import (
	"math"
	"path/filepath"
	"testing"
)

func TestCodecRoundTrip(t *testing.T) {
	input := make([]Sample, 1000)
	for index := range input {
		input[index] = Sample{Timestamp: 1_700_000_000_000 + int64(index*1000), Value: 70 + math.Sin(float64(index)/10)}
	}
	decoded, err := decodeSamples(encodeSamples(input))
	if err != nil {
		t.Fatal(err)
	}
	if len(decoded) != len(input) {
		t.Fatalf("decoded %d samples", len(decoded))
	}
	for index := range input {
		if decoded[index] != input[index] {
			t.Fatalf("sample %d changed: %#v", index, decoded[index])
		}
	}
}

func TestWALFlushBlocksAndRecovery(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "database")
	store, err := OpenStore(dir, 3)
	if err != nil {
		t.Fatal(err)
	}
	series := Series{Name: "cpu_usage", Labels: map[string]string{"host": "api-1"}}
	if err := store.Write(PointBatch{Series: series, Samples: []Sample{{Timestamp: 1000, Value: 1}, {Timestamp: 2000, Value: 2}, {Timestamp: 3000, Value: 3}}}); err != nil {
		t.Fatal(err)
	}
	if err := store.Write(PointBatch{Series: series, Samples: []Sample{{Timestamp: 4000, Value: 4}}}); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenStore(dir, 3)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	samples, err := reopened.Query(series, 1500, 4500)
	if err != nil {
		t.Fatal(err)
	}
	if len(samples) != 3 || samples[0].Value != 2 || samples[2].Value != 4 {
		t.Fatalf("unexpected recovery: %#v", samples)
	}
}

func BenchmarkCodec(b *testing.B) {
	samples := make([]Sample, 4096)
	for index := range samples {
		samples[index] = Sample{Timestamp: int64(index * 1000), Value: float64(index % 100)}
	}
	b.SetBytes(int64(len(samples) * 16))
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		_ = encodeSamples(samples)
	}
}
