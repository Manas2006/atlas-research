package tsdb

import (
	"encoding/binary"
	"errors"
	"math"
	"sort"
)

type Sample struct {
	Timestamp int64   `json:"timestamp"`
	Value     float64 `json:"value"`
}

// encodeSamples combines delta-of-delta timestamp compression with XOR value
// compression. It is intentionally simple and byte-aligned so the format is
// easy to inspect and fuzz.
func encodeSamples(input []Sample) []byte {
	if len(input) == 0 {
		return []byte{0}
	}
	samples := append([]Sample(nil), input...)
	sort.SliceStable(samples, func(i, j int) bool { return samples[i].Timestamp < samples[j].Timestamp })
	deduplicated := samples[:0]
	for _, sample := range samples {
		if len(deduplicated) > 0 && deduplicated[len(deduplicated)-1].Timestamp == sample.Timestamp {
			deduplicated[len(deduplicated)-1] = sample
		} else {
			deduplicated = append(deduplicated, sample)
		}
	}
	samples = deduplicated
	result := make([]byte, 0, len(samples)*4)
	result = appendUvarint(result, uint64(len(samples)))
	result = appendVarint(result, samples[0].Timestamp)
	previousBits := math.Float64bits(samples[0].Value)
	result = appendUvarint(result, previousBits)
	if len(samples) == 1 {
		return result
	}
	previousDelta := samples[1].Timestamp - samples[0].Timestamp
	result = appendVarint(result, previousDelta)
	bits := math.Float64bits(samples[1].Value)
	result = appendUvarint(result, bits^previousBits)
	previousBits = bits
	for index := 2; index < len(samples); index++ {
		delta := samples[index].Timestamp - samples[index-1].Timestamp
		result = appendVarint(result, delta-previousDelta)
		bits = math.Float64bits(samples[index].Value)
		result = appendUvarint(result, bits^previousBits)
		previousDelta, previousBits = delta, bits
	}
	return result
}

func decodeSamples(encoded []byte) ([]Sample, error) {
	count, read := binary.Uvarint(encoded)
	if read <= 0 {
		return nil, errors.New("invalid sample count")
	}
	encoded = encoded[read:]
	if count == 0 {
		return nil, nil
	}
	timestamp, read := binary.Varint(encoded)
	if read <= 0 {
		return nil, errors.New("invalid first timestamp")
	}
	encoded = encoded[read:]
	bits, read := binary.Uvarint(encoded)
	if read <= 0 {
		return nil, errors.New("invalid first value")
	}
	encoded = encoded[read:]
	result := make([]Sample, 0, count)
	result = append(result, Sample{Timestamp: timestamp, Value: math.Float64frombits(bits)})
	if count == 1 {
		return result, nil
	}
	delta, read := binary.Varint(encoded)
	if read <= 0 {
		return nil, errors.New("invalid first delta")
	}
	encoded = encoded[read:]
	xor, read := binary.Uvarint(encoded)
	if read <= 0 {
		return nil, errors.New("invalid value xor")
	}
	encoded = encoded[read:]
	bits ^= xor
	timestamp += delta
	result = append(result, Sample{Timestamp: timestamp, Value: math.Float64frombits(bits)})
	for index := uint64(2); index < count; index++ {
		deltaOfDelta, n := binary.Varint(encoded)
		if n <= 0 {
			return nil, errors.New("invalid delta of delta")
		}
		encoded = encoded[n:]
		xor, n = binary.Uvarint(encoded)
		if n <= 0 {
			return nil, errors.New("invalid value xor")
		}
		encoded = encoded[n:]
		delta += deltaOfDelta
		timestamp += delta
		bits ^= xor
		result = append(result, Sample{Timestamp: timestamp, Value: math.Float64frombits(bits)})
	}
	return result, nil
}

func appendUvarint(destination []byte, value uint64) []byte {
	var scratch [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(scratch[:], value)
	return append(destination, scratch[:n]...)
}

func appendVarint(destination []byte, value int64) []byte {
	var scratch [binary.MaxVarintLen64]byte
	n := binary.PutVarint(scratch[:], value)
	return append(destination, scratch[:n]...)
}
