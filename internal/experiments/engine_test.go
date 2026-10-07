package experiments

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func createRunning(t *testing.T, engine *Engine, mutate func(*Experiment)) Experiment {
	t.Helper()
	config := Experiment{Name: "Campaign lift", Hypothesis: "incremental conversions", OutcomeName: "conversion", TreatmentAllocation: 0.5, ObservationWindowSeconds: 3600, AllowedLatenessSeconds: 300}
	if mutate != nil {
		mutate(&config)
	}
	created, err := engine.Create(config)
	if err != nil {
		t.Fatal(err)
	}
	started, _, err := engine.SetStatus(created.ID, Running)
	if err != nil {
		t.Fatal(err)
	}
	return started
}

func TestDeterministicAssignmentStabilityAndDistribution(t *testing.T) {
	engine, err := Open(filepath.Join(t.TempDir(), "events.wal"))
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	experiment := createRunning(t, engine, func(config *Experiment) { config.TreatmentAllocation = 0.3 })
	treatment := 0
	for index := 0; index < 10000; index++ {
		subject := fmt.Sprintf("subject-%d", index)
		first, partitionOne, err := engine.Assign(experiment.ID, subject)
		if err != nil {
			t.Fatal(err)
		}
		second, partitionTwo, _ := engine.Assign(experiment.ID, subject)
		if first != second || partitionOne != partitionTwo {
			t.Fatalf("unstable assignment for %s", subject)
		}
		if first == experiment.TreatmentArm {
			treatment++
		}
	}
	share := float64(treatment) / 10000
	if math.Abs(share-0.3) > 0.02 {
		t.Fatalf("treatment share %.3f", share)
	}
}

func TestDuplicateOutOfOrderAttributionAndWindow(t *testing.T) {
	engine, _ := Open(filepath.Join(t.TempDir(), "events.wal"))
	defer engine.Close()
	experiment := createRunning(t, engine, nil)
	base := time.Now().Add(-time.Minute).UTC()
	subject := "out-of-order"
	arm, _, _ := engine.Assign(experiment.ID, subject)
	value := 40.0
	outcome := Event{EventID: "outcome-1", ExperimentID: experiment.ID, Type: Outcome, SubjectID: subject, Arm: arm, Timestamp: base.Add(time.Minute), OutcomeName: "conversion", Value: &value, ConfigVersion: 1}
	if receipt, _, err := engine.Process(outcome); err != nil || receipt.Status != "accepted" {
		t.Fatalf("outcome: %#v %v", receipt, err)
	}
	_, before, _ := engine.Get(experiment.ID)
	if before.Diagnostics.UnmatchedOutcomes != 1 || before.TreatmentSampleSize+before.ControlSampleSize != 0 {
		t.Fatalf("unexpected pre-join result: %+v", before)
	}
	exposure := Event{EventID: "exposure-1", ExperimentID: experiment.ID, Type: Exposure, SubjectID: subject, Arm: arm, Timestamp: base, ConfigVersion: 1}
	_, joined, err := engine.Process(exposure)
	if err != nil {
		t.Fatal(err)
	}
	if joined.TreatmentSampleSize+joined.ControlSampleSize != 1 || joined.TreatmentConversions+joined.ControlConversions != 1 {
		t.Fatalf("join failed: %+v", joined)
	}
	if receipt, duplicate, err := engine.Process(outcome); err != nil || receipt.Status != "duplicate" || duplicate.Diagnostics.Duplicates != 1 {
		t.Fatalf("duplicate: %#v %+v %v", receipt, duplicate, err)
	}

	other := "outside-window"
	otherArm, _, _ := engine.Assign(experiment.ID, other)
	_, _, _ = engine.Process(Event{EventID: "exposure-2", ExperimentID: experiment.ID, Type: Exposure, SubjectID: other, Arm: otherArm, Timestamp: base.Add(2 * time.Minute), ConfigVersion: 1})
	_, result, _ := engine.Process(Event{EventID: "outcome-2", ExperimentID: experiment.ID, Type: Outcome, SubjectID: other, Arm: otherArm, Timestamp: base.Add(2 * time.Hour), OutcomeName: "conversion", ConfigVersion: 1})
	if result.TreatmentConversions+result.ControlConversions != 1 {
		t.Fatalf("outside-window outcome counted: %+v", result)
	}
}

func TestLateAndRejectedEventsAreDiagnosed(t *testing.T) {
	engine, _ := Open(filepath.Join(t.TempDir(), "events.wal"))
	defer engine.Close()
	experiment := createRunning(t, engine, func(config *Experiment) { config.AllowedLatenessSeconds = 10 })
	base := time.Now().UTC()
	arm, _, _ := engine.Assign(experiment.ID, "s1")
	_, _, _ = engine.Process(Event{EventID: "new", ExperimentID: experiment.ID, Type: Exposure, SubjectID: "s1", Arm: arm, Timestamp: base, ConfigVersion: 1})
	receipt, result, _ := engine.Process(Event{EventID: "old", ExperimentID: experiment.ID, Type: Outcome, SubjectID: "s1", Arm: arm, Timestamp: base.Add(-time.Minute), OutcomeName: "conversion", ConfigVersion: 1})
	if receipt.Status != "late" || result.Diagnostics.Late != 1 {
		t.Fatalf("late policy not applied: %#v %+v", receipt, result.Diagnostics)
	}
	receipt, result, _ = engine.Process(Event{EventID: "wrong-version", ExperimentID: experiment.ID, Type: Exposure, SubjectID: "s2", Timestamp: base, ConfigVersion: 2})
	if receipt.Status != "rejected" || result.Diagnostics.Rejected != 1 {
		t.Fatalf("bad version not rejected: %#v %+v", receipt, result.Diagnostics)
	}
}

func TestRestartRecoveryAndRetryAfterLostAcknowledgement(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.wal")
	engine, _ := Open(path)
	experiment := createRunning(t, engine, nil)
	at := time.Now().UTC()
	arm, _, _ := engine.Assign(experiment.ID, "durable")
	event := Event{EventID: "durable-exposure", ExperimentID: experiment.ID, Type: Exposure, SubjectID: "durable", Arm: arm, Timestamp: at, ConfigVersion: 1}
	if _, _, err := engine.Process(event); err != nil {
		t.Fatal(err)
	}
	// Closing immediately after Process models a crash before an upstream
	// consumer commits its offset. The redelivery below must be absorbed.
	if err := engine.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	receipt, result, err := reopened.Process(event)
	if err != nil || receipt.Status != "duplicate" {
		t.Fatalf("redelivery: %#v %v", receipt, err)
	}
	if result.TreatmentSampleSize+result.ControlSampleSize != 1 || result.Diagnostics.Duplicates != 1 {
		t.Fatalf("recovery double-counted: %+v", result)
	}
}

func TestRestartDropsOnlyPartialUnacknowledgedWALTail(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.wal")
	engine, _ := Open(path)
	experiment := createRunning(t, engine, nil)
	if err := engine.Close(); err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteString(`{"kind":"events","events":[`); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, ok := reopened.Get(experiment.ID); !ok {
		t.Fatal("acknowledged experiment was lost")
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
	again, err := Open(path)
	if err != nil {
		t.Fatalf("partial tail was not removed: %v", err)
	}
	defer again.Close()
}

func TestAggregateAndConfidenceInterval(t *testing.T) {
	engine, _ := Open(filepath.Join(t.TempDir(), "events.wal"))
	defer engine.Close()
	experiment := createRunning(t, engine, nil)
	base := time.Now().UTC()
	counts := map[string]int{"treatment": 0, "control": 0}
	for index := 0; counts["treatment"] < 100 || counts["control"] < 100; index++ {
		subject := fmt.Sprintf("ci-%d", index)
		arm, _, _ := engine.Assign(experiment.ID, subject)
		if counts[arm] >= 100 {
			continue
		}
		position := counts[arm]
		counts[arm]++
		_, _, _ = engine.Process(Event{EventID: fmt.Sprintf("e-%d", index), ExperimentID: experiment.ID, Type: Exposure, SubjectID: subject, Arm: arm, Timestamp: base, ConfigVersion: 1})
		limit := 20
		if arm == experiment.TreatmentArm {
			limit = 30
		}
		if position < limit {
			_, _, _ = engine.Process(Event{EventID: fmt.Sprintf("o-%d", index), ExperimentID: experiment.ID, Type: Outcome, SubjectID: subject, Arm: arm, Timestamp: base.Add(time.Second), OutcomeName: "conversion", ConfigVersion: 1})
		}
	}
	_, result, _ := engine.Get(experiment.ID)
	if math.Abs(result.AbsoluteLift-0.1) > 1e-9 {
		t.Fatalf("lift: %+v", result)
	}
	if result.ConfidenceLow >= result.AbsoluteLift || result.ConfidenceHigh <= result.AbsoluteLift || result.InsufficientData {
		t.Fatalf("CI/state: %+v", result)
	}
	if result.Recommendation != "continue collecting data" {
		t.Fatalf("unexpected recommendation: %s", result.Recommendation)
	}
}

func TestKnownSyntheticLiftConvergesWithDuplicatesAndPartitionOrder(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.wal")
	engine, _ := Open(path)
	experiment := createRunning(t, engine, func(config *Experiment) { config.ObservationWindowSeconds = int64((7 * 24 * time.Hour).Seconds()) })
	batches, err := SyntheticBatches(experiment, SyntheticConfig{Subjects: 12000, ControlRate: 0.1, AbsoluteLift: 0.04, DuplicatePct: 0.05, Seed: 9})
	if err != nil {
		t.Fatal(err)
	}
	// This test exercises recovery with large, fixed-size batches, so it
	// regroups the delivery order into 500-event chunks, each handed over when
	// its last event has arrived. Fixed-size chunks wait hours to fill in the
	// sparse outcome tail, so lag is asserted in the demo-style test instead.
	var events []Event
	var arrivals []time.Time
	for _, batch := range batches {
		for _, event := range batch.Events {
			events = append(events, event)
			arrivals = append(arrivals, batch.ArrivedAt)
		}
	}
	// Process half, revoke ownership (close), then reassign/recover and finish.
	for start := 0; start < len(events)/2; start += 500 {
		end := start + 500
		if end > len(events)/2 {
			end = len(events) / 2
		}
		if _, _, err := engine.ProcessBatchAt(events[start:end], arrivals[end-1]); err != nil {
			t.Fatal(err)
		}
	}
	if err := engine.Close(); err != nil {
		t.Fatal(err)
	}
	engine, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	for start := len(events) / 2; start < len(events); start += 500 {
		end := start + 500
		if end > len(events) {
			end = len(events)
		}
		if _, _, err := engine.ProcessBatchAt(events[start:end], arrivals[end-1]); err != nil {
			t.Fatal(err)
		}
	}
	_, result, _ := engine.Get(experiment.ID)
	if math.Abs(result.AbsoluteLift-0.04) > 0.018 {
		t.Fatalf("estimated lift did not converge: %+v", result)
	}
	if result.Diagnostics.Duplicates == 0 || result.TreatmentSampleSize+result.ControlSampleSize != 12000 {
		t.Fatalf("recovery/dedup failed: %+v", result)
	}
}

func TestEventTimeLagUsesStreamClockAndAckLatencyUsesLatencyClock(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.wal")
	engine, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	// The wall clock sits nine months after the historical events below. If
	// lag were measured against it, every sample would be about 279 days.
	wall := time.Date(2026, 10, 7, 12, 0, 0, 0, time.UTC)
	engine.now = func() time.Time { return wall }
	// Each read of the latency clock advances it 1 ms more than the previous
	// read, so batch one acknowledges in 2 ms and batch two in 4 ms.
	tick, step := time.Unix(0, 0), time.Duration(0)
	engine.latencyClock = func() time.Time {
		step += time.Millisecond
		tick = tick.Add(step)
		return tick
	}
	experiment := createRunning(t, engine, func(config *Experiment) { config.AllowedLatenessSeconds = 60 })
	arm, _, _ := engine.Assign(experiment.ID, "s1")
	base := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	event := func(id string, at time.Duration, version int) Event {
		return Event{EventID: id, ExperimentID: experiment.ID, Type: Exposure, SubjectID: "s1", Arm: arm, Timestamp: base.Add(at), ConfigVersion: version}
	}
	if _, _, err := engine.ProcessBatchAt([]Event{event("e1", 9*time.Second, 1), event("e2", 8500*time.Millisecond, 1)}, base.Add(10*time.Second)); err != nil {
		t.Fatal(err)
	}
	// Batch two arrives two seconds later with an out-of-order record (e4),
	// a duplicate (e1), and a rejected record (e5). Only e3 and e4 add lag.
	_, result, err := engine.ProcessBatchAt([]Event{event("e3", 11750*time.Millisecond, 1), event("e4", 7*time.Second, 1), event("e1", 9*time.Second, 1), event("e5", 11*time.Second, 2)}, base.Add(12*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	// Lags are 1000, 1500, 250, and 5000 ms; nearest-rank p50 is the second
	// smallest and p95/p99 are the largest.
	want := Diagnostics{ProcessingLagP50: 1000, ProcessingLagP95: 5000, ProcessingLagP99: 5000, ProcessingLagSamples: 4, WatermarkLag: 60250, AckLatencyP50: 2, AckLatencyP95: 4, AckLatencyP99: 4, AckLatencySamples: 2}
	got := result.Diagnostics
	if got.ProcessingLagP50 != want.ProcessingLagP50 || got.ProcessingLagP95 != want.ProcessingLagP95 || got.ProcessingLagP99 != want.ProcessingLagP99 || got.ProcessingLagSamples != want.ProcessingLagSamples {
		t.Fatalf("processing lag: got %+v, want %+v", got, want)
	}
	// Watermark = newest event time (base+11.75s) minus 60 s of lateness; the
	// newest arrival is base+12s, so the watermark trails it by 60.25 s.
	if got.WatermarkLag != want.WatermarkLag {
		t.Fatalf("watermark lag: got %v, want %v", got.WatermarkLag, want.WatermarkLag)
	}
	if got.AckLatencyP50 != want.AckLatencyP50 || got.AckLatencyP95 != want.AckLatencyP95 || got.AckLatencyP99 != want.AckLatencyP99 || got.AckLatencySamples != want.AckLatencySamples {
		t.Fatalf("ack latency: got %+v, want %+v", got, want)
	}
	if got.Accepted != 4 || got.Duplicates != 1 || got.Rejected != 1 {
		t.Fatalf("counts: %+v", got)
	}
	// A later read must not change event-time diagnostics.
	wall = wall.Add(24 * time.Hour)
	if _, later, _ := engine.Get(experiment.ID); later.Diagnostics.WatermarkLag != want.WatermarkLag || later.Diagnostics.ProcessingLagP95 != want.ProcessingLagP95 {
		t.Fatalf("idle read changed lag: %+v", later.Diagnostics)
	}

	// Replay runs on the real clocks. It must rebuild event-time lag from the
	// persisted arrival times and record no ack latency.
	if err := engine.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	_, recovered, _ := reopened.Get(experiment.ID)
	rd := recovered.Diagnostics
	if rd.ProcessingLagP50 != want.ProcessingLagP50 || rd.ProcessingLagP95 != want.ProcessingLagP95 || rd.ProcessingLagP99 != want.ProcessingLagP99 || rd.ProcessingLagSamples != want.ProcessingLagSamples || rd.WatermarkLag != want.WatermarkLag {
		t.Fatalf("replay changed event-time lag: got %+v, want %+v", rd, want)
	}
	if rd.AckLatencySamples != 0 || rd.AckLatencyP50 != 0 || rd.AckLatencyP99 != 0 {
		t.Fatalf("replay recorded ack latency: %+v", rd)
	}

	// Live ingestion uses the wall clock as the arrival time. A record dated
	// after its arrival (producer clock skew) contributes zero lag.
	live := createRunning(t, reopened, func(config *Experiment) { config.AllowedLatenessSeconds = 60 })
	reopened.now = func() time.Time { return wall }
	liveArm, _, _ := reopened.Assign(live.ID, "live")
	_, liveResult, err := reopened.ProcessBatch([]Event{
		{EventID: "l1", ExperimentID: live.ID, Type: Exposure, SubjectID: "live", Arm: liveArm, Timestamp: wall.Add(-2 * time.Second), ConfigVersion: 1},
		{EventID: "l2", ExperimentID: live.ID, Type: Exposure, SubjectID: "live", Arm: liveArm, Timestamp: wall.Add(time.Second), ConfigVersion: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	ld := liveResult.Diagnostics
	if ld.ProcessingLagP50 != 0 || ld.ProcessingLagP95 != 2000 || ld.ProcessingLagSamples != 2 || ld.WatermarkLag != 59000 || ld.AckLatencySamples != 1 {
		t.Fatalf("live lag: %+v", ld)
	}
}

func TestReplayOfLegacyBatchWithoutArrivalTimeAddsNoLagSamples(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.wal")
	engine, _ := Open(path)
	experiment := createRunning(t, engine, nil)
	if err := engine.Close(); err != nil {
		t.Fatal(err)
	}
	arm := assign(experiment, "legacy")
	// Records written before arrival times were persisted have no
	// arrived_at; the old engine measured their lag against replay time.
	legacy, err := json.Marshal(walRecord{Kind: "events", Events: []Event{{EventID: "old", ExperimentID: experiment.ID, Type: Exposure, SubjectID: "legacy", Arm: arm, Timestamp: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), ConfigVersion: 1}}})
	if err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write(append(legacy, '\n')); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	_, result, _ := reopened.Get(experiment.ID)
	if result.Diagnostics.Accepted != 1 || result.Diagnostics.ProcessingLagSamples != 0 || result.Diagnostics.ProcessingLagP99 != 0 || result.Diagnostics.WatermarkLag != 0 {
		t.Fatalf("legacy replay: %+v", result.Diagnostics)
	}
}

// TestDemoStyleRunReportsPlausibleLatencies mirrors cmd/impact-demo: default
// lateness, the demo's seed and delivery model, a restart at the midpoint,
// and redelivery of the uncertain batch. It uses fewer subjects than the demo
// so the race-enabled run stays short.
func TestDemoStyleRunReportsPlausibleLatencies(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.wal")
	engine, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	created, err := engine.Create(Experiment{ID: "demo-style-latency", Name: "Synthetic campaign holdout", Hypothesis: "the campaign creates incremental conversions", OutcomeName: "conversion", TreatmentAllocation: 0.5})
	if err != nil {
		t.Fatal(err)
	}
	experiment, _, err := engine.SetStatus(created.ID, Running)
	if err != nil {
		t.Fatal(err)
	}
	const subjects = 3000
	batches, err := SyntheticBatches(experiment, SyntheticConfig{Subjects: subjects, ControlRate: 0.08, AbsoluteLift: 0.04, DuplicatePct: 0.05, Seed: 20261007})
	if err != nil {
		t.Fatal(err)
	}
	// The generator must deliver out of event-time order on a nondecreasing
	// arrival clock, and never before an event happened.
	var newest, previousArrival time.Time
	disordered := 0
	for _, batch := range batches {
		if batch.ArrivedAt.Before(previousArrival) {
			t.Fatalf("arrival clock went backwards at %s", batch.ArrivedAt)
		}
		previousArrival = batch.ArrivedAt
		for _, event := range batch.Events {
			if event.Timestamp.After(batch.ArrivedAt) {
				t.Fatalf("event %s delivered before it happened", event.EventID)
			}
			if event.Timestamp.Before(newest) {
				disordered++
			} else {
				newest = event.Timestamp
			}
		}
	}
	if disordered == 0 {
		t.Fatal("synthetic delivery is in perfect event-time order")
	}

	process := func(from, to int) {
		for _, batch := range batches[from:to] {
			if _, _, err := engine.ProcessBatchAt(batch.Events, batch.ArrivedAt); err != nil {
				t.Fatal(err)
			}
		}
	}
	deliveries := 0
	for _, batch := range batches {
		deliveries += len(batch.Events)
	}
	middle, delivered := 0, 0
	for middle < len(batches) && delivered < deliveries/2 {
		delivered += len(batches[middle].Events)
		middle++
	}
	process(0, middle)
	if err := engine.Close(); err != nil {
		t.Fatal(err)
	}
	if engine, err = Open(path); err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	process(middle-1, len(batches))
	_, result, _ := engine.Get(experiment.ID)
	d := result.Diagnostics
	if result.TreatmentSampleSize+result.ControlSampleSize != subjects || d.Duplicates == 0 || d.Late != 0 {
		t.Fatalf("demo-style run did not recover: %+v", result)
	}
	// Simulated event-time lag: at least the 50 ms transport floor and at most
	// the generator's 74 s bound for a redelivered duplicate.
	const maxLagMS = 74000
	if d.ProcessingLagSamples == 0 || d.ProcessingLagP50 < 50 || d.ProcessingLagP50 > d.ProcessingLagP95 || d.ProcessingLagP95 > d.ProcessingLagP99 || d.ProcessingLagP99 > maxLagMS {
		t.Fatalf("implausible processing lag: %+v", d)
	}
	lateness := float64(experiment.AllowedLatenessSeconds * 1000)
	if d.WatermarkLag < lateness || d.WatermarkLag > lateness+maxLagMS {
		t.Fatalf("implausible watermark lag: %v ms with %v ms lateness", d.WatermarkLag, lateness)
	}
	// Ack latency is a real wall-clock measurement, so the bound is loose
	// enough for a slow CI disk under the race detector. Only batches
	// acknowledged after the restart count: replay adds none.
	if want := min(len(batches)-middle+1, latencyWindow); d.AckLatencySamples != want {
		t.Fatalf("ack latency samples %d, want %d post-restart batches", d.AckLatencySamples, want)
	}
	if d.AckLatencyP50 <= 0 || d.AckLatencyP50 > d.AckLatencyP95 || d.AckLatencyP95 > d.AckLatencyP99 || d.AckLatencyP99 > 5000 {
		t.Fatalf("implausible ack latency: %+v", d)
	}
}

func BenchmarkProcessBatch(b *testing.B) {
	engine, err := Open(filepath.Join(b.TempDir(), "events.wal"))
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = engine.Close() })
	experiment, err := engine.Create(Experiment{Name: "benchmark", Hypothesis: "throughput", OutcomeName: "success", TreatmentAllocation: 0.5})
	if err != nil {
		b.Fatal(err)
	}
	experiment, _, err = engine.SetStatus(experiment.ID, Running)
	if err != nil {
		b.Fatal(err)
	}
	const batchSize = 256
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		events := make([]Event, batchSize)
		for index := range events {
			subject := fmt.Sprintf("bench-%d-%d", iteration, index)
			arm, _, _ := engine.Assign(experiment.ID, subject)
			events[index] = Event{EventID: subject, ExperimentID: experiment.ID, Type: Exposure, SubjectID: subject, Arm: arm, Timestamp: time.Now().UTC(), ConfigVersion: 1}
		}
		if _, _, err := engine.ProcessBatch(events); err != nil {
			b.Fatal(err)
		}
	}
	if elapsed := b.Elapsed().Seconds(); elapsed > 0 {
		b.ReportMetric(float64(b.N*batchSize)/elapsed, "events/s")
	}
}
