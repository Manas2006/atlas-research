package experiments

import (
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
	events, err := SyntheticEvents(experiment, SyntheticConfig{Subjects: 12000, ControlRate: 0.1, AbsoluteLift: 0.04, DuplicatePct: 0.05, Seed: 9})
	if err != nil {
		t.Fatal(err)
	}
	// Process half, revoke ownership (close), then reassign/recover and finish.
	for start := 0; start < len(events)/2; start += 500 {
		end := start + 500
		if end > len(events)/2 {
			end = len(events) / 2
		}
		if _, _, err := engine.ProcessBatch(events[start:end]); err != nil {
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
		if _, _, err := engine.ProcessBatch(events[start:end]); err != nil {
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
