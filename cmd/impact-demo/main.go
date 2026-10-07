package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"os"
	"path/filepath"

	"github.com/Manas2006/atlas-research/internal/experiments"
)

func main() {
	subjects := flag.Int("subjects", 12000, "synthetic subjects")
	controlRate := flag.Float64("control-rate", 0.08, "control conversion probability")
	lift := flag.Float64("lift", 0.04, "known absolute treatment lift")
	dataDir := flag.String("data", "", "directory used for the recovery exercise (temporary by default)")
	flag.Parse()
	directory := *dataDir
	if directory == "" {
		var err error
		directory, err = os.MkdirTemp("", "atlas-impact-demo-")
		if err != nil {
			fatal(err)
		}
		defer os.RemoveAll(directory)
	}
	path := filepath.Join(directory, "events.wal")
	engine, err := experiments.Open(path)
	if err != nil {
		fatal(err)
	}
	experiment, err := engine.Create(experiments.Experiment{Name: "Synthetic campaign holdout", Hypothesis: "the campaign creates incremental conversions", OutcomeName: "conversion", TreatmentAllocation: 0.5})
	if err != nil {
		fatal(err)
	}
	experiment, _, err = engine.SetStatus(experiment.ID, experiments.Running)
	if err != nil {
		fatal(err)
	}
	batches, err := experiments.SyntheticBatches(experiment, experiments.SyntheticConfig{Subjects: *subjects, ControlRate: *controlRate, AbsoluteLift: *lift, DuplicatePct: 0.05, Seed: 20261007})
	if err != nil {
		fatal(err)
	}
	deliveries := 0
	for _, batch := range batches {
		deliveries += len(batch.Events)
	}

	// Each batch is handed to the engine at its simulated arrival time, so
	// event-time lag reflects the modeled transport delay and disorder rather
	// than the age of the synthetic timestamps. Ack latency is wall-clock time.
	// Restart at the batch boundary where half of the deliveries are done.
	middle, delivered := 0, 0
	for middle < len(batches) && delivered < deliveries/2 {
		delivered += len(batches[middle].Events)
		middle++
	}
	process := func(from, to int) {
		for _, batch := range batches[from:to] {
			if _, _, err := engine.ProcessBatchAt(batch.Events, batch.ArrivedAt); err != nil {
				fatal(err)
			}
		}
	}
	process(0, middle)
	// Simulate a process crash/rebalance after durable processing but before
	// the source offset is committed. Reopen, then redeliver the last batch.
	// The simulated restart takes no stream time; its real cost is reported
	// as recovery_time_ms.
	if err := engine.Close(); err != nil {
		fatal(err)
	}
	engine, err = experiments.Open(path)
	if err != nil {
		fatal(err)
	}
	retryStart := middle - 1
	if retryStart < 0 {
		retryStart = 0
	}
	process(retryStart, middle)
	process(middle, len(batches))
	_, result, ok := engine.Get(experiment.ID)
	if !ok {
		fatal(fmt.Errorf("experiment missing after restart"))
	}
	if err := engine.Close(); err != nil {
		fatal(err)
	}
	historyPoints := len(result.History)
	result.History = nil
	output := map[string]any{
		"synthetic_data": true, "subjects": *subjects, "known_absolute_lift": *lift,
		"estimated_absolute_lift": result.AbsoluteLift, "absolute_error": math.Abs(result.AbsoluteLift - *lift),
		"duplicates_absorbed": result.Diagnostics.Duplicates, "convergence_points": historyPoints,
		"deliveries": deliveries, "delivery_batches": len(batches), "allowed_lateness_seconds": experiment.AllowedLatenessSeconds, "result": result,
	}
	if err := json.NewEncoder(os.Stdout).Encode(output); err != nil {
		fatal(err)
	}
	if result.TreatmentSampleSize+result.ControlSampleSize != int64(*subjects) || result.Diagnostics.Duplicates == 0 || math.Abs(result.AbsoluteLift-*lift) > 0.02 {
		fatal(fmt.Errorf("demo did not converge or recover correctly"))
	}
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "impact demo:", err)
	os.Exit(1)
}
