package experiments

import (
	"fmt"
	"math"
	"math/rand"
	"sort"
	"time"
)

type SyntheticConfig struct {
	Subjects     int     `json:"subjects"`
	ControlRate  float64 `json:"control_rate"`
	AbsoluteLift float64 `json:"absolute_lift"`
	DuplicatePct float64 `json:"duplicate_pct"`
	Seed         int64   `json:"seed"`
}

type SyntheticSummary struct {
	Subjects                int     `json:"subjects"`
	KnownControlRate        float64 `json:"known_control_rate"`
	KnownAbsoluteLift       float64 `json:"known_absolute_lift"`
	EstimatedAbsoluteLift   float64 `json:"estimated_absolute_lift"`
	AbsoluteEstimationError float64 `json:"absolute_estimation_error"`
	Deliveries              int     `json:"deliveries"`
	Batches                 int     `json:"batches"`
}

// SyntheticBatch is one consumer poll of the simulated stream: the events in
// delivery order and the time the poll returned them, on the stream's own
// clock. Pass ArrivedAt to Engine.ProcessBatchAt so event-time lag reflects
// the simulated delivery rather than how long ago the synthetic data is dated.
type SyntheticBatch struct {
	ArrivedAt time.Time
	Events    []Event
}

// The simulated consumer polls once per second of stream time and takes at
// most this many records per poll, like a consumer's poll loop with a
// max-records setting.
const (
	syntheticPollInterval = time.Second
	syntheticMaxBatch     = 500
)

// SyntheticBatches produces synthetic advertising observations only. Subject
// identifiers are generated opaque labels; no user data is read. Conversions
// are delayed, click exposures are mixed with impression exposures, and
// delivery is duplicated and out of order: every record reaches the consumer
// after a modeled transport delay (see transportDelay), duplicates are
// redelivered 1 to 20 seconds after the original, and a consumer polling once
// per second of stream time returns what has arrived, at most 500 records per
// poll. Every original record is polled within 54 seconds of its event time
// and every duplicate within 74 seconds, inside the default two-minute
// allowed lateness.
func SyntheticBatches(experiment Experiment, config SyntheticConfig) ([]SyntheticBatch, error) {
	if config.Subjects == 0 {
		config.Subjects = 5000
	}
	if config.ControlRate == 0 {
		config.ControlRate = 0.08
	}
	if config.AbsoluteLift == 0 {
		config.AbsoluteLift = 0.04
	}
	if config.DuplicatePct == 0 {
		config.DuplicatePct = 0.03
	}
	if config.Seed == 0 {
		config.Seed = 20261007
	}
	if config.Subjects < 100 || config.Subjects > 100000 {
		return nil, fmt.Errorf("subjects must be between 100 and 100000")
	}
	if config.ControlRate <= 0 || config.ControlRate >= 1 || config.ControlRate+config.AbsoluteLift <= 0 || config.ControlRate+config.AbsoluteLift >= 1 {
		return nil, fmt.Errorf("conversion rates must be between 0 and 1")
	}
	if config.DuplicatePct < 0 || config.DuplicatePct > 0.5 {
		return nil, fmt.Errorf("duplicate_pct must be between 0 and 0.5")
	}
	random := rand.New(rand.NewSource(config.Seed))
	start := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	events := make([]Event, 0, config.Subjects*3)
	for index := 0; index < config.Subjects; index++ {
		subject := fmt.Sprintf("synthetic-%08d", index)
		arm := assign(experiment, subject)
		// Millisecond precision keeps measured lags from snapping to whole
		// seconds of the one-second poll schedule.
		at := start.Add(time.Duration(index%1800)*time.Second + time.Duration(random.Intn(1000))*time.Millisecond)
		events = append(events,
			Event{EventID: fmt.Sprintf("a-%08d", index), ExperimentID: experiment.ID, Type: Assignment, SubjectID: subject, Arm: arm, Timestamp: at, ConfigVersion: experiment.ConfigVersion},
			Event{EventID: fmt.Sprintf("i-%08d", index), ExperimentID: experiment.ID, Type: Exposure, SubjectID: subject, Arm: arm, Timestamp: at.Add(time.Second), ConfigVersion: experiment.ConfigVersion},
		)
		// A second exposure represents a click for a realistic subset. It does
		// not increase sample size because attribution is per subject.
		if random.Float64() < 0.18 {
			events = append(events, Event{EventID: fmt.Sprintf("c-%08d", index), ExperimentID: experiment.ID, Type: Exposure, SubjectID: subject, Arm: arm, Timestamp: at.Add(time.Duration(2+random.Intn(90)) * time.Second), ConfigVersion: experiment.ConfigVersion})
		}
		rate := config.ControlRate
		if arm == experiment.TreatmentArm {
			rate += config.AbsoluteLift
		}
		if random.Float64() < rate {
			value := 25 + random.Float64()*175
			events = append(events, Event{EventID: fmt.Sprintf("o-%08d", index), ExperimentID: experiment.ID, Type: Outcome, SubjectID: subject, Arm: arm, Timestamp: at.Add(time.Duration(5+random.Intn(3*24*60*60)) * time.Second), OutcomeName: experiment.OutcomeName, Value: &value, ConfigVersion: experiment.ConfigVersion})
		}
	}
	base := len(events)
	var duplicateOf []int
	for index := 0; index < base; index++ {
		if random.Float64() < config.DuplicatePct {
			duplicateOf = append(duplicateOf, index)
		}
	}

	// Each of the 64 logical partitions runs a fixed amount behind the others,
	// which models partitions arriving at different speeds.
	partitionSkew := make([]time.Duration, 64)
	for index := range partitionSkew {
		partitionSkew[index] = time.Duration(random.Int63n(int64(2 * time.Second)))
	}
	arrivals := make([]time.Time, 0, base+len(duplicateOf))
	for _, event := range events {
		arrivals = append(arrivals, event.Timestamp.Add(transportDelay(random, partitionSkew[partition(event.SubjectID)])))
	}
	for _, index := range duplicateOf {
		events = append(events, events[index])
		arrivals = append(arrivals, arrivals[index].Add(time.Second+time.Duration(random.Int63n(int64(19*time.Second)))))
	}

	order := make([]int, len(events))
	for index := range order {
		order[index] = index
	}
	sort.SliceStable(order, func(i, j int) bool { return arrivals[order[i]].Before(arrivals[order[j]]) })
	var batches []SyntheticBatch
	for first := 0; first < len(order); {
		// The poll that returns a record is the first poll at or after its
		// arrival. A poll with more than syntheticMaxBatch records waiting is
		// followed immediately by another poll at the same stream time.
		poll := arrivals[order[first]].Truncate(syntheticPollInterval)
		if poll.Before(arrivals[order[first]]) {
			poll = poll.Add(syntheticPollInterval)
		}
		end := first
		for end < len(order) && end-first < syntheticMaxBatch && !arrivals[order[end]].After(poll) {
			end++
		}
		batch := SyntheticBatch{ArrivedAt: poll, Events: make([]Event, 0, end-first)}
		for _, index := range order[first:end] {
			batch.Events = append(batch.Events, events[index])
		}
		batches = append(batches, batch)
		first = end
	}
	return batches, nil
}

// transportDelay is the simulated time from an event occurring to it reaching
// the consumer: a 50 ms floor, the partition's skew (0 to 2 s), exponential
// jitter with a 250 ms mean capped at 5 s, and for 1% of records a straggler
// delay of 5 to 45 s, as a producer retry would add. The total stays below
// 53 s.
func transportDelay(random *rand.Rand, partitionSkew time.Duration) time.Duration {
	jitter := math.Min(random.ExpFloat64(), 20) * float64(250*time.Millisecond)
	delay := 50*time.Millisecond + partitionSkew + time.Duration(jitter)
	if random.Float64() < 0.01 {
		delay += 5*time.Second + time.Duration(random.Int63n(int64(40*time.Second)))
	}
	return delay
}
