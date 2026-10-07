package experiments

import (
	"fmt"
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
}

// SyntheticEvents produces synthetic advertising observations only. Subject
// identifiers are generated opaque labels; no user data is read. Delivery is
// deliberately shuffled and duplicated, conversions are delayed, and click
// exposures are mixed with impression exposures.
func SyntheticEvents(experiment Experiment, config SyntheticConfig) ([]Event, error) {
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
		at := start.Add(time.Duration(index%1800) * time.Second)
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
	for index := 0; index < base; index++ {
		if random.Float64() < config.DuplicatePct {
			events = append(events, events[index])
		}
	}
	// Mostly event-time order with bounded local disorder models partitions
	// arriving at different speeds without turning every early record late.
	sort.SliceStable(events, func(i, j int) bool { return events[i].Timestamp.Before(events[j].Timestamp) })
	for start := 0; start < len(events); {
		end := start + 1
		for end < len(events) && end-start < 40 && events[end].Timestamp.Sub(events[start].Timestamp) <= 30*time.Second {
			end++
		}
		random.Shuffle(end-start, func(i, j int) { events[start+i], events[start+j] = events[start+j], events[start+i] })
		start = end
	}
	return events, nil
}
