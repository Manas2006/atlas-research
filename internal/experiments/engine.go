// Package experiments implements Atlas impact experiments: deterministic
// treatment assignment, event-time attribution, durable replay, and simple
// treatment-versus-control statistics.
package experiments

import (
	"bufio"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

type Status string

const (
	Draft     Status = "draft"
	Running   Status = "running"
	Stopped   Status = "stopped"
	Completed Status = "completed"
)

type Experiment struct {
	ID                       string    `json:"id"`
	Name                     string    `json:"name"`
	Hypothesis               string    `json:"hypothesis"`
	Status                   Status    `json:"status"`
	TreatmentAllocation      float64   `json:"treatment_allocation"`
	TreatmentArm             string    `json:"treatment_arm"`
	ControlArm               string    `json:"control_arm"`
	OutcomeName              string    `json:"outcome_name"`
	ObservationWindowSeconds int64     `json:"observation_window_seconds"`
	AllowedLatenessSeconds   int64     `json:"allowed_lateness_seconds"`
	ConfigVersion            int       `json:"config_version"`
	AnalysisDocID            string    `json:"analysis_doc_id,omitempty"`
	StartTime                time.Time `json:"start_time,omitempty"`
	EndTime                  time.Time `json:"end_time,omitempty"`
	CreatedAt                time.Time `json:"created_at"`
	UpdatedAt                time.Time `json:"updated_at"`
}

type EventType string

const (
	Assignment EventType = "assignment"
	Exposure   EventType = "exposure"
	Outcome    EventType = "outcome"
)

type Event struct {
	EventID       string    `json:"event_id"`
	ExperimentID  string    `json:"experiment_id"`
	Type          EventType `json:"type"`
	SubjectID     string    `json:"subject_id"`
	Arm           string    `json:"arm,omitempty"`
	Timestamp     time.Time `json:"event_timestamp"`
	OutcomeName   string    `json:"outcome_name,omitempty"`
	Value         *float64  `json:"value,omitempty"`
	ConfigVersion int       `json:"config_version"`
}

// Diagnostics mixes two clocks on purpose; docs/components/impact-experiments.md
// defines every field. Event-time lags (processing_lag_*, watermark_lag_ms) are
// measured on the stream's arrival clock, which is the wall clock for live
// ingestion and the caller's arrival time for backfills or simulations, and
// are rebuilt exactly by WAL replay. Ack latencies are wall-clock durations
// inside this process and are never produced by replay.
type Diagnostics struct {
	Received             int64   `json:"received"`
	Accepted             int64   `json:"accepted"`
	Duplicates           int64   `json:"duplicates"`
	Rejected             int64   `json:"rejected"`
	Late                 int64   `json:"late"`
	UnmatchedOutcomes    int64   `json:"unmatched_outcomes"`
	ProcessingLagP50     float64 `json:"processing_lag_p50_ms"`
	ProcessingLagP95     float64 `json:"processing_lag_p95_ms"`
	ProcessingLagP99     float64 `json:"processing_lag_p99_ms"`
	ProcessingLagSamples int     `json:"processing_lag_samples"`
	WatermarkLag         float64 `json:"watermark_lag_ms"`
	AckLatencyP50        float64 `json:"ack_latency_p50_ms"`
	AckLatencyP95        float64 `json:"ack_latency_p95_ms"`
	AckLatencyP99        float64 `json:"ack_latency_p99_ms"`
	AckLatencySamples    int     `json:"ack_latency_samples"`
	StateSubjects        int     `json:"state_subjects"`
	StateBytesEstimate   int64   `json:"state_bytes_estimate"`
	EventsPerSecond      float64 `json:"events_per_second"`
	RecoveryTime         float64 `json:"recovery_time_ms"`
}

type Result struct {
	ExperimentID            string      `json:"experiment_id"`
	TreatmentSampleSize     int64       `json:"treatment_sample_size"`
	ControlSampleSize       int64       `json:"control_sample_size"`
	TreatmentConversions    int64       `json:"treatment_conversions"`
	ControlConversions      int64       `json:"control_conversions"`
	TreatmentConversionRate float64     `json:"treatment_conversion_rate"`
	ControlConversionRate   float64     `json:"control_conversion_rate"`
	AbsoluteLift            float64     `json:"absolute_lift"`
	RelativeLift            *float64    `json:"relative_lift,omitempty"`
	IncrementalConversions  float64     `json:"incremental_conversions"`
	IncrementalOutcomeValue float64     `json:"incremental_outcome_value"`
	ConfidenceLow           float64     `json:"confidence_low"`
	ConfidenceHigh          float64     `json:"confidence_high"`
	InsufficientData        bool        `json:"insufficient_data"`
	Recommendation          string      `json:"recommendation"`
	Diagnostics             Diagnostics `json:"diagnostics"`
	History                 []Snapshot  `json:"history"`
	UpdatedAt               time.Time   `json:"updated_at"`
}

type Snapshot struct {
	EventTime      time.Time `json:"event_time"`
	TreatmentN     int64     `json:"treatment_n"`
	ControlN       int64     `json:"control_n"`
	AbsoluteLift   float64   `json:"absolute_lift"`
	ConfidenceLow  float64   `json:"confidence_low"`
	ConfidenceHigh float64   `json:"confidence_high"`
}

type Receipt struct {
	EventID   string `json:"event_id"`
	Status    string `json:"status"`
	Arm       string `json:"arm,omitempty"`
	Partition uint32 `json:"subject_partition"`
}

type contribution struct {
	arm       string
	exposed   bool
	converted bool
	value     float64
}

type outcomeEvent struct {
	at    time.Time
	value float64
}

type subjectState struct {
	arm       string
	exposures []time.Time
	outcomes  []outcomeEvent
	latest    time.Time
	current   contribution
}

type aggregate struct {
	treatmentN, controlN         int64
	treatmentY, controlY         int64
	treatmentValue, controlValue float64
}

type experimentState struct {
	config       Experiment
	subjects     map[string]*subjectState
	seen         map[string]struct{}
	aggregate    aggregate
	diagnostics  Diagnostics
	maxEventTime time.Time
	// latestArrival is the newest arrival time on the stream's clock; the
	// watermark lag is measured against it rather than against query time.
	latestArrival time.Time
	lags          sampleWindow
	ackLatencies  sampleWindow
	history       []Snapshot
}

// latencyWindow bounds memory for percentile diagnostics: each distribution
// covers the most recent latencyWindow samples.
const latencyWindow = 2048

// sampleWindow is a fixed-size ring of the most recent millisecond samples.
type sampleWindow struct {
	values []float64
	next   int
}

func (w *sampleWindow) add(value float64) {
	if len(w.values) < latencyWindow {
		w.values = append(w.values, value)
		return
	}
	w.values[w.next] = value
	w.next = (w.next + 1) % latencyWindow
}

// percentiles returns nearest-rank p50, p95, and p99 of the window.
func (w *sampleWindow) percentiles() (p50, p95, p99 float64) {
	if len(w.values) == 0 {
		return 0, 0, 0
	}
	sorted := append([]float64(nil), w.values...)
	sort.Float64s(sorted)
	rank := func(p float64) float64 {
		index := int(math.Ceil(p/100*float64(len(sorted)))) - 1
		if index < 0 {
			index = 0
		}
		return sorted[index]
	}
	return rank(50), rank(95), rank(99)
}

type walRecord struct {
	Kind       string      `json:"kind"`
	Experiment *Experiment `json:"experiment,omitempty"`
	Event      *Event      `json:"event,omitempty"`
	Events     []Event     `json:"events,omitempty"`
	// ArrivedAt is when the Events batch arrived, on the stream's clock.
	// Replay uses it to rebuild event-time lag exactly as measured; records
	// written before it existed contribute no lag samples.
	ArrivedAt *time.Time `json:"arrived_at,omitempty"`
}

type Engine struct {
	mu     sync.RWMutex
	path   string
	wal    *os.File
	states map[string]*experimentState
	// now is the wall clock for configuration timestamps, throughput, and the
	// arrival time of live deliveries (ProcessBatch).
	now func() time.Time
	// latencyClock times acknowledgements. It must keep Go's monotonic clock
	// reading, so it is time.Now and never time.Now().UTC().
	latencyClock func() time.Time
}

func Open(path string) (*Engine, error) {
	opened := time.Now()
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	e := &Engine{path: path, wal: file, states: make(map[string]*experimentState), now: func() time.Time { return time.Now().UTC() }, latencyClock: time.Now}
	if err := e.replay(); err != nil {
		file.Close()
		return nil, err
	}
	recoveryMS := time.Since(opened).Seconds() * 1000
	for _, state := range e.states {
		state.diagnostics.RecoveryTime = recoveryMS
	}
	return e, nil
}

func (e *Engine) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.wal.Close()
}

func (e *Engine) Count() int {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return len(e.states)
}

func (e *Engine) Create(config Experiment) (Experiment, error) {
	config.Name = strings.TrimSpace(config.Name)
	config.Hypothesis = strings.TrimSpace(config.Hypothesis)
	config.OutcomeName = strings.TrimSpace(config.OutcomeName)
	config.AnalysisDocID = strings.TrimSpace(config.AnalysisDocID)
	if config.Name == "" || config.Hypothesis == "" || config.OutcomeName == "" {
		return Experiment{}, errors.New("name, hypothesis, and outcome_name are required")
	}
	if config.TreatmentAllocation == 0 {
		config.TreatmentAllocation = 0.5
	}
	if config.TreatmentAllocation <= 0 || config.TreatmentAllocation >= 1 {
		return Experiment{}, errors.New("treatment_allocation must be between 0 and 1")
	}
	if config.ObservationWindowSeconds == 0 {
		config.ObservationWindowSeconds = int64((7 * 24 * time.Hour).Seconds())
	}
	if config.ObservationWindowSeconds < 1 {
		return Experiment{}, errors.New("observation_window_seconds must be positive")
	}
	if config.AllowedLatenessSeconds == 0 {
		config.AllowedLatenessSeconds = int64((2 * time.Minute).Seconds())
	}
	if config.AllowedLatenessSeconds < 0 {
		return Experiment{}, errors.New("allowed_lateness_seconds cannot be negative")
	}
	if config.TreatmentArm == "" {
		config.TreatmentArm = "treatment"
	}
	if config.ControlArm == "" {
		config.ControlArm = "control"
	}
	if config.TreatmentArm == config.ControlArm {
		return Experiment{}, errors.New("treatment_arm and control_arm must differ")
	}
	if config.ID == "" {
		config.ID = randomID()
	}
	if config.ConfigVersion == 0 {
		config.ConfigVersion = 1
	}
	config.Status = Draft
	config.CreatedAt = e.now()
	config.UpdatedAt = config.CreatedAt

	e.mu.Lock()
	defer e.mu.Unlock()
	if _, exists := e.states[config.ID]; exists {
		return Experiment{}, errors.New("experiment id already exists")
	}
	if err := e.appendLocked(walRecord{Kind: "experiment", Experiment: &config}); err != nil {
		return Experiment{}, err
	}
	e.applyExperiment(config)
	return config, nil
}

func (e *Engine) List() []Experiment {
	e.mu.RLock()
	defer e.mu.RUnlock()
	items := make([]Experiment, 0, len(e.states))
	for _, state := range e.states {
		items = append(items, state.config)
	}
	sort.Slice(items, func(i, j int) bool { return items[i].UpdatedAt.After(items[j].UpdatedAt) })
	return items
}

func (e *Engine) Get(id string) (Experiment, Result, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	state, ok := e.states[id]
	if !ok {
		return Experiment{}, Result{}, false
	}
	return state.config, resultFor(state, e.now()), true
}

func (e *Engine) SetStatus(id string, status Status) (Experiment, Result, error) {
	if status != Running && status != Stopped && status != Completed {
		return Experiment{}, Result{}, errors.New("status must be running, stopped, or completed")
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	state, ok := e.states[id]
	if !ok {
		return Experiment{}, Result{}, os.ErrNotExist
	}
	config := state.config
	if status == Running && config.Status != Draft && config.Status != Stopped {
		return Experiment{}, Result{}, fmt.Errorf("cannot start an experiment in %s status", config.Status)
	}
	if (status == Stopped || status == Completed) && config.Status != Running {
		return Experiment{}, Result{}, fmt.Errorf("cannot stop an experiment in %s status", config.Status)
	}
	config.Status = status
	config.UpdatedAt = e.now()
	if status == Running && config.StartTime.IsZero() {
		config.StartTime = config.UpdatedAt
	}
	if status == Stopped || status == Completed {
		config.EndTime = config.UpdatedAt
	}
	if err := e.appendLocked(walRecord{Kind: "experiment", Experiment: &config}); err != nil {
		return Experiment{}, Result{}, err
	}
	e.applyExperiment(config)
	return config, resultFor(state, e.now()), nil
}

func (e *Engine) Assign(experimentID, subjectID string) (string, uint32, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	state, ok := e.states[experimentID]
	if !ok {
		return "", 0, os.ErrNotExist
	}
	if strings.TrimSpace(subjectID) == "" {
		return "", 0, errors.New("subject_id is required")
	}
	return assign(state.config, subjectID), partition(subjectID), nil
}

// Process durably records an event before changing in-memory state. A crash
// after fsync and before the caller receives a response is safe to retry with
// the same event ID; replay/deduplication applies it once.
func (e *Engine) Process(event Event) (Receipt, Result, error) {
	receipts, result, err := e.ProcessBatch([]Event{event})
	if err != nil {
		return Receipt{}, Result{}, err
	}
	return receipts[0], result, nil
}

// ProcessBatch durably records one delivery batch and then applies it. This
// mirrors a stream consumer's poll transaction: callers commit their source
// offsets only after this method returns. Retrying an uncertain batch is safe.
// The batch arrives now on the wall clock, which is right for live ingestion.
func (e *Engine) ProcessBatch(events []Event) ([]Receipt, Result, error) {
	return e.ProcessBatchAt(events, time.Time{})
}

// ProcessBatchAt is ProcessBatch for a delivery whose arrival time comes from
// the stream's own clock: when the consumer received the batch during a
// backfill, or the poll time of a simulated delivery schedule. Event-time lag
// is measured against arrivedAt, so replaying old data does not report the
// data's age as lag. A zero arrivedAt means the wall clock now.
func (e *Engine) ProcessBatchAt(events []Event, arrivedAt time.Time) ([]Receipt, Result, error) {
	// Ack latency starts before the lock so it includes queueing behind other
	// batches, which a concurrent caller also waits for.
	started := e.latencyClock()
	if len(events) == 0 {
		return nil, Result{}, errors.New("at least one event is required")
	}
	if arrivedAt.IsZero() {
		arrivedAt = e.now()
	}
	arrivedAt = arrivedAt.UTC()
	e.mu.Lock()
	defer e.mu.Unlock()
	state, ok := e.states[events[0].ExperimentID]
	if !ok {
		return nil, Result{}, os.ErrNotExist
	}
	for _, event := range events {
		if event.ExperimentID != state.config.ID {
			return nil, Result{}, errors.New("a batch cannot mix experiments")
		}
		if err := validateEvent(event); err != nil {
			return nil, Result{}, err
		}
	}
	if state.config.Status != Running {
		return nil, Result{}, fmt.Errorf("experiment is %s", state.config.Status)
	}
	if err := e.appendLocked(walRecord{Kind: "events", Events: events, ArrivedAt: &arrivedAt}); err != nil {
		return nil, Result{}, err
	}
	receipts := make([]Receipt, 0, len(events))
	for _, event := range events {
		receipts = append(receipts, e.applyEvent(state, event, arrivedAt))
	}
	// The batch is now fsynced and applied to the results readers see, which
	// is the commit point the API acknowledges. Building the returned
	// snapshot and encoding the HTTP response are not included.
	state.ackLatencies.add(float64(e.latencyClock().Sub(started)) / float64(time.Millisecond))
	return receipts, resultFor(state, e.now()), nil
}

func validateEvent(event Event) error {
	if strings.TrimSpace(event.EventID) == "" || strings.TrimSpace(event.ExperimentID) == "" || strings.TrimSpace(event.SubjectID) == "" {
		return errors.New("event_id, experiment_id, and subject_id are required")
	}
	if event.Timestamp.IsZero() {
		return errors.New("event_timestamp is required")
	}
	if event.Type != Assignment && event.Type != Exposure && event.Type != Outcome {
		return errors.New("type must be assignment, exposure, or outcome")
	}
	return nil
}

// applyEvent applies one event that arrived at arrivedAt on the stream's clock.
// Live processing and replay share it, so both derive identical event-time
// diagnostics from the same durable inputs. A zero arrivedAt (a WAL record
// written before arrival times were persisted) records no lag sample.
func (e *Engine) applyEvent(state *experimentState, event Event, arrivedAt time.Time) Receipt {
	if arrivedAt.After(state.latestArrival) {
		state.latestArrival = arrivedAt
	}
	receipt := Receipt{EventID: event.EventID, Partition: partition(event.SubjectID)}
	state.diagnostics.Received++
	if _, duplicate := state.seen[event.EventID]; duplicate {
		state.diagnostics.Duplicates++
		receipt.Status = "duplicate"
		return receipt
	}
	state.seen[event.EventID] = struct{}{}
	expectedArm := assign(state.config, event.SubjectID)
	receipt.Arm = expectedArm
	if event.ConfigVersion != state.config.ConfigVersion || (event.Arm != "" && event.Arm != expectedArm) || (event.Type == Outcome && event.OutcomeName != state.config.OutcomeName) {
		state.diagnostics.Rejected++
		receipt.Status = "rejected"
		return receipt
	}
	watermark := state.maxEventTime.Add(-time.Duration(state.config.AllowedLatenessSeconds) * time.Second)
	if !state.maxEventTime.IsZero() && event.Timestamp.Before(watermark) {
		state.diagnostics.Late++
		receipt.Status = "late"
		return receipt
	}
	if event.Timestamp.After(state.maxEventTime) {
		state.maxEventTime = event.Timestamp
	}
	if !arrivedAt.IsZero() {
		// Event-time lag on the stream's clock. A timestamp ahead of its
		// arrival (producer clock skew) counts as zero lag.
		lag := float64(arrivedAt.Sub(event.Timestamp)) / float64(time.Millisecond)
		if lag < 0 {
			lag = 0
		}
		state.lags.add(lag)
	}
	subject := state.subjects[event.SubjectID]
	if subject == nil {
		subject = &subjectState{arm: expectedArm}
		state.subjects[event.SubjectID] = subject
	}
	if event.Timestamp.After(subject.latest) {
		subject.latest = event.Timestamp
	}
	switch event.Type {
	case Exposure:
		subject.exposures = append(subject.exposures, event.Timestamp)
	case Outcome:
		value := 1.0
		if event.Value != nil {
			value = *event.Value
		}
		subject.outcomes = append(subject.outcomes, outcomeEvent{at: event.Timestamp, value: value})
	}
	state.aggregate = removeContribution(state.aggregate, subject.current, state.config.TreatmentArm)
	subject.current = calculateContribution(subject, time.Duration(state.config.ObservationWindowSeconds)*time.Second)
	state.aggregate = addContribution(state.aggregate, subject.current, state.config.TreatmentArm)
	state.diagnostics.Accepted++
	receipt.Status = "accepted"
	if state.diagnostics.Accepted%128 == 0 {
		state.history = append(state.history, snapshotFor(state))
		if len(state.history) > 256 {
			state.history = append([]Snapshot(nil), state.history[len(state.history)-256:]...)
		}
	}
	// Expiration is amortized; walking all active subjects for every event
	// would make high-cardinality streams quadratic.
	if state.diagnostics.Accepted%256 == 0 {
		e.expireSubjects(state)
	}
	return receipt
}

func calculateContribution(subject *subjectState, window time.Duration) contribution {
	c := contribution{arm: subject.arm}
	if len(subject.exposures) == 0 {
		return c
	}
	sort.Slice(subject.exposures, func(i, j int) bool { return subject.exposures[i].Before(subject.exposures[j]) })
	exposure := subject.exposures[0]
	c.exposed = true
	for _, outcome := range subject.outcomes {
		if !outcome.at.Before(exposure) && !outcome.at.After(exposure.Add(window)) {
			c.converted = true
			c.value += outcome.value
		}
	}
	return c
}

func addContribution(a aggregate, c contribution, treatmentArm string) aggregate {
	if !c.exposed {
		return a
	}
	if c.arm == "" {
		return a
	}
	if c.arm == treatmentArm {
		a.treatmentN++
		if c.converted {
			a.treatmentY++
		}
		a.treatmentValue += c.value
	} else {
		a.controlN++
		if c.converted {
			a.controlY++
		}
		a.controlValue += c.value
	}
	return a
}

func removeContribution(a aggregate, c contribution, treatmentArm string) aggregate {
	if !c.exposed {
		return a
	}
	if c.arm == treatmentArm {
		a.treatmentN--
		if c.converted {
			a.treatmentY--
		}
		a.treatmentValue -= c.value
	} else {
		a.controlN--
		if c.converted {
			a.controlY--
		}
		a.controlValue -= c.value
	}
	return a
}

func (e *Engine) expireSubjects(state *experimentState) {
	watermark := state.maxEventTime.Add(-time.Duration(state.config.AllowedLatenessSeconds) * time.Second)
	window := time.Duration(state.config.ObservationWindowSeconds) * time.Second
	for id, subject := range state.subjects {
		deadline := subject.latest.Add(window)
		if len(subject.exposures) > 0 {
			deadline = subject.exposures[0].Add(window)
		}
		if watermark.After(deadline) {
			if len(subject.outcomes) > 0 && len(subject.exposures) == 0 {
				state.diagnostics.UnmatchedOutcomes += int64(len(subject.outcomes))
			}
			delete(state.subjects, id)
		}
	}
}

func resultFor(state *experimentState, now time.Time) Result {
	a := state.aggregate
	result := Result{ExperimentID: state.config.ID, TreatmentSampleSize: a.treatmentN, ControlSampleSize: a.controlN, TreatmentConversions: a.treatmentY, ControlConversions: a.controlY, UpdatedAt: now}
	if a.treatmentN > 0 {
		result.TreatmentConversionRate = float64(a.treatmentY) / float64(a.treatmentN)
	}
	if a.controlN > 0 {
		result.ControlConversionRate = float64(a.controlY) / float64(a.controlN)
	}
	result.AbsoluteLift = result.TreatmentConversionRate - result.ControlConversionRate
	if result.ControlConversionRate > 0 {
		relative := result.AbsoluteLift / result.ControlConversionRate
		result.RelativeLift = &relative
	}
	result.IncrementalConversions = result.AbsoluteLift * float64(a.treatmentN)
	var treatmentMean, controlMean float64
	if a.treatmentN > 0 {
		treatmentMean = a.treatmentValue / float64(a.treatmentN)
	}
	if a.controlN > 0 {
		controlMean = a.controlValue / float64(a.controlN)
	}
	result.IncrementalOutcomeValue = (treatmentMean - controlMean) * float64(a.treatmentN)
	standardError := 0.0
	if a.treatmentN > 0 && a.controlN > 0 {
		standardError = math.Sqrt(result.TreatmentConversionRate*(1-result.TreatmentConversionRate)/float64(a.treatmentN) + result.ControlConversionRate*(1-result.ControlConversionRate)/float64(a.controlN))
	}
	result.ConfidenceLow = result.AbsoluteLift - 1.96*standardError
	result.ConfidenceHigh = result.AbsoluteLift + 1.96*standardError
	result.InsufficientData = a.treatmentN < 30 || a.controlN < 30 || a.treatmentY < 5 || a.controlY < 5 || a.treatmentN-a.treatmentY < 5 || a.controlN-a.controlY < 5
	switch {
	case result.InsufficientData:
		result.Recommendation = "continue collecting data"
	case result.ConfidenceLow > 0:
		result.Recommendation = "launch"
	case result.ConfidenceHigh < 0:
		result.Recommendation = "stop"
	default:
		result.Recommendation = "continue collecting data"
	}
	result.Diagnostics = state.diagnostics
	result.History = append([]Snapshot(nil), state.history...)
	for _, subject := range state.subjects {
		if len(subject.outcomes) > 0 && len(subject.exposures) == 0 {
			result.Diagnostics.UnmatchedOutcomes += int64(len(subject.outcomes))
		}
	}
	result.Diagnostics.StateSubjects = len(state.subjects)
	result.Diagnostics.StateBytesEstimate = int64(len(state.seen) * 32)
	for id, subject := range state.subjects {
		result.Diagnostics.StateBytesEstimate += int64(len(id) + 96 + len(subject.exposures)*24 + len(subject.outcomes)*32)
	}
	if !state.config.StartTime.IsZero() {
		seconds := now.Sub(state.config.StartTime).Seconds()
		if seconds > 0 {
			result.Diagnostics.EventsPerSecond = float64(state.diagnostics.Received) / seconds
		}
	}
	result.Diagnostics.ProcessingLagP50, result.Diagnostics.ProcessingLagP95, result.Diagnostics.ProcessingLagP99 = state.lags.percentiles()
	result.Diagnostics.ProcessingLagSamples = len(state.lags.values)
	result.Diagnostics.AckLatencyP50, result.Diagnostics.AckLatencyP95, result.Diagnostics.AckLatencyP99 = state.ackLatencies.percentiles()
	result.Diagnostics.AckLatencySamples = len(state.ackLatencies.values)
	if !state.maxEventTime.IsZero() && !state.latestArrival.IsZero() {
		// Measured at the latest delivery, not at query time, so an idle stream
		// or a later read does not inflate it and replay reproduces it.
		watermark := state.maxEventTime.Add(-time.Duration(state.config.AllowedLatenessSeconds) * time.Second)
		result.Diagnostics.WatermarkLag = math.Max(0, float64(state.latestArrival.Sub(watermark))/float64(time.Millisecond))
	}
	return result
}

func snapshotFor(state *experimentState) Snapshot {
	a := state.aggregate
	var treatmentRate, controlRate float64
	if a.treatmentN > 0 {
		treatmentRate = float64(a.treatmentY) / float64(a.treatmentN)
	}
	if a.controlN > 0 {
		controlRate = float64(a.controlY) / float64(a.controlN)
	}
	lift := treatmentRate - controlRate
	var standardError float64
	if a.treatmentN > 0 && a.controlN > 0 {
		standardError = math.Sqrt(treatmentRate*(1-treatmentRate)/float64(a.treatmentN) + controlRate*(1-controlRate)/float64(a.controlN))
	}
	return Snapshot{EventTime: state.maxEventTime, TreatmentN: a.treatmentN, ControlN: a.controlN, AbsoluteLift: lift, ConfidenceLow: lift - 1.96*standardError, ConfidenceHigh: lift + 1.96*standardError}
}

func assign(config Experiment, subjectID string) string {
	digest := sha256.Sum256([]byte(fmt.Sprintf("%s\x00%d\x00%s", config.ID, config.ConfigVersion, subjectID)))
	value := binary.BigEndian.Uint64(digest[:8])
	threshold := uint64(config.TreatmentAllocation * float64(^uint64(0)))
	if value <= threshold {
		return config.TreatmentArm
	}
	return config.ControlArm
}

func partition(subjectID string) uint32 {
	hash := fnv.New32a()
	_, _ = hash.Write([]byte(subjectID))
	return hash.Sum32() % 64
}

func (e *Engine) appendLocked(record walRecord) error {
	payload, err := json.Marshal(record)
	if err != nil {
		return err
	}
	if _, err := e.wal.Write(append(payload, '\n')); err != nil {
		return err
	}
	return e.wal.Sync()
}

func (e *Engine) replay() error {
	if _, err := e.wal.Seek(0, 0); err != nil {
		return err
	}
	reader := bufio.NewReader(e.wal)
	var validBytes int64
	for {
		line, err := reader.ReadBytes('\n')
		if errors.Is(err, io.EOF) {
			// A crash before fsync may leave a partial, unacknowledged tail.
			// Ignore it; every acknowledged record ends with a newline and was
			// synced before the response was sent.
			break
		}
		if err != nil {
			return err
		}
		validBytes += int64(len(line))
		var record walRecord
		if err := json.Unmarshal(line, &record); err != nil {
			return fmt.Errorf("replay impact WAL: %w", err)
		}
		switch {
		case record.Kind == "experiment" && record.Experiment != nil:
			e.applyExperiment(*record.Experiment)
		case record.Kind == "event" && record.Event != nil:
			state := e.states[record.Event.ExperimentID]
			if state == nil {
				return errors.New("impact WAL event precedes experiment")
			}
			// Legacy single-event records have no arrival time, so they add no
			// lag sample rather than one measured against replay time.
			e.applyEvent(state, *record.Event, time.Time{})
		case record.Kind == "events" && len(record.Events) > 0:
			// Replay rebuilds event-time lag from the persisted arrival time and
			// never records ack latency, so neither depends on replay speed.
			var arrivedAt time.Time
			if record.ArrivedAt != nil {
				arrivedAt = *record.ArrivedAt
			}
			for _, event := range record.Events {
				state := e.states[event.ExperimentID]
				if state == nil {
					return errors.New("impact WAL event precedes experiment")
				}
				e.applyEvent(state, event, arrivedAt)
			}
		default:
			return errors.New("invalid impact WAL record")
		}
	}
	if err := e.wal.Truncate(validBytes); err != nil {
		return err
	}
	_, err := e.wal.Seek(0, 2)
	return err
}

func (e *Engine) applyExperiment(config Experiment) {
	state := e.states[config.ID]
	if state == nil {
		state = &experimentState{subjects: make(map[string]*subjectState), seen: make(map[string]struct{})}
		e.states[config.ID] = state
	}
	state.config = config
}

func randomID() string {
	value := make([]byte, 12)
	if _, err := rand.Read(value); err != nil {
		return fmt.Sprintf("impact-%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(value)
}
