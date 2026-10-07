package atlas

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/Manas2006/atlas-research/internal/experiments"
	"github.com/Manas2006/atlas-research/internal/search"
	"github.com/Manas2006/atlas-research/internal/tsdb"
)

func (s *Server) impactExperiments(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet {
		items := s.impact.List()
		type summary struct {
			Experiment experiments.Experiment `json:"experiment"`
			Result     experiments.Result     `json:"result"`
		}
		results := make([]summary, 0, len(items))
		for _, item := range items {
			_, result, _ := s.impact.Get(item.ID)
			results = append(results, summary{Experiment: item, Result: result})
		}
		writeJSON(w, http.StatusOK, map[string]any{"impact_experiments": results})
		return
	}
	var config experiments.Experiment
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&config); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "valid JSON body required"})
		return
	}
	created, err := s.impact.Create(config)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	_, result, _ := s.impact.Get(created.ID)
	writeJSON(w, http.StatusCreated, map[string]any{"experiment": created, "result": result})
}

func (s *Server) impactExperiment(w http.ResponseWriter, r *http.Request) {
	experiment, result, ok := s.impact.Get(r.PathValue("id"))
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "impact experiment not found"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"experiment": experiment, "result": result})
}

func (s *Server) startImpactExperiment(w http.ResponseWriter, r *http.Request) {
	experiment, result, err := s.impact.SetStatus(r.PathValue("id"), experiments.Running)
	if err != nil {
		writeImpactError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"experiment": experiment, "result": result})
}

func (s *Server) stopImpactExperiment(w http.ResponseWriter, r *http.Request) {
	experiment, result, err := s.impact.SetStatus(r.PathValue("id"), experiments.Completed)
	if err != nil {
		writeImpactError(w, err)
		return
	}
	entry, err := s.indexImpactConclusion(experiment, result)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "experiment completed, but its conclusion could not be indexed: " + err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"experiment": experiment, "result": result, "knowledge_entry": entry})
}

type assignmentRequest struct {
	SubjectID string    `json:"subject_id"`
	EventID   string    `json:"event_id"`
	Timestamp time.Time `json:"event_timestamp"`
}

func (s *Server) assignImpactSubject(w http.ResponseWriter, r *http.Request) {
	var request assignmentRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&request); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "valid JSON body required"})
		return
	}
	experiment, _, ok := s.impact.Get(r.PathValue("id"))
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "impact experiment not found"})
		return
	}
	arm, partition, err := s.impact.Assign(experiment.ID, request.SubjectID)
	if err != nil {
		writeImpactError(w, err)
		return
	}
	if request.EventID == "" {
		writeJSON(w, http.StatusOK, map[string]any{"experiment_id": experiment.ID, "subject_id": request.SubjectID, "arm": arm, "config_version": experiment.ConfigVersion, "subject_partition": partition})
		return
	}
	if request.Timestamp.IsZero() {
		request.Timestamp = time.Now().UTC()
	}
	receipt, result, err := s.impact.Process(experiments.Event{EventID: request.EventID, ExperimentID: experiment.ID, Type: experiments.Assignment, SubjectID: request.SubjectID, Arm: arm, Timestamp: request.Timestamp, ConfigVersion: experiment.ConfigVersion})
	if err != nil {
		writeImpactError(w, err)
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"assignment": map[string]any{"experiment_id": experiment.ID, "subject_id": request.SubjectID, "arm": arm, "config_version": experiment.ConfigVersion, "subject_partition": partition}, "receipt": receipt, "result": result})
}

type eventBatch struct {
	Events []experiments.Event `json:"events"`
}

func (s *Server) ingestImpactEvents(w http.ResponseWriter, r *http.Request) {
	payload, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 16<<20))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	var batch eventBatch
	if err := json.Unmarshal(payload, &batch); err != nil || len(batch.Events) == 0 {
		var event experiments.Event
		if singleErr := json.Unmarshal(payload, &event); singleErr != nil || event.EventID == "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "an event or non-empty events array is required"})
			return
		}
		batch.Events = []experiments.Event{event}
	}
	for index := range batch.Events {
		if batch.Events[index].ExperimentID == "" {
			batch.Events[index].ExperimentID = r.PathValue("id")
		}
		if batch.Events[index].ExperimentID != r.PathValue("id") {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "event experiment_id must match the URL"})
			return
		}
	}
	receipts, result, err := s.impact.ProcessBatch(batch.Events)
	if err != nil {
		writeImpactError(w, err)
		return
	}
	s.recordImpactMetrics(result)
	writeJSON(w, http.StatusAccepted, map[string]any{"receipts": receipts, "result": result})
}

func (s *Server) runImpactDemo(w http.ResponseWriter, r *http.Request) {
	experiment, _, ok := s.impact.Get(r.PathValue("id"))
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "impact experiment not found"})
		return
	}
	var config experiments.SyntheticConfig
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&config); err != nil && !errors.Is(err, io.EOF) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "valid JSON body required"})
		return
	}
	events, err := experiments.SyntheticEvents(experiment, config)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	var result experiments.Result
	for start := 0; start < len(events); start += 500 {
		end := start + 500
		if end > len(events) {
			end = len(events)
		}
		_, result, err = s.impact.ProcessBatch(events[start:end])
		if err != nil {
			writeImpactError(w, err)
			return
		}
		s.recordImpactMetrics(result)
	}
	known := config.AbsoluteLift
	if known == 0 {
		known = 0.04
	}
	summary := experiments.SyntheticSummary{Subjects: int(result.TreatmentSampleSize + result.ControlSampleSize), KnownControlRate: config.ControlRate, KnownAbsoluteLift: known, EstimatedAbsoluteLift: result.AbsoluteLift, AbsoluteEstimationError: math.Abs(result.AbsoluteLift - known), Deliveries: len(events)}
	if summary.KnownControlRate == 0 {
		summary.KnownControlRate = 0.08
	}
	writeJSON(w, http.StatusOK, map[string]any{"synthetic": true, "summary": summary, "result": result})
}

func (s *Server) recordImpactMetrics(result experiments.Result) {
	timestamp := time.Now().UnixMilli()
	labels := map[string]string{"experiment": result.ExperimentID}
	for name, value := range map[string]float64{
		"impact_absolute_lift":             result.AbsoluteLift,
		"impact_treatment_conversion_rate": result.TreatmentConversionRate,
		"impact_control_conversion_rate":   result.ControlConversionRate,
		"impact_processing_lag_p95_ms":     result.Diagnostics.ProcessingLagP95,
	} {
		_ = s.metrics.Write(tsdb.PointBatch{Series: tsdb.Series{Name: name, Labels: labels}, Samples: []tsdb.Sample{{Timestamp: timestamp, Value: value}}})
	}
}

func (s *Server) indexImpactConclusion(experiment experiments.Experiment, result experiments.Result) (Entry, error) {
	relative := "undefined because the control rate was zero"
	if result.RelativeLift != nil {
		relative = fmt.Sprintf("%.2f%%", *result.RelativeLift*100)
	}
	body := fmt.Sprintf("Hypothesis: %s\nOutcome: %s\nTreatment: %d subjects, %.2f%% conversion. Control: %d subjects, %.2f%% conversion. Absolute lift: %.2f percentage points (95%% CI %.2f to %.2f); relative lift: %s. Decision: %s.", experiment.Hypothesis, experiment.OutcomeName, result.TreatmentSampleSize, result.TreatmentConversionRate*100, result.ControlSampleSize, result.ControlConversionRate*100, result.AbsoluteLift*100, result.ConfidenceLow*100, result.ConfidenceHigh*100, relative, result.Recommendation)
	if experiment.AnalysisDocID != "" {
		body += " Analysis live doc: #docs/" + experiment.AnalysisDocID + "."
	}
	entry, err := s.catalog.SaveEntry(Entry{ID: "impact-" + experiment.ID, Title: "Impact conclusion: " + experiment.Name, Body: body, Type: "Run log", Tags: []string{"impact-experiment", "causal-lift", strings.ToLower(experiment.OutcomeName)}})
	if err != nil {
		return Entry{}, err
	}
	document := search.Document{ID: entry.ID, Title: entry.Title, Body: entry.Body + " " + strings.Join(entry.Tags, " ")}
	if err := s.wal.Append(document); err != nil {
		return Entry{}, err
	}
	s.index.Upsert(document)
	return entry, nil
}

func writeImpactError(w http.ResponseWriter, err error) {
	status := http.StatusBadRequest
	if errors.Is(err, os.ErrNotExist) {
		status = http.StatusNotFound
	}
	writeJSON(w, status, map[string]string{"error": err.Error()})
}
