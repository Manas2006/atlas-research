package tsdb

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type API struct{ Store *Store }

func (a *API) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, 200, map[string]any{"status": "ok", "series": a.Store.SeriesCount()})
	})
	mux.HandleFunc("POST /v1/write", a.write)
	mux.HandleFunc("GET /v1/query", a.query)
	mux.HandleFunc("POST /v1/admin/flush", func(w http.ResponseWriter, _ *http.Request) {
		if err := a.Store.Flush(); err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		writeJSON(w, 200, map[string]string{"status": "flushed"})
	})
	mux.HandleFunc("POST /v1/admin/retention", a.retention)
	return mux
}

func (a *API) write(w http.ResponseWriter, r *http.Request) {
	var batches []PointBatch
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 16<<20)).Decode(&batches); err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	for _, batch := range batches {
		if err := a.Store.Write(batch); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
	}
	writeJSON(w, http.StatusAccepted, map[string]int{"series": len(batches)})
}

func (a *API) query(w http.ResponseWriter, r *http.Request) {
	start, err := strconv.ParseInt(r.URL.Query().Get("start"), 10, 64)
	if err != nil {
		http.Error(w, "start milliseconds required", 400)
		return
	}
	end, _ := strconv.ParseInt(r.URL.Query().Get("end"), 10, 64)
	labels := make(map[string]string)
	for key, values := range r.URL.Query() {
		if strings.HasPrefix(key, "label.") && len(values) > 0 {
			labels[strings.TrimPrefix(key, "label.")] = values[0]
		}
	}
	series := Series{Name: r.URL.Query().Get("name"), Labels: labels}
	samples, err := a.Store.Query(series, start, end)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	writeJSON(w, 200, map[string]any{"series": series, "samples": samples})
}

func (a *API) retention(w http.ResponseWriter, r *http.Request) {
	hours, err := strconv.Atoi(r.URL.Query().Get("hours"))
	if err != nil || hours < 1 {
		http.Error(w, "positive hours required", 400)
		return
	}
	deleted, err := a.Store.DeleteExpired(time.Now().Add(-time.Duration(hours) * time.Hour))
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	writeJSON(w, 200, map[string]int{"deleted_blocks": deleted})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
