package kv

import (
	"sort"
	"strings"
	"sync"
	"time"
)

type Operation string

const (
	Put    Operation = "put"
	Delete Operation = "delete"
)

type Command struct {
	Operation Operation `json:"operation"`
	Key       string    `json:"key"`
	Value     string    `json:"value,omitempty"`
	ExpiresAt int64     `json:"expires_at,omitempty"`
}

type Value struct {
	Data      string `json:"data"`
	ExpiresAt int64  `json:"expires_at,omitempty"`
}

type Item struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type StateMachine struct {
	mu   sync.RWMutex
	data map[string]Value
}

func NewStateMachine() *StateMachine { return &StateMachine{data: make(map[string]Value)} }

func (s *StateMachine) Apply(command Command) {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch command.Operation {
	case Put:
		s.data[command.Key] = Value{Data: command.Value, ExpiresAt: command.ExpiresAt}
	case Delete:
		delete(s.data, command.Key)
	}
}

func (s *StateMachine) Get(key string, now time.Time) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	value, ok := s.data[key]
	if !ok || expired(value, now) {
		return "", false
	}
	return value.Data, true
}

func (s *StateMachine) Range(start, end, prefix string, limit int, now time.Time) []Item {
	s.mu.RLock()
	defer s.mu.RUnlock()
	keys := make([]string, 0, len(s.data))
	for key, value := range s.data {
		if expired(value, now) || (prefix != "" && !strings.HasPrefix(key, prefix)) || (start != "" && key < start) || (end != "" && key >= end) {
			continue
		}
		keys = append(keys, key)
	}
	sort.Strings(keys)
	if limit > 0 && len(keys) > limit {
		keys = keys[:limit]
	}
	items := make([]Item, 0, len(keys))
	for _, key := range keys {
		items = append(items, Item{Key: key, Value: s.data[key].Data})
	}
	return items
}

func expired(value Value, now time.Time) bool {
	return value.ExpiresAt != 0 && value.ExpiresAt <= now.UnixNano()
}

func (s *StateMachine) copyData() map[string]Value {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make(map[string]Value, len(s.data))
	for key, value := range s.data {
		result[key] = value
	}
	return result
}

func (s *StateMachine) replaceData(data map[string]Value) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data = data
}
