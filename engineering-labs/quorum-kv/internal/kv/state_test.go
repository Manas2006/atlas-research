package kv

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestStateMachineTTLAndRange(t *testing.T) {
	machine := NewStateMachine()
	now := time.Unix(100, 0)
	machine.Apply(Command{Operation: Put, Key: "users/a", Value: "A"})
	machine.Apply(Command{Operation: Put, Key: "users/b", Value: "B", ExpiresAt: now.Add(-time.Second).UnixNano()})
	machine.Apply(Command{Operation: Put, Key: "jobs/a", Value: "J"})
	if value, ok := machine.Get("users/b", now); ok || value != "" {
		t.Fatal("expired value was visible")
	}
	items := machine.Range("", "", "users/", 10, now)
	if len(items) != 1 || items[0].Key != "users/a" {
		t.Fatalf("unexpected items: %#v", items)
	}
}

type localTransport struct {
	nodes map[string]*Node
	down  map[string]bool
}

func (t *localTransport) RequestVote(_ context.Context, peer string, request RequestVoteRequest) (RequestVoteResponse, error) {
	if t.down[peer] {
		return RequestVoteResponse{}, errors.New("down")
	}
	return t.nodes[peer].HandleRequestVote(request), nil
}

func (t *localTransport) AppendEntries(_ context.Context, peer string, request AppendEntriesRequest) (AppendEntriesResponse, error) {
	if t.down[peer] {
		return AppendEntriesResponse{}, errors.New("down")
	}
	return t.nodes[peer].HandleAppendEntries(request), nil
}

func TestRaftQuorumAndFollowerRecovery(t *testing.T) {
	transport := &localTransport{nodes: make(map[string]*Node), down: make(map[string]bool)}
	ids := []string{"n1", "n2", "n3"}
	for _, id := range ids {
		var peers []string
		for _, peer := range ids {
			if peer != id {
				peers = append(peers, peer)
			}
		}
		node, err := NewNode(id, peers, transport, t.TempDir()+"/"+id)
		if err != nil {
			t.Fatal(err)
		}
		transport.nodes[id] = node
	}
	leader := transport.nodes["n1"]
	leader.mu.Lock()
	leader.role, leader.term, leader.leaderID = Leader, 1, "n1"
	leader.mu.Unlock()
	transport.down["n3"] = true
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := leader.Propose(ctx, Command{Operation: Put, Key: "answer", Value: "42"}); err != nil {
		t.Fatal(err)
	}
	if value, ok := leader.machine.Get("answer", time.Now()); !ok || value != "42" {
		t.Fatal("committed value missing")
	}
	transport.down["n2"] = true
	ctx2, cancel2 := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel2()
	if _, err := leader.Propose(ctx2, Command{Operation: Put, Key: "unsafe", Value: "write"}); err == nil {
		t.Fatal("write succeeded without quorum")
	}
}

func BenchmarkRangeScan(b *testing.B) {
	machine := NewStateMachine()
	for index := 0; index < 100_000; index++ {
		machine.Apply(Command{Operation: Put, Key: fmt.Sprintf("key/%08d", index), Value: "value"})
	}
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		_ = machine.Range("key/00050000", "key/00051000", "", 100, time.Now())
	}
}
