package kv

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"
)

type Role string

const (
	Follower  Role = "follower"
	Candidate Role = "candidate"
	Leader    Role = "leader"
)

var ErrNotLeader = errors.New("node is not the leader")

type Entry struct {
	Index   uint64  `json:"index"`
	Term    uint64  `json:"term"`
	Command Command `json:"command"`
}

type RequestVoteRequest struct {
	Term         uint64 `json:"term"`
	CandidateID  string `json:"candidate_id"`
	LastLogIndex uint64 `json:"last_log_index"`
	LastLogTerm  uint64 `json:"last_log_term"`
}

type RequestVoteResponse struct {
	Term        uint64 `json:"term"`
	VoteGranted bool   `json:"vote_granted"`
}

type AppendEntriesRequest struct {
	Term         uint64  `json:"term"`
	LeaderID     string  `json:"leader_id"`
	PrevLogIndex uint64  `json:"prev_log_index"`
	PrevLogTerm  uint64  `json:"prev_log_term"`
	Entries      []Entry `json:"entries"`
	LeaderCommit uint64  `json:"leader_commit"`
}

type AppendEntriesResponse struct {
	Term          uint64 `json:"term"`
	Success       bool   `json:"success"`
	ConflictIndex uint64 `json:"conflict_index,omitempty"`
}

type Transport interface {
	RequestVote(context.Context, string, RequestVoteRequest) (RequestVoteResponse, error)
	AppendEntries(context.Context, string, AppendEntriesRequest) (AppendEntriesResponse, error)
}

type Node struct {
	mu            sync.Mutex
	id            string
	peers         []string
	transport     Transport
	store         *LogStore
	machine       *StateMachine
	role          Role
	term          uint64
	votedFor      string
	leaderID      string
	log           []Entry
	commitIndex   uint64
	lastApplied   uint64
	resetElection chan struct{}
	stop          chan struct{}
	stopOnce      sync.Once
}

func NewNode(id string, peers []string, transport Transport, dataDir string) (*Node, error) {
	store, meta, entries, err := OpenLogStore(dataDir)
	if err != nil {
		return nil, err
	}
	machine := NewStateMachine()
	lastApplied, err := store.LoadSnapshot(machine)
	if err != nil {
		return nil, err
	}
	if meta.CommitIndex >= uint64(len(entries)) {
		return nil, fmt.Errorf("commit index %d exceeds log", meta.CommitIndex)
	}
	for index := lastApplied + 1; index <= meta.CommitIndex; index++ {
		machine.Apply(entries[index].Command)
		lastApplied = index
	}
	return &Node{
		id: id, peers: peers, transport: transport, store: store, machine: machine,
		role: Follower, term: meta.Term, votedFor: meta.VotedFor, log: entries,
		commitIndex: meta.CommitIndex, lastApplied: lastApplied,
		resetElection: make(chan struct{}, 1), stop: make(chan struct{}),
	}, nil
}

func (n *Node) Start() {
	go n.electionLoop()
	go n.heartbeatLoop()
}

func (n *Node) Stop() { n.stopOnce.Do(func() { close(n.stop) }) }

func (n *Node) ID() string { return n.id }

func (n *Node) Status() (Role, uint64, string, uint64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.role, n.term, n.leaderID, n.commitIndex
}

func (n *Node) randomElectionTimeout() time.Duration {
	return time.Duration(350+rand.Intn(300)) * time.Millisecond
}

func (n *Node) electionLoop() {
	timer := time.NewTimer(n.randomElectionTimeout())
	defer timer.Stop()
	for {
		select {
		case <-n.stop:
			return
		case <-n.resetElection:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(n.randomElectionTimeout())
		case <-timer.C:
			n.startElection()
			timer.Reset(n.randomElectionTimeout())
		}
	}
}

func (n *Node) heartbeatLoop() {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-n.stop:
			return
		case <-ticker.C:
			n.mu.Lock()
			leader := n.role == Leader
			n.mu.Unlock()
			if leader {
				ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
				_ = n.quorumHeartbeat(ctx)
				cancel()
			}
		}
	}
}

func (n *Node) startElection() {
	n.mu.Lock()
	if n.role == Leader {
		n.mu.Unlock()
		return
	}
	n.role = Candidate
	n.term++
	term := n.term
	n.votedFor = n.id
	n.leaderID = ""
	last := n.log[len(n.log)-1]
	_ = n.persistMetadataLocked()
	n.mu.Unlock()

	request := RequestVoteRequest{Term: term, CandidateID: n.id, LastLogIndex: last.Index, LastLogTerm: last.Term}
	votes := make(chan RequestVoteResponse, len(n.peers))
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	for _, peer := range n.peers {
		go func(peerID string) {
			response, err := n.transport.RequestVote(ctx, peerID, request)
			if err == nil {
				votes <- response
			}
		}(peer)
	}
	granted := 1
	needed := (len(n.peers)+1)/2 + 1
	for granted < needed {
		select {
		case response := <-votes:
			n.mu.Lock()
			if response.Term > n.term {
				n.becomeFollowerLocked(response.Term, "")
				n.mu.Unlock()
				return
			}
			n.mu.Unlock()
			if response.VoteGranted {
				granted++
			}
		case <-ctx.Done():
			return
		}
	}
	n.mu.Lock()
	if n.role == Candidate && n.term == term {
		n.role = Leader
		n.leaderID = n.id
	}
	n.mu.Unlock()
}

func (n *Node) HandleRequestVote(request RequestVoteRequest) RequestVoteResponse {
	n.mu.Lock()
	defer n.mu.Unlock()
	if request.Term < n.term {
		return RequestVoteResponse{Term: n.term}
	}
	if request.Term > n.term {
		n.becomeFollowerLocked(request.Term, "")
	}
	last := n.log[len(n.log)-1]
	upToDate := request.LastLogTerm > last.Term || (request.LastLogTerm == last.Term && request.LastLogIndex >= last.Index)
	if upToDate && (n.votedFor == "" || n.votedFor == request.CandidateID) {
		n.votedFor = request.CandidateID
		_ = n.persistMetadataLocked()
		n.signalElectionReset()
		return RequestVoteResponse{Term: n.term, VoteGranted: true}
	}
	return RequestVoteResponse{Term: n.term}
}

func (n *Node) HandleAppendEntries(request AppendEntriesRequest) AppendEntriesResponse {
	n.mu.Lock()
	defer n.mu.Unlock()
	if request.Term < n.term {
		return AppendEntriesResponse{Term: n.term, ConflictIndex: uint64(len(n.log))}
	}
	if request.Term > n.term || n.role != Follower {
		n.becomeFollowerLocked(request.Term, request.LeaderID)
	}
	n.leaderID = request.LeaderID
	n.signalElectionReset()
	if request.PrevLogIndex >= uint64(len(n.log)) {
		return AppendEntriesResponse{Term: n.term, ConflictIndex: uint64(len(n.log))}
	}
	if n.log[request.PrevLogIndex].Term != request.PrevLogTerm {
		conflictTerm := n.log[request.PrevLogIndex].Term
		index := request.PrevLogIndex
		for index > 0 && n.log[index-1].Term == conflictTerm {
			index--
		}
		return AppendEntriesResponse{Term: n.term, ConflictIndex: index}
	}

	changed := false
	for offset, incoming := range request.Entries {
		index := request.PrevLogIndex + 1 + uint64(offset)
		if index < uint64(len(n.log)) && n.log[index].Term != incoming.Term {
			n.log = n.log[:index]
			changed = true
		}
		if index == uint64(len(n.log)) {
			n.log = append(n.log, incoming)
			changed = true
		}
	}
	if changed {
		if err := n.store.Rewrite(n.log); err != nil {
			return AppendEntriesResponse{Term: n.term, ConflictIndex: uint64(len(n.log))}
		}
	}
	if request.LeaderCommit > n.commitIndex {
		n.commitIndex = min(request.LeaderCommit, uint64(len(n.log)-1))
		n.applyCommittedLocked()
		_ = n.persistMetadataLocked()
	}
	return AppendEntriesResponse{Term: n.term, Success: true}
}

func (n *Node) Propose(ctx context.Context, command Command) (uint64, error) {
	n.mu.Lock()
	if n.role != Leader {
		n.mu.Unlock()
		return 0, ErrNotLeader
	}
	entry := Entry{Index: uint64(len(n.log)), Term: n.term, Command: command}
	if err := n.store.Append(entry); err != nil {
		n.mu.Unlock()
		return 0, err
	}
	n.log = append(n.log, entry)
	term := n.term
	n.mu.Unlock()

	acknowledged := 1
	responses := make(chan bool, len(n.peers))
	for _, peer := range n.peers {
		go func(peerID string) { responses <- n.replicateToPeer(ctx, peerID, term) }(peer)
	}
	needed := (len(n.peers)+1)/2 + 1
	for acknowledged < needed {
		select {
		case success := <-responses:
			if success {
				acknowledged++
			}
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.role != Leader || n.term != term {
		return 0, ErrNotLeader
	}
	n.commitIndex = entry.Index
	n.applyCommittedLocked()
	if err := n.persistMetadataLocked(); err != nil {
		return 0, err
	}
	return entry.Index, nil
}

func (n *Node) replicateToPeer(ctx context.Context, peer string, expectedTerm uint64) bool {
	n.mu.Lock()
	next := uint64(len(n.log) - 1)
	n.mu.Unlock()
	for {
		n.mu.Lock()
		if n.role != Leader || n.term != expectedTerm {
			n.mu.Unlock()
			return false
		}
		previous := next - 1
		request := AppendEntriesRequest{
			Term: n.term, LeaderID: n.id, PrevLogIndex: previous, PrevLogTerm: n.log[previous].Term,
			Entries: append([]Entry(nil), n.log[next:]...), LeaderCommit: n.commitIndex,
		}
		n.mu.Unlock()
		response, err := n.transport.AppendEntries(ctx, peer, request)
		if err != nil {
			return false
		}
		n.mu.Lock()
		if response.Term > n.term {
			n.becomeFollowerLocked(response.Term, "")
			n.mu.Unlock()
			return false
		}
		n.mu.Unlock()
		if response.Success {
			return true
		}
		if response.ConflictIndex == 0 {
			return false
		}
		next = response.ConflictIndex
	}
}

func (n *Node) quorumHeartbeat(ctx context.Context) bool {
	n.mu.Lock()
	if n.role != Leader {
		n.mu.Unlock()
		return false
	}
	term := n.term
	last := n.log[len(n.log)-1]
	commit := n.commitIndex
	n.mu.Unlock()
	acks := 1
	responses := make(chan bool, len(n.peers))
	for _, peer := range n.peers {
		go func(peerID string) {
			response, err := n.transport.AppendEntries(ctx, peerID, AppendEntriesRequest{Term: term, LeaderID: n.id, PrevLogIndex: last.Index, PrevLogTerm: last.Term, LeaderCommit: commit})
			responses <- err == nil && response.Success
		}(peer)
	}
	needed := (len(n.peers)+1)/2 + 1
	for acks < needed {
		select {
		case ok := <-responses:
			if ok {
				acks++
			}
		case <-ctx.Done():
			return false
		}
	}
	return true
}

func (n *Node) LinearizableGet(ctx context.Context, key string) (string, bool, error) {
	if !n.quorumHeartbeat(ctx) {
		return "", false, ErrNotLeader
	}
	value, ok := n.machine.Get(key, time.Now())
	return value, ok, nil
}

func (n *Node) LinearizableRange(ctx context.Context, start, end, prefix string, limit int) ([]Item, error) {
	if !n.quorumHeartbeat(ctx) {
		return nil, ErrNotLeader
	}
	return n.machine.Range(start, end, prefix, limit, time.Now()), nil
}

func (n *Node) Snapshot() error {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.store.SaveSnapshot(n.lastApplied, n.machine)
}

func (n *Node) applyCommittedLocked() {
	for n.lastApplied < n.commitIndex {
		n.lastApplied++
		n.machine.Apply(n.log[n.lastApplied].Command)
	}
}

func (n *Node) becomeFollowerLocked(term uint64, leader string) {
	if term > n.term {
		n.term = term
		n.votedFor = ""
	}
	n.role = Follower
	n.leaderID = leader
	_ = n.persistMetadataLocked()
	n.signalElectionReset()
}

func (n *Node) persistMetadataLocked() error {
	return n.store.SaveMetadata(metadata{Term: n.term, VotedFor: n.votedFor, CommitIndex: n.commitIndex})
}

func (n *Node) signalElectionReset() {
	select {
	case n.resetElection <- struct{}{}:
	default:
	}
}
