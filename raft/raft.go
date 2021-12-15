// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"math/rand"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64 // 自身的ID

	Term uint64 // 任期
	Vote uint64 // 给谁投票

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool // 投票记录

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int // Leader推动心跳计时器(heartbeatElapsed)
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int // Follower推动选举计时器(electionElapsed)

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64

	randomElectionElapsed int
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}

	prs := make(map[uint64]*Progress)
	for _, peer := range c.peers {
		prs[peer] = &Progress{}
	}
	raft := Raft{}
	raft.Prs = prs
	raft.id = c.ID
	raft.Term = None
	raft.Vote = None
	raft.RaftLog = newLog(c.Storage)
	raft.State = StateFollower
	raft.votes = nil
	raft.msgs = nil
	raft.Lead = None
	raft.heartbeatTimeout = c.HeartbeatTick
	raft.electionTimeout = c.ElectionTick
	raft.randomElectionElapsed = c.ElectionTick + rand.Intn(c.ElectionTick)
	return &raft
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	msg := pb.Message{
		From:    r.id,
		To:      to,
		Term:    r.Term,
		MsgType: pb.MessageType_MsgPropose,
	}
	r.msgs = append(r.msgs, msg)
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	msg := pb.Message{
		From:    r.id,
		To:      to,
		Term:    r.Term,
		MsgType: pb.MessageType_MsgHeartbeat,
	}
	r.msgs = append(r.msgs, msg)
}

// 驱动内部逻辑时钟摆动一个时钟, 来使得选举事件或者心跳事件发生超时.
func (r *Raft) tick() {
	switch r.State {
	case StateFollower:
		r.electionElapsed += 1
		if r.electionElapsed >= r.randomElectionElapsed {
			r.electionElapsed = 0
			r.Step(pb.Message{MsgType: pb.MessageType_MsgHup, Term: r.Term, From: r.id})
		}
	case StateCandidate:
		r.electionElapsed += 1
		if r.electionElapsed >= r.randomElectionElapsed {
			r.electionElapsed = 0
			r.Step(pb.Message{MsgType: pb.MessageType_MsgHup, Term: r.Term, From: r.id})
		}
	case StateLeader:
		r.heartbeatElapsed += 1
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			r.heartbeatElapsed = 0
			r.Step(pb.Message{MsgType: pb.MessageType_MsgBeat, Term: r.Term, From: r.id})
		}
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	r.randomElectionElapsed = r.electionTimeout + rand.Intn(r.electionTimeout)
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.votes = make(map[uint64]bool, 0)
	r.Lead = None

	r.State = StateFollower
	if term > r.Term {
		r.Term = term
		r.Vote = None
	}

}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	r.randomElectionElapsed = r.electionTimeout + rand.Intn(r.electionTimeout)
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.votes = make(map[uint64]bool)
	r.Lead = None

	r.State = StateCandidate
	r.Term = r.Term + 1
	r.Vote = r.id
	r.votes[r.id] = true
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	r.randomElectionElapsed = r.electionTimeout + rand.Intn(r.electionTimeout)
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.votes = make(map[uint64]bool)
	r.Lead = None

	r.State = StateLeader
	r.Lead = r.id
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	if m.Term > r.Term {
		r.becomeFollower(m.Term, m.From)
	}
	switch r.State {
	case StateFollower:
		switch m.MsgType {
		case pb.MessageType_MsgHup:
			r.becomeCandidate()
			if len(r.Prs) == 1 {
				r.becomeLeader()
			} else {
				for prs := range r.Prs {
					if prs != r.id {
						msg := pb.Message{
							From:    r.id,
							To:      prs,
							Term:    r.Term,
							MsgType: pb.MessageType_MsgRequestVote,
							LogTerm: r.RaftLog.LastTerm(),
							Index:   r.RaftLog.LastIndex(),
						}
						r.msgs = append(r.msgs, msg)
					}
				}
			}
		case pb.MessageType_MsgAppend:
			if m.Term >= r.Term {
				r.Term = m.Term
				r.handleAppendEntries(m)
			}
		}

	case StateCandidate:
		switch m.MsgType {
		case pb.MessageType_MsgRequestVoteResponse:
			if !m.Reject {
				r.votes[m.From] = true
				if len(r.votes) > len(r.Prs)/2 {
					r.becomeLeader()
				}
			}

		case pb.MessageType_MsgHup:
			r.becomeCandidate()
			for prs := range r.Prs {
				if prs != r.id {
					msg := pb.Message{
						From:    r.id,
						To:      prs,
						Term:    r.Term,
						MsgType: pb.MessageType_MsgRequestVote,
						LogTerm: r.RaftLog.LastTerm(),
						Index:   r.RaftLog.LastIndex(),
					}
					r.msgs = append(r.msgs, msg)
				}
			}
		case pb.MessageType_MsgAppend:
			if m.Term >= r.Term {
				r.Term = m.Term
				r.handleAppendEntries(m)
				r.becomeFollower(m.Term, m.From)
			}
		}
	case StateLeader:
		switch m.MsgType {
		case pb.MessageType_MsgBeat:
			for prs := range r.Prs {
				if prs != r.id {
					r.sendHeartbeat(prs)
				}
			}
		case pb.MessageType_MsgPropose:
			for prs := range r.Prs {
				if prs != r.id {
					r.sendAppend(prs)
				}
			}
		case pb.MessageType_MsgAppend:
			if m.Term >= r.Term {
				r.Term = m.Term
				r.becomeFollower(m.Term, m.From)
			}
		}
	}

	if m.MsgType == pb.MessageType_MsgRequestVote {
		reject := true
		if r.Term > m.Term {
			reject = true
		} else if r.Term == m.Term {
			if r.Vote != None && r.Vote != m.From{
				reject = true
			} else {
				reject = false
				r.Vote = m.From
			}
		} else if r.RaftLog.LastTerm() > m.LogTerm {
			reject = true
		} else if r.RaftLog.LastTerm() == m.LogTerm {
			if r.RaftLog.LastIndex() > m.Index {
				reject = true
			}else {
				reject = false
				r.Vote = m.From
			}
		} else {
			reject = false
			r.Vote = m.From
		}
		msg := pb.Message{
			From:    r.id,
			To:      m.From,
			Term:    r.Term,
			MsgType: pb.MessageType_MsgRequestVoteResponse,
			Reject:  reject,
		}
		r.msgs = append(r.msgs, msg)
	}
	return nil
}


// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}
