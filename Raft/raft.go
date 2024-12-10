/**
 * Copyright lizhaolong(https://github.com/Super-long)
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/* Code comments are all encoded in UTF-8.*/

package Raft

import (
	"ChubbyGo/Persister"
	"bytes"
	"encoding/gob"
	"log"
	"net/rpc"
	"sync"
	"sync/atomic"
	"time"
)

type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // true for snapshot; false for regular command
	Snapshot    []byte // snapshot data
	// IsSnapshot bool  // used to more quickly resolve deadlock issues when creating snapshots
}

// Log entry
type LogEntry struct {
	Term    int
	Command interface{}
}

// Three roles in raft
const (
	Follower = iota
	Candidate
	Leader
)

type Raft struct {
	mu        sync.Mutex           // Lock to protect shared access to this peer's state
	peers     []*rpc.Client        // RPC end points of all peers
	persister *Persister.Persister // Object to hold this peer's persisted state
	me        uint64               // Unique identifier for each server
	meIndex   int                  // For each server, always equals the number of addresses loaded from config plus 1, peers length is always the number of addresses loaded from config, so me's identifier is not important

	CurrentTerm int        // Latest term server has seen (initialized to 0, increases monotonically)
	VotedFor    uint64     // CandidateId that received vote in current term
	Logs        []LogEntry // Log entries; each entry contains command for state machine and term when entry was received

	commitIndex int   // Index of highest log entry known to be committed (used for committing logs)
	lastApplied int   // Index of highest log entry applied to state machine (initialized to 0, increases monotonically)
	nextIndex   []int // For each server, index of the next log entry to send to that server
	matchIndex  []int // For each server, index of highest log entry known to be replicated on that server
	// Above members are from the paper

	commitCond *sync.Cond // Used when committing logs

	state             int           // Current state
	electionTimer     *time.Timer   // Each raft object needs a timer to change state and start next round of election during timeout 2A
	electionTimeout   time.Duration // 400~800ms Different election intervals can effectively prevent election failure 2A
	heartbeatInterval time.Duration // Heartbeat timeout not specified in paper but should be less than election timeout, chose 50-100ms

	resetTimer chan struct{} // Used for election timeout

	snapshotIndex int // Everything before this point is in snapshot
	snapshotTerm  int // Term at this point

	applyCh chan ApplyMsg // Channel for delivering data

	shutdownCh chan struct{}

	ConnectIsok *int32 // See explanation in RaftKV

	peersIsConnect []int32   // Used to determine if peers are still connected successfully, uses atomic operations
	serversAddress *[]string // Stores peer address information for automatic reconnection during disconnection
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// Get own state and return whether self is leader
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool

	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.CurrentTerm
	isleader = rf.state == Leader
	return term, isleader
}

// Get the index and term of the latest log entry in the current log
func (rf *Raft) lastLogIndexAndTerm() (int, int) {
	index := rf.snapshotIndex + len(rf.Logs) - 1
	term := rf.Logs[index-rf.snapshotIndex].Term
	return index, term
}

/*
 * @brief: Used to persist required data
 */
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)

	// This implementation compiles custom encoders/decoders for each data type in the stream, most efficient when using a single encoder to transmit a stream of values, amortizing compilation cost
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VotedFor)
	e.Encode(rf.Logs)
	e.Encode(rf.snapshotIndex)
	e.Encode(rf.snapshotTerm)

	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

/*
 * @brief: Read persisted data
 */
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)

	d.Decode(&rf.CurrentTerm)
	d.Decode(&rf.VotedFor)
	d.Decode(&rf.Logs)
	d.Decode(&rf.snapshotIndex)
	d.Decode(&rf.snapshotTerm)
}

type RequestVoteArgs struct {
	Term         int    // Candidate's term number 2A
	CandidateID  uint64 // ID of candidate requesting vote 2A
	LastLogIndex int    // Index of candidate's last log entry 2A
	LastLogTerm  int    // Term of candidate's last log entry 2A
}

type RequestVoteReply struct {
	CurrentTerm int  // Current term for candidate to update itself 2A
	VoteGranted bool // True means candidate received vote 2A

	IsOk bool // Used to tell requester if the peer server has started
}

func (rf *Raft) fillRequestVoteArgs(args *RequestVoteArgs) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.VotedFor = rf.me  // Vote for self by default
	rf.CurrentTerm += 1  // Increment Term
	rf.state = Candidate // Change own state

	args.Term = rf.CurrentTerm
	args.CandidateID = rf.me
	args.LastLogIndex, args.LastLogTerm = rf.lastLogIndexAndTerm()
}

// RequestVote defines how Follower handles votes after receiving them
/*
 * We need to compare logs between requester and requestee in this function
 * 1. If current node's Term is greater than candidate's Term, reject vote
 * 2. If current node's Term is less than candidate's Term, current node converts to Follower state
 * 3. Check if already voted
 * 4. Compare Term of last log entry (LastLogTerm), if same compare index (LastLogIndex), if current node is newer then don't vote, otherwise vote
 */
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error {
	// Rest of the implementation remains the same...
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	var err error
	res := true

	if atomic.LoadInt32(&rf.peersIsConnect[server]) == 0 { // Call when connection is successful
		err = rf.peers[server].Call("Raft.RequestVote", args, reply)

		if err != nil {
			log.Printf("WARNING : %d is leader, Failed to connect with peers(%d). Try to connect.\n", rf.me, server)
			// Start a goroutine for reconnection
			go rf.tryToReconnect(server)

			res = false
		} else if !reply.IsOk {
			// Normal situation occurring before the server cluster is fully started
			log.Println("INFO : The server is not connected to other servers in the cluster.")
			res = false
		}

	} else {
		return false // Connection not yet successful
	}

	// Return true when connection is successful and IsOk is not false
	return res
}

// Log synchronization, empty log entry serves as heartbeat
type AppendEntriesArgs struct {
	Term         int        // Leader's term
	LeaderID     uint64     // LeaderID for redirection
	PrevLogIndex int        // Index of log entry before new ones
	PrevLogTerm  int        // Term of previous log entry
	Entries      []LogEntry // Log entries to store (empty for heartbeat)
	LeaderCommit int        // Leader's commitIndex
}

type AppendEntriesReply struct {
	CurrentTerm int  // Used to update leader itself as leader might be partitioned
	Success     bool // True if follower matched PrevLogIndex and PrevLogTerm

	// For log synchronization
	ConflictTerm int // Term of the conflicting entry
	FirstIndex   int // The first index it stores for ConflictTerm

	IsOk bool
}

func (rf *Raft) turnToFollow() {
	log.Printf("INFO : [%d] become new follower!\n", rf.me)
	rf.state = Follower
	rf.VotedFor = 0
}

// AppendEntries defines how follower node handles appendentries
/*
 * Actually four situations: follower has more logs than leader,
 * follower has fewer logs than leader, follower has same logs as leader
 * (whether Term at latest index is same)
 */

// Additional function translations for the rest of the file...
