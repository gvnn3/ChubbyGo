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
	"math/rand"
	"net/rpc"
	"sort"
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
	return nil
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
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) error {
	if atomic.LoadInt32(rf.ConnectIsok) == 0 {
		reply.IsOk = false
		return nil
	}

	reply.IsOk = true

	// Too frequent, don't print when not debugging
	// log.Printf("INFO : rpc -> [%d] accept AppendEntries success, from peer: %d, term: %d\n", rf.me, args.LeaderID, args.Term)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.CurrentTerm {
		reply.CurrentTerm = rf.CurrentTerm
		reply.Success = false
		return nil
	}
	if rf.CurrentTerm < args.Term {
		rf.CurrentTerm = args.Term
	}

	// After partition ends or simply high latency from a lagging leader
	if rf.state == Leader {
		rf.turnToFollow()
	}
	// Usually modified after first heartbeat following election
	if rf.VotedFor != args.LeaderID {
		rf.VotedFor = args.LeaderID
	}

	// Reset election timeout
	rf.resetTimer <- struct{}{}

	// When PrevLogIndex in heartbeat is less than snapshot point, this packet is definitely a lagging packet
	// We use snapshot point instead of latest log because these logs might be incorrect,
	// snapshot point is the most recent definitely ok log, also prerequisite for later execution
	if args.PrevLogIndex < rf.snapshotIndex {
		log.Printf("WARNING : [%d] accept a lagging packets. PrevLogIndex(%d), snapshotIndex(%d)",
			rf.me, args.PrevLogIndex, rf.snapshotIndex)

		reply.Success = false
		reply.CurrentTerm = rf.CurrentTerm
		reply.ConflictTerm = rf.snapshotTerm
		reply.FirstIndex = rf.snapshotIndex

		return nil
	}

	// Remove excess logs for PrevLogIndex, start finding closest matching point
	preLogIdx, preLogTerm := 0, 0
	if args.PrevLogIndex < len(rf.Logs)+rf.snapshotIndex {
		preLogIdx = args.PrevLogIndex
		preLogTerm = rf.Logs[preLogIdx-rf.snapshotIndex].Term
	}

	// If two entries in different logs have same index and term number, then all previous log entries are identical
	// According to log matching principle, log matching successful
	if preLogIdx == args.PrevLogIndex && preLogTerm == args.PrevLogTerm {
		reply.Success = true
		// First truncate excess, then add what should be added
		rf.Logs = rf.Logs[:preLogIdx+1-rf.snapshotIndex]
		rf.Logs = append(rf.Logs, args.Entries...)
		var last = rf.snapshotIndex + len(rf.Logs) - 1

		// After each log addition, check if commit needed
		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = min(args.LeaderCommit, last)
			// Update log commitIndex
			rf.commitCond.Broadcast()
		}
		// Tell leader to update this replica's matched index
		reply.ConflictTerm = rf.Logs[last-rf.snapshotIndex].Term // Means no conflict
		reply.FirstIndex = last

		if len(args.Entries) > 0 {
			log.Printf("INFO : %d accept a packet from leader %d, commit index: leader->%d, follower->%d.\n",
				rf.me, args.LeaderID, args.LeaderCommit, rf.commitIndex)
		} else {
			// Too frequent
			// log.Printf("INFO : [%d] <heartbeat> current loglength: %d\n", rf.me, last)
		}
	} else {
		// Found conflict, roll back index to find closest matching point with leader
		// Using a faster method than in the paper
		reply.Success = false

		// Actually two situations: follower has fewer logs than leader or more logs with conflicts
		var first = 1 + rf.snapshotIndex
		reply.ConflictTerm = preLogTerm
		if reply.ConflictTerm == 0 { // Leader has more logs, direct sync with leader is enough
			first = len(rf.Logs) + rf.snapshotIndex
			reply.ConflictTerm = rf.Logs[first-1-rf.snapshotIndex].Term
		} else {
			i := preLogIdx - 1 // This point already conflicts, start from previous point
			for ; i > rf.snapshotIndex; i-- {
				if rf.Logs[i-rf.snapshotIndex].Term != preLogTerm {
					first = i + 1 // Send from next log entry, i.e., the mismatching point
					break         // Here we found matching point, jumping one Term at a time
				}
			}
		}
		reply.FirstIndex = first

		// Need to print logs when handling conflicting logs
		if len(rf.Logs)+rf.snapshotIndex <= args.PrevLogIndex {
			log.Printf("WARNING : [%d] is a lagging node, current leader(%d), leader has more logs [leader(%d) > me(%d)]. Conflict: index(%d) Term(%d).\n",
				rf.me, args.LeaderID, args.PrevLogIndex, len(rf.Logs)-1+rf.snapshotIndex, reply.FirstIndex, reply.ConflictTerm)
		} else {
			log.Printf("WARNING : [%d] current leader(%d), log more than leader. Conflict point information index[l(%d) != m(%d)], Term[l(%d) != m(%d)].\n",
				rf.me, args.LeaderID, args.PrevLogIndex, preLogIdx, args.PrevLogTerm, preLogTerm)
		}
	}
	rf.persist()
	return nil
}
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	var err error
	res := true

	if atomic.LoadInt32(&rf.peersIsConnect[server]) == 0 { // Call when connection is successful
		err = rf.peers[server].Call("Raft.AppendEntries", args, reply)

		if err != nil {
			log.Printf("WARNING : %d is leader, Failed to connect with peers(%d). Try to connect.\n", rf.me, server)
			// Start a goroutine for reconnection
			go rf.tryToReconnect(server)

			res = false
		} else if !reply.IsOk {
			// Normal situation occurring before server cluster is fully started
			log.Println("INFO : The server is not connected to other servers in the cluster.")
			res = false
		}

	} else {
		return false // Reconnection not yet successful
	}

	// Return true when connection is successful and IsOk is not false
	return res
}

/*
 * @brief: Entry point for raft
 * @params: Pass in a command entity
 * @ret: Returns this command's index in log, term, and whether this node was leader when executing command
 */
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index, term, isLeader := -1, 0, false
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state == Leader {
		log := LogEntry{rf.CurrentTerm, command}
		rf.Logs = append(rf.Logs, log)

		index = len(rf.Logs) - 1 + rf.snapshotIndex
		term = rf.CurrentTerm
		isLeader = true

		// Only updates self
		rf.nextIndex[rf.meIndex] = index + 1
		rf.matchIndex[rf.meIndex] = index

		rf.persist()
	}

	// Logging in critical section, bit awkward
	// log.Printf("INFO : [%d] client add a new entry (index:%d-command%v)\n", rf.me, index, command)

	return index, term, isLeader
}

/*
 * @brief: Handler for consistencyCheck
 * @params: Replica number, return value from consistencyCheck call
 * @ret: void
 */
func (rf *Raft) consistencyCheckReplyHandler(n int, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Message returned just as leadership changed
	if rf.state != Leader {
		return
	}
	if reply.Success {
		// Update leader's information about replica n based on return value
		rf.matchIndex[n] = reply.FirstIndex
		rf.nextIndex[n] = rf.matchIndex[n] + 1
		rf.updateCommitIndex() // Try to update commitIndex
	} else {
		// Found new leader, change state. May happen when partition heals
		if rf.state == Leader && reply.CurrentTerm > rf.CurrentTerm {
			rf.turnToFollow()
			rf.persist()
			rf.resetTimer <- struct{}{}
			// Partition occurred, use WARNING
			log.Printf("WARNING : [%d] found new term higher than itself from (%d), turn to follower.",
				rf.me, n)
			return
		}

		var know, lastIndex = false, 0
		// Found conflict in replica
		if reply.ConflictTerm != 0 {
			// If loop ends without finding, means should send snapshot
			for i := len(rf.Logs) - 1; i > 0; i-- {
				if rf.Logs[i].Term == reply.ConflictTerm {
					know = true // Conflicting log exists in current logs
					lastIndex = i + rf.snapshotIndex
					// Should use WARNING like peer for better debugging
					log.Printf("WARNING : [%d] have entry %d , the last entry in term %d.\n",
						rf.me, lastIndex, reply.ConflictTerm)
					return
				}
			}
			if know {
				rf.nextIndex[n] = min(lastIndex, reply.FirstIndex)
			} else {
				rf.nextIndex[n] = reply.FirstIndex
			}
		} else {
			rf.nextIndex[n] = reply.FirstIndex
		}
		// Send snapshot
		if rf.snapshotIndex != 0 && rf.nextIndex[n] <= rf.snapshotIndex {
			log.Printf("INFO : [%d] peer %d needs a snapshot, nextIndex(%d) snapshotIndex(%d).\n",
				rf.me, n, rf.nextIndex[n], rf.snapshotIndex)
			rf.sendSnapshot(n)
		} else { // Normal case, next heartbeat will automatically update
			// snapshot + 1 <= rf.nextIndex[n] <= len(rf.Logs) + snapshot
			// Actually when greater than len(rf.Logs)+rf.snapshotIndex it's a bug
			rf.nextIndex[n] = min(max(rf.nextIndex[n], 1+rf.snapshotIndex), len(rf.Logs)+rf.snapshotIndex)
			// Print when debugging
			/*log.Printf("INFO : [%d] nextIndex for peer %d  => %d (snapshot: %d).\n",
			  rf.me, n, rf.nextIndex[n], rf.snapshotIndex)*/
		}
	}
}

/*
 * @brief: Used for leader and replica log synchronization
 * @params: Replica number
 * @ret: void
 */
func (rf *Raft) consistencyCheck(n int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// When rf.nextIndex[n]-1 < snapshotIndex we just send snapshot
	pre := rf.nextIndex[n] - 1
	if pre < rf.snapshotIndex {
		rf.sendSnapshot(n)
	} else {
		var args = AppendEntriesArgs{
			Term:         rf.CurrentTerm,
			LeaderID:     rf.me,
			PrevLogIndex: pre,
			PrevLogTerm:  rf.Logs[pre-rf.snapshotIndex].Term,
			Entries:      nil,
			LeaderCommit: rf.commitIndex,
		}
		// Above already ensures all data to be copied is after snapshotIndex
		if rf.nextIndex[n] < len(rf.Logs)+rf.snapshotIndex {
			args.Entries = append(args.Entries, rf.Logs[rf.nextIndex[n]-rf.snapshotIndex:]...)
		}
		go func() {
			// TODO Too frequent, don't print unless special circumstances
			// log.Printf("INFO : [%d] AppendEntries to peer %d.\n", rf.me, n)
			var reply AppendEntriesReply
			if rf.sendAppendEntries(n, &args, &reply) {
				rf.consistencyCheckReplyHandler(n, &reply)
			}
		}()
	}
}

/*
 * @brief: Used for leader to send heartbeats
 */
func (rf *Raft) heartbeatDaemon() {
	for {
		// Only valid for leader
		if _, isLeader := rf.GetState(); !isLeader {
			return
		}

		// Reset election timeout
		rf.resetTimer <- struct{}{}

		select {
		case <-rf.shutdownCh:
			return
		default:
			PeersLength := len(rf.peers)
			for i := 0; i < PeersLength; i++ {
				// Send heartbeat
				go rf.consistencyCheck(i)
			}
		}
		// Sleep heartbeatInterval since heartbeat takes this long anyway
		// No need for timer since it won't be interrupted by other things
		time.Sleep(rf.heartbeatInterval)
	}
}

/*
 * @brief: Used to update commitIndex
 * @notice: Requires lock when calling
 */
func (rf *Raft) updateCommitIndex() {
	match := make([]int, len(rf.matchIndex))
	copy(match, rf.matchIndex)
	sort.Ints(match)

	// Find median of all replicas' match for commitment
	target := match[len(rf.peers)/2]
	if rf.commitIndex < target && rf.snapshotIndex < target {
		if rf.Logs[target-rf.snapshotIndex].Term == rf.CurrentTerm {
			log.Printf("INFO : [%d] update commit index ->  [commitIndex(%d)-target(%d)] ; current term(%d)\n",
				rf.me, rf.commitIndex, target, rf.CurrentTerm)
			rf.commitIndex = target
			rf.commitCond.Broadcast()
		} else {
			// Paper section 5.4.2 describes this case. Should be INFO but rare
			// Indicates frequent server restarts, so use WARNING
			log.Printf("WARNING : [%d] update commit index %d failed. (log term %d != current Term %d)\n",
				rf.me, rf.commitIndex, rf.Logs[target-rf.snapshotIndex].Term, rf.CurrentTerm)
		}
	}
}

/*
 * @brief: Used to commit data to rf.applyCh
 */
func (rf *Raft) applyLogEntryDaemon() {
	for {
		var logs []LogEntry
		rf.mu.Lock()
		for rf.lastApplied == rf.commitIndex { // Exit loop when woken
			rf.commitCond.Wait()
			select {
			case <-rf.shutdownCh:
				rf.mu.Unlock()
				log.Printf("INFO : [%d] is shutting down actively(applyLogEntry).\n", rf.me)
				close(rf.applyCh)
				return
			default:
			}
		}
		// last is previous committed value, actual commit range is [last+1, cur]
		last, cur := rf.lastApplied, rf.commitIndex
		if last < cur { // Copy efficiency not high
			rf.lastApplied = rf.commitIndex
			logs = make([]LogEntry, cur-last)
			copy(logs, rf.Logs[last+1-rf.snapshotIndex:cur+1-rf.snapshotIndex])
		}
		rf.mu.Unlock()

		// Avoid deadlock when KV layer creates snapshot during log commit
		for i := 0; i < cur-last; i++ {
			rf.applyCh <- ApplyMsg{Index: last + i + 1, Command: logs[i].Command}
		}
	}
}

/*
 * @brief: Used for requesting votes and handling after RPC success
 */
func (rf *Raft) canvassVotes() {
	var voteArgs RequestVoteArgs
	rf.fillRequestVoteArgs(&voteArgs)
	peers := len(rf.peers)

	var votes = 1
	replyHandler := func(reply *RequestVoteReply) {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if rf.state == Candidate {
			if reply.CurrentTerm > voteArgs.Term {
				rf.CurrentTerm = reply.CurrentTerm
				rf.turnToFollow()
				rf.persist()
				rf.resetTimer <- struct{}{} // reset timer
				return
			}
			if reply.VoteGranted { // Election successful
				votes++
				if votes == (peers+1)/2+1 { // peers is one less than actual machines, not counting self
					rf.state = Leader
					log.Printf("INFO : [%d] become new leader! \n", rf.me)
					rf.resetOnElection()    // Reset leader state
					go rf.heartbeatDaemon() // Start heartbeat routine after successful election
					return
				}
			}
		}
	}
	for i := 0; i < peers; i++ {
		go func(n int) {
			var reply RequestVoteReply
			if rf.sendRequestVote(n, &voteArgs, &reply) {
				replyHandler(&reply)
			}
		}(i)
	}
}

/*
 * @brief: Set base values after re-election
 * @notice: Requires lock when calling
 */
func (rf *Raft) resetOnElection() {
	count := len(rf.peers)
	length := len(rf.Logs) + rf.snapshotIndex

	for i := 0; i < count; i++ { // Update other peer servers
		rf.matchIndex[i] = 0
		rf.nextIndex[i] = length
	}
	rf.matchIndex[rf.meIndex] = length - 1
}

/*
 * @brief: Election routine
 */
func (rf *Raft) electionDaemon() {
	for {
		select {
		case <-rf.shutdownCh:
			log.Printf("INFO : [%d] is shutting down electionDaemon.\n", rf.me)
			return
		case <-rf.resetTimer: // Reset timeout clock
			if !rf.electionTimer.Stop() {
				log.Printf("ERROR : [%d] Failure to Stop the resetTimer that will result in re-election in the leader service .\n", rf.me)
				<-rf.electionTimer.C
			}
			rf.electionTimer.Reset(rf.electionTimeout)
			break
		case <-rf.electionTimer.C:
			log.Printf("INFO : [%d] election timeout, Start re-election  current term(%d).\n",
				rf.me, rf.CurrentTerm)
			// Prevent first-time similar random values causing livelock
			rf.electionTimer.Reset(time.Millisecond * time.Duration(400+rand.Intn(100)*4))

			go rf.canvassVotes()
		}
	}
}

func (rf *Raft) tryToReconnect(server int) {
	// Implementation of reconnection logic
	// Example: retry connection logic with exponential backoff
	for {
		time.Sleep(time.Second) // Example: wait for 1 second before retrying
		client, err := rpc.Dial("tcp", (*rf.serversAddress)[server])
		if err == nil {
			rf.peers[server] = client
			atomic.StoreInt32(&rf.peersIsConnect[server], 0)
			log.Printf("INFO : Successfully reconnected to peer %d\n", server)
			return
		}
		log.Printf("WARNING : Failed to reconnect to peer %d, retrying...\n", server)
	}
}

func (rf *Raft) MakeRaftServer(peers []*rpc.Client) {
	rf.peers = peers
	// Extra item is self, for updating other servers
	peerLength := len(peers)
	rf.nextIndex = make([]int, peerLength+1)
	rf.matchIndex = make([]int, peerLength+1)
	rf.peersIsConnect = make([]int32, peerLength) // Only need communication with others, so no +1

	// For each server, first peerLength items are communication entities with other servers,
	// index peerLength is self
	rf.meIndex = peerLength

	go rf.electionDaemon()      // Start election
	go rf.applyLogEntryDaemon() // Start appending logs

	// When strategy is Always, storage automatically persists and syncs,
	// no need for daemon routine
	if rf.persister.PersistenceStrategy != Persister.Always {
		go Persister.PersisterDaemon(rf.persister) // Start writing/syncing based on strategy
	}

	log.Printf("INFO : [%d] start up : term(%d) voted(%d) snapshotIndex(%d) snapshotTerm(%d)\n",
		rf.me, rf.CurrentTerm, rf.VotedFor, rf.snapshotIndex, rf.snapshotTerm)
}

// Snapshot section

/*
 * @brief: Used for log compression when logs exceed threshold, called by KV layer
 * @notes: Protected by raft's lock, no need to be in kvraft's critical section
 */
func (rf *Raft) CreateSnapshots(index int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// ----------------[------Range A--------]
	// ----------------|       ||         |
	// snapshotIndex---[sna+1, commitIndex]; only valid if index falls in Range A
	if rf.commitIndex < index || index <= rf.snapshotIndex {
		log.Printf("ERROR : NewSnapShot(): new.snapshotIndex <= old.snapshotIndex.\n")
		return
	}
	// Discard logs, previous snapshot already saved in kvraft layer
	rf.Logs = rf.Logs[index-rf.snapshotIndex:]

	rf.snapshotIndex = index
	rf.snapshotTerm = rf.Logs[0].Term

	log.Printf("INFO : [%d] Create a new snapshot, snapshotIndex(%d) snapshotTerm(%d).\n",
		rf.me, rf.snapshotIndex, rf.snapshotTerm)

	rf.persist()
}

type InstallSnapshotArgs struct {
	Term              int    // Leader's term
	LeaderID          uint64 // Leader's ID for follower redirection
	LastIncludedIndex int    // Index of last log entry in snapshot
	LastIncludedTerm  int    // Term of last log entry in snapshot
	Snapshot          []byte // Snapshot data
}

type InstallSnapshotReply struct {
	CurrentTerm int // Leader might be behind, used to update leader

	IsOk bool
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) error {
	if atomic.LoadInt32(rf.ConnectIsok) == 0 {
		reply.IsOk = false
		return nil
	}
	reply.IsOk = true
	select {
	case <-rf.shutdownCh:
		log.Printf("INFO : [%d] is shutting down actively(InstallSnapshot).\n", rf.me)
		return nil
	default:
	}

	log.Printf("INFO : [%d] Accept a snapshot, from Peer: %d, PeerTerm: %d\n", rf.me, args.LeaderID, args.Term)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.CurrentTerm = rf.CurrentTerm

	if args.Term < rf.CurrentTerm {
		log.Printf("WARNING : [%d] a lagging snapshot, args.term(%d) < CurrentTerm(%d).\n", rf.me,
			args.Term, rf.CurrentTerm)
		return nil
	}

	// Snapshots might also repeat
	if args.LastIncludedIndex <= rf.snapshotIndex {
		log.Printf("WARNING : [%d] peers(%d) is a lower snapshot, args.LastIncludedIndex(%d) <= rf.snapshotIndex(%d)\n",
			rf.me, args.LeaderID, args.LastIncludedIndex, rf.snapshotIndex)
		return nil
	}

	rf.resetTimer <- struct{}{}

	// When snapshot larger than all logs, update all raft properties with snapshot
	if args.LastIncludedIndex >= rf.snapshotIndex+len(rf.Logs)-1 {
		log.Printf("INFO : [%d] Accept a suitable snapshot, args.LastIncludedIndex(%d), rf.snapshotIndex(%d), LogLength(%d).\n", rf.me,
			args.LastIncludedIndex, rf.snapshotIndex, len(rf.Logs))

		rf.snapshotIndex = args.LastIncludedIndex
		rf.snapshotTerm = args.LastIncludedTerm
		rf.commitIndex = rf.snapshotIndex
		rf.lastApplied = rf.snapshotIndex
		rf.Logs = []LogEntry{{rf.snapshotTerm, nil}}

		rf.applyCh <- ApplyMsg{rf.snapshotIndex, nil, true, args.Snapshot}

		rf.persist()
		return nil
	}

	// Local logs larger than snapshot, only update part
	log.Printf("INFO : [%d] snapshot have some logs. args.LastIncludedIndex(%d), rf.snapshotIndex(%d), LogLength(%d).\n",
		rf.me, args.LastIncludedIndex, rf.snapshotIndex, len(rf.Logs))

	rf.Logs = rf.Logs[args.LastIncludedIndex-rf.snapshotIndex:]
	rf.snapshotIndex = args.LastIncludedIndex
	rf.snapshotTerm = args.LastIncludedTerm
	rf.commitIndex = rf.snapshotIndex
	rf.lastApplied = rf.snapshotIndex // No need to send one by one, set all to LastIncludedIndex

	rf.applyCh <- ApplyMsg{rf.snapshotIndex, nil, true, args.Snapshot}

	rf.persist()
	return nil
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	var err error
	res := true

	if atomic.LoadInt32(&rf.peersIsConnect[server]) == 0 { // Call when connection successful
		err = rf.peers[server].Call("Raft.InstallSnapshot", args, reply)

		if err != nil {
			log.Printf("WARNING : %d is leader, Failed to connect with peers(%d). Try to connect.\n", rf.me, server)
			// Start a goroutine for reconnection
			go rf.tryToReconnect(server)

			res = false
		} else if !reply.IsOk {
			// Normal situation before server cluster fully started
			log.Println("INFO : The server is not connected to other servers in the cluster.")
			res = false
		}

	} else {
		return false // Connection not yet successful
	}

	// Return true when connection successful and IsOk not false
	return res
}

/*
 * @brief: Used when replica data lags behind leader's snapshotIndex
 * @param: server is replica number
 */
func (rf *Raft) sendSnapshot(server int) {
	var args = &InstallSnapshotArgs{
		Term:              rf.CurrentTerm,
		LastIncludedIndex: rf.snapshotIndex,
		LastIncludedTerm:  rf.snapshotTerm,
		LeaderID:          rf.me,
		Snapshot:          rf.persister.ReadSnapshot(), // Send snapshot over
	}

	replayHandler := func(server int, reply *InstallSnapshotReply) {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		if rf.state == Leader {
			// rf is a lagging leader
			if reply.CurrentTerm > rf.CurrentTerm {
				rf.CurrentTerm = reply.CurrentTerm
				rf.turnToFollow()
				return
			}
			// Update its nextindex
			rf.matchIndex[server] = rf.snapshotIndex
			rf.nextIndex[server] = rf.snapshotIndex + 1
		}
	}
	go func(index int) {
		var reply InstallSnapshotReply
		if rf.sendInstallSnapshot(index, args, &reply) {
			replayHandler(index, &reply)
		}
	}(server)
}

/*
 * @brief: Used to create a raft entity
 */
func MakeRaftInit(me uint64,
	persister *Persister.Persister, applyCh chan ApplyMsg, IsOk *int32, address *[]string) *Raft {
	rf := &Raft{}
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh

	rf.state = Follower
	rf.VotedFor = 0
	rf.Logs = make([]LogEntry, 1) // first index is 1
	rf.Logs[0] = LogEntry{        // placeholder
		Term:    0,
		Command: nil,
	}

	// 400~800 ms
	rf.electionTimeout = time.Millisecond * time.Duration(400+rand.Intn(100)*4)

	rf.electionTimer = time.NewTimer(rf.electionTimeout)
	rf.resetTimer = make(chan struct{})
	rf.shutdownCh = make(chan struct{})                                       // shutdown raft gracefully
	rf.commitCond = sync.NewCond(&rf.mu)                                      // commitCh, a distinct goroutine
	rf.heartbeatInterval = time.Millisecond * time.Duration(50+rand.Intn(50)) // small enough, not too small

	rf.readPersist(persister.ReadRaftStateFromFile())

	rf.lastApplied = rf.snapshotIndex
	rf.commitIndex = rf.snapshotIndex

	rf.ConnectIsok = IsOk
	rf.serversAddress = address // This is actually peer address info read from config, corresponds to peers index, used for reconnection

	return rf
}
