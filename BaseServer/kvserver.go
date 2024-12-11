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

package BaseServer

import (
	"ChubbyGo/Persister"
	"ChubbyGo/Raft"
	"bytes"
	"encoding/gob"
	"log"
	"net/rpc"
	"sync"
	"sync/atomic"
)

type LatestReply struct {
	Seq   int    // Latest sequence number
	Value string // The reason why get does not directly fetch from db is that the latest value at the time of fetching may not be the latest value at the time of reading. We need a strictly ordered sequence of operations.
}

type RaftKV struct {
	mu      sync.Mutex
	me      uint64
	rf      *Raft.Raft
	applyCh chan Raft.ApplyMsg

	maxraftstate   int                   // Snapshot threshold
	persist        *Persister.Persister  // For persistence
	LogIndexNotice map[int]chan struct{} // For synchronizing information between server and raft layer

	// Information that needs to be persisted
	snapshotIndex  int                    // The position in the log up to which everything has been snapshotted, including this position
	KvDictionary   *ChubbyGoConcurrentMap // Performance improved by about 30% after changing to concurrent map
	ClientSeqCache map[int64]*LatestReply // Used to determine if the current request has already been executed

	// Both of the following items are used as notification mechanisms; note that 0 is used as an invalid value to avoid using locks when reading. ClientInstanceSeq is used as instance and token.
	ClientInstanceSeq map[uint64]chan uint64 // Used to return InstanceSeq to the client
	// Only used for Open operations
	ClientInstanceCheckSum map[uint64]chan uint64 // Used to return CheckSum to the client

	CASNotice map[uint64]chan bool // Used as a notification for CAS operations, as CAS is not always successful

	shutdownCh chan struct{}

	// Obviously, the maximum value of this number is 1, which means the connection is successful, and there are only two cases, 0 and 1.
	// The reason for not using bool is that Golang's atomic does not seem to provide an atomic_flag like C++ to ensure a lock-free bool value.
	// If you forcefully use bool with a lock, it is slow and difficult to write, because raft and kvraft should share this value.
	ConnectIsok *int32 // Used to synchronize the specific startup time of services between servers. Raft and kvraft should use the same value.
	/*
	   - ok is the time point when this process connects to all other servers. Obviously, the ok process and the first ok process can communicate immediately.
	   - Obviously, the election and heartbeat behavior performed by p1 on other servers when it just ok should be invalid; so all functions called by RPC should first check ConnectIsok to decide whether to return a value.
	   - However, the daemon process of p1 has already started.
	   - The current approach is to set a field called ConnectIsok on each server.
	   - When being RPCed by a remote end, if the connection of this segment is not yet complete, the RPC returns a failure.
	     p1			p2			p3
	     ok
	     ok
	     ok
	*/
}

// Used in server_handler.go to register raft service
func (kv *RaftKV) GetRaft() *Raft.Raft {
	return kv.rf
}

// Obviously, concurrent execution of multiple operations is not allowed for the same client, it will cause deadlock, this is my defined usage specification
func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) error {
	// This may also be triggered by kvraft, used when the client connects to three servers, but one of the servers has not yet connected to all other servers. At this time, this server should reject the request.
	// The client only needs to switch the leader.
	if atomic.LoadInt32(kv.ConnectIsok) == 0 {
		reply.Err = ConnectError
		return nil
	}

	// If it is no longer the leader, return immediately
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = NoLeader
		return nil
	}

	NewOperation := KvOp{Key: args.Key, Op: "Get", ClientID: args.ClientID, Clientseq: args.SeqNo}

	Notice := make(chan struct{})

	log.Printf("INFO : ClientId[%d], GET:key(%s)\n", args.ClientID, args.Key)

	kv.mu.Lock()
	if dup, ok := kv.ClientSeqCache[int64(args.ClientID)]; ok {
		if args.SeqNo <= dup.Seq {
			kv.mu.Unlock()
			log.Printf("WARNING : ClientId[%d], This request(GET -> key(%s)) is repeated.\n", args.ClientID, args.Key)
			reply.Err = Duplicate
			reply.Value = dup.Value
			return nil
		}
	}

	// Raft itself has a lock, the reason for putting it in the critical section is that we must get the index, and start must be after checking clientSeqCache. Fortunately, Start is not a blocking function.
	index, term, _ := kv.rf.Start(NewOperation)

	kv.LogIndexNotice[index] = Notice

	kv.mu.Unlock()

	reply.Err = OK

	select {
	case <-Notice:
		curTerm, isLeader := kv.rf.GetState()
		// It may re-elect before or after submission, so it needs to be resent
		if !isLeader || term != curTerm {
			reply.Err = ReElection
			return nil
		}

		// After changing to ChubbyGoMap, there is no need to lock mu.lock; thus improving concurrency;
		// log.Printf("DEBUG : Now key(%s).\n", args.Key)
		if value, ok := kv.KvDictionary.ChubbyGoMapGet(args.Key); ok {
			reply.Value = value
		} else {
			reply.Err = ErrNoKey
			reply.Value = "" // This way client.go can have one less conditional statement
		}

	case <-kv.shutdownCh:
	}

	return nil
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	// This end has not yet successfully connected to other servers
	if atomic.LoadInt32(kv.ConnectIsok) == 0 {
		reply.Err = ConnectError
		return nil
	}

	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = NoLeader
		return nil
	}

	NewOperation := KvOp{Key: args.Key, Value: args.Value, Op: args.Op, ClientID: args.ClientID, Clientseq: args.SeqNo}
	Notice := make(chan struct{})

	log.Printf("INFO : ClientId[%d], PUTAPPEND:key(%s),value(%s)\n", args.ClientID, args.Key, args.Value)

	kv.mu.Lock()
	if dup, ok := kv.ClientSeqCache[int64(args.ClientID)]; ok {
		//log.Printf("DEBUG : args.SeqNo : %d , dup.Seq : %d\n", args.SeqNo, dup.Seq)
		if args.SeqNo <= dup.Seq {
			kv.mu.Unlock()

			log.Printf("WARNING : ClientId[%d], This request(PutAppend -> key(%s) value(%s)) is repeated.\n", args.ClientID, args.Key, args.Value)
			reply.Err = Duplicate
			return nil
		}
	}

	index, term, _ := kv.rf.Start(NewOperation)
	//log.Printf("DEBUG client %d : index %d\n", kv.me, index)

	kv.LogIndexNotice[index] = Notice

	kv.mu.Unlock()

	reply.Err = OK

	select {
	case <-Notice:
		curTerm, isLeader := kv.rf.GetState()
		if !isLeader || term != curTerm {
			reply.Err = ReElection
			return nil
		}
	case <-kv.shutdownCh:
		return nil
	}

	return nil
}

/*
 * @notes: This function must be called within the lock protection range of kvraft
 */
func (kv *RaftKV) persisterSnapshot(index int) {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)

	kv.snapshotIndex = index

	e.Encode(kv.KvDictionary) // The values before the snapshot point have been stored in KvDictionary
	e.Encode(kv.snapshotIndex)
	e.Encode(kv.ClientSeqCache)

	data := w.Bytes()

	// The disk operation here is within the critical section, which is one of the reasons why always is not recommended
	kv.persist.SaveSnapshot(data)
}

func (kv *RaftKV) readSnapshot(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)

	// TODO This also needs to be changed
	kv.KvDictionary = NewChubbyGoMap(SyncMap)
	kv.ClientSeqCache = make(map[int64]*LatestReply)

	d.Decode(&kv.KvDictionary)
	d.Decode(&kv.snapshotIndex)
	d.Decode(&kv.ClientSeqCache)
}

/*func (kv *RaftKV) Kill() {
	close(kv.shutdownCh)
	kv.rf.Kill()

}*/

func StartKVServerInit(me uint64, persister *Persister.Persister, maxraftstate int, chubbygomap uint32) *RaftKV {
	gob.Register(KvOp{})
	gob.Register(FileOp{})
	gob.Register(CASOp{})

	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate

	kv.applyCh = make(chan Raft.ApplyMsg)

	kv.KvDictionary = NewChubbyGoMap(chubbygomap)
	kv.ClientInstanceSeq = make(map[uint64]chan uint64)
	kv.ClientInstanceCheckSum = make(map[uint64]chan uint64)
	kv.CASNotice = make(map[uint64]chan bool)
	kv.LogIndexNotice = make(map[int]chan struct{})
	kv.persist = persister

	kv.shutdownCh = make(chan struct{})

	kv.ClientSeqCache = make(map[int64]*LatestReply)

	var Isok int32 = 0 // Maximum can only be 1 because it will only be incremented once when the connection is successful
	kv.ConnectIsok = &Isok

	// Read the snapshot at the beginning, when encountering invalid open files
	kv.readSnapshot(persister.ReadSnapshotFromFile())
	// ps: Very important, because the values read from the file are only fields, they have not been stored in persister yet. Only after storing can persistence be successful, otherwise, the snapshot.hdb will be 0 after a crash restart.
	kv.persisterSnapshot(kv.snapshotIndex) // Writing this way will cause the initial snapshot.hdb file size to be 76, but it doesn't matter much because it must be done this way

	//log.Printf("DEBUG : [%d] snapshotIndex(%d), len(%d) .\n",kv.me ,kv.snapshotIndex, len(kv.ClientSeqCache))

	// If not written here, it will be written in InitFileOperation, causing circular calls. The structure here can be modified later, the coupling is too high.
	RootFileOperation.pathToFileSystemNodePointer[RootFileOperation.root.nowPath] = RootFileOperation.root

	return kv
}

func (kv *RaftKV) StartKVServer(servers []*rpc.Client) {

	atomic.AddInt32(kv.ConnectIsok, 1) // By this point, it must have connected to other servers

	// Start kv service
	kv.rf.MakeRaftServer(servers)

	go kv.acceptFromRaftDaemon()
}

func (kv *RaftKV) StartRaftServer(address *[]string) {
	kv.rf = Raft.MakeRaftInit(kv.me, kv.persist, kv.applyCh, kv.ConnectIsok, address)
}
