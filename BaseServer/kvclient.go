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
	"ChubbyGo/Flake"
	"log"
	mrand "math/rand"
	"net/rpc"
	"sync/atomic"
	"time"
)

type Clerk struct {
	servers []*rpc.Client

	leader int // Record which one is the leader
	// To ensure consistency of operations
	seq         int      // Current operation number
	ClientID    uint64   // Record the current client ID
	serversIsOk *[]int32 // Used to record which server is currently connectable, it is a boolean bit
}

// When created, it already knows how to interact with the server
func MakeClerk(servers []*rpc.Client, IsOk *[]int32) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.serversIsOk = IsOk

	ck.leader = mrand.Intn(len(servers)) // Randomly select a starting value, generate a random number in the range (0, len(server)-1)
	ck.seq = 1
	ck.ClientID = Flake.GetSonyflake()

	log.Printf("INFO : Create a new clerk(%d).\n", ck.ClientID)

	return ck
}

/*
 * @brief: To ensure strong consistency, a client will only run one operation at a time
 */
func (ck *Clerk) Get(key string) string {
	// log.Printf("INFO : Clerk Get: %s\n", key)

	serverLength := len(ck.servers)
	for {
		args := &GetArgs{Key: key, ClientID: ck.ClientID, SeqNo: ck.seq}
		reply := new(GetReply)

		ck.leader %= serverLength
		// In Go, * and [] have different precedence, need to add parentheses, quite tricky
		if atomic.LoadInt32(&((*ck.serversIsOk)[ck.leader])) == 0 {
			ck.leader++
			continue // If cannot connect, switch
		}

		replyArrival := make(chan bool, 1)
		go func() {
			err := ck.servers[ck.leader].Call("RaftKV.Get", args, reply)
			flag := true
			if err != nil {
				log.Printf("ERROR : Get call error, Find the cause as soon as possible -> (%s).\n", err.Error())
				flag = false
			}
			replyArrival <- flag
		}()
		select {
		case ok := <-replyArrival:
			if ok {
				if reply.Err == OK || reply.Err == ErrNoKey || reply.Err == Duplicate {
					log.Println(ck.ClientID, reply.Err, reply.Value)
					ck.seq++
					return reply.Value
				} else if reply.Err == ReElection || reply.Err == NoLeader { // In these two cases, we need to resend the request, i.e., reselect the leader
					ck.leader++
				}
			} else {
				ck.leader++
			}
		case <-time.After(200 * time.Millisecond): // If RPC exceeds 200ms, switch server. Generally, 200ms is definitely enough if there is no issue with the channel
			ck.leader++
		}
	}
}

func (ck *Clerk) PutAppend(key string, value string, op string) {
	// log.Printf("INFO : Clerk(%d) operation(%s): key(%s),value(%s)\n", ck.ClientID, op, key, value)

	cnt := len(ck.servers)
	for {
		args := &PutAppendArgs{Key: key, Value: value, Op: op, ClientID: ck.ClientID, SeqNo: ck.seq}
		reply := new(PutAppendReply)

		ck.leader %= cnt

		if atomic.LoadInt32(&((*ck.serversIsOk)[ck.leader])) == 0 {
			ck.leader++ // If cannot connect, switch
			continue
		}

		replyArrival := make(chan bool, 1)
		go func() {
			err := ck.servers[ck.leader].Call("RaftKV.PutAppend", args, reply)
			flag := true
			if err != nil {
				log.Printf("ERROR : PutAppend call error, Find the cause as soon as possible -> (%s).\n", err.Error())
				flag = false
			}
			replyArrival <- flag
		}()
		select {
		case <-time.After(200 * time.Millisecond): // RPC timeout: 200ms
			ck.leader++
			continue
		case ok := <-replyArrival:
			if ok && (reply.Err == OK || reply.Err == Duplicate) {
				ck.seq++
				return
			}
			ck.leader++
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
