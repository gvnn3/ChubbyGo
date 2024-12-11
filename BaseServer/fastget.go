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
	"log"
	"sync/atomic"
	"time"
)

/*
 * @brief: FastGet request does not provide consistency guarantee, directly fetches value from the main server's dictionary, bypassing the raft layer;
 * @notes: FastGet request is suitable for clients that do not modify a specific key for a long time after modifying it, so all clients can directly get it. In this case, fastget is the most efficient.
 */

func (ck *Clerk) FastGet(key string) string {
	serverLength := len(ck.servers)

	for {
		args := &GetArgs{Key: key, ClientID: ck.ClientID, SeqNo: ck.seq}
		reply := new(GetReply)

		ck.leader %= serverLength
		if atomic.LoadInt32(&((*ck.serversIsOk)[ck.leader])) == 0 {
			ck.leader++
			continue // Switch if cannot connect
		}

		replyArrival := make(chan bool, 1)
		go func() {
			err := ck.servers[ck.leader].Call("RaftKV.FastGet", args, reply)
			flag := true
			if err != nil {
				log.Printf("ERROR : FastGet call error, Find the cause as soon as possible -> (%s).\n", err.Error())
				flag = false
			}
			replyArrival <- flag
		}()
		select {
		case ok := <-replyArrival:
			if ok {
				if reply.Err == OK || reply.Err == ErrNoKey || reply.Err == Duplicate {
					log.Printf("INFO : FastGet -> ck.ClientID(%d), reply.Err(%s), reply.Value(%s).\n",
						ck.ClientID, reply.Err, reply.Value)
					ck.seq++
					return reply.Value
				} else if reply.Err == ReElection || reply.Err == NoLeader { // In these two cases, we need to resend the request, i.e., reselect the leader
					ck.leader++
				}
			} else {
				ck.leader++
			}
		case <-time.After(200 * time.Millisecond): // If RPC exceeds 200ms, switch server. Generally speaking, 200ms is definitely enough if there is no issue with the channel.
			ck.leader++
		}
	}
}

func (kv *RaftKV) FastGet(args *GetArgs, reply *GetReply) error {
	if atomic.LoadInt32(kv.ConnectIsok) == 0 {
		reply.Err = ConnectError
		return nil
	}

	// If not the leader anymore, return immediately
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = NoLeader
		return nil
	}

	log.Printf("INFO : ClientId[%d], FastGET:key(%s)\n", args.ClientID, args.Key)

	// This lock is still a bottleneck
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
	kv.mu.Unlock()

	reply.Err = OK

	if value, ok := kv.KvDictionary.ChubbyGoMapGet(args.Key); ok {
		reply.Value = value
	} else {
		reply.Err = ErrNoKey
		reply.Value = "" // This way client.go can have one less conditional statement
	}

	return nil
}
