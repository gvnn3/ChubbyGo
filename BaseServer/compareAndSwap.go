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

/*
 * @brief: The function of this feature is to support flash sales, for example, there are only 100 items, and the client only needs to perform atomic decrement (flag is 2, new is 1, old is 0). The basic logic is that the client sends a key value, and of course the value must be convertible to a number.
 * @param: key; old; new; flag;
 * @notes: flag is 1 for normal CAS operation; flag is 2 for decrementing new, minimum is old; flag is 4 for incrementing new, maximum is old;
 */

package BaseServer

import (
	"log"
	"strconv"
	"sync/atomic"
	"time"
)

const (
	Cas = 1
	Add = 2
	Sub = 4
)

/*
 * @brief: CAS operation is only valid when the value corresponding to the key is a number
 */
func (kv *RaftKV) CompareAndSwap(args *CompareAndSwapArgs, reply *CompareAndSwapReply) error {
	// This end has not yet successfully connected to other servers
	if atomic.LoadInt32(kv.ConnectIsok) == 0 {
		reply.Err = ConnectError
		return nil
	}

	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = NoLeader
		return nil
	}

	var ValueOfKey string

	// If the key is found, put it in OldValue, if not, return directly
	if value, ok := kv.KvDictionary.ChubbyGoMapGet(args.Key); ok {
		ValueOfKey = value
	} else {
		reply.Err = ErrNoKey // There may actually be other error reasons, see ChubbyGoMapGet for details
		return nil
	}

	// This step ensures that the value can be converted to a number and assigned to NowValue
	NowValue, err := strconv.Atoi(ValueOfKey)
	if err != nil {
		reply.Err = ValueCannotBeConvertedToNumber
		return nil
	}

	// When interval is zero, it defaults to a normal CAS operation, and no further attempts are made to decrement after comparison failure
	NewOperation := CASOp{Key: args.Key, ClientID: args.ClientID, Clientseq: args.SeqNo, Interval: args.New}

	// The old and new here are not necessarily correct; so why calculate here? The reason is to reduce the calculation in the critical section of the daemon
	if args.Flag&1 != 0 { // CAS
		NewOperation.Old = strconv.Itoa(args.Old)
		NewOperation.New = strconv.Itoa(args.New)
		NewOperation.Interval = 0 // Zero for CAS, args.new for other times
	} else if args.Flag&2 != 0 { // ADD
		rhs := args.Old
		if NowValue+args.New > rhs {
			reply.Err = CurrentValueExceedsExpectedValue
			return nil
		}
		NewOperation.Old = strconv.Itoa(NowValue)
		NewOperation.New = strconv.Itoa(NowValue + args.New)
		NewOperation.Boundary = rhs
	} else if args.Flag&4 != 0 { // SUB
		lhs := args.Old
		if NowValue-args.New < lhs {
			reply.Err = CurrentValueExceedsExpectedValue
			return nil
		}
		NewOperation.Old = strconv.Itoa(NowValue)
		NewOperation.New = strconv.Itoa(NowValue - args.New)
		NewOperation.Boundary = lhs
	} else { // Undefined behavior
		log.Printf("ERROR : [%d] CompareAndSwap exhibits undefined behavior.\n", kv.me)
		reply.Err = CASFlagUndefined
		return nil
	}

	// At this point, old and new are valid, but not necessarily ultimately valid. In the daemon, we first compare, CAS, and then check the boundary value for calculation if it fails, to ensure the validity of ADD and SUB

	Notice := make(chan struct{})

	log.Printf("INFO : ClientId[%d], CompareAndSwap:key(%s), oldvalue(%s), newvalue(%s)\n",
		args.ClientID, args.Key, NewOperation.Old, NewOperation.New)

	kv.mu.Lock()
	if dup, ok := kv.ClientSeqCache[int64(args.ClientID)]; ok {
		//log.Printf("DEBUG : args.SeqNo : %d , dup.Seq : %d\n", args.SeqNo, dup.Seq)
		if args.SeqNo <= dup.Seq {
			kv.mu.Unlock()

			log.Printf("WARNING : ClientId[%d], This CompareAndSwap(key(%s) SeqNumber(%d) dup.Seq(%d)) is repeated.\n",
				args.ClientID, args.Key, args.SeqNo, dup.Seq)

			reply.Err = Duplicate
			return nil
		}
	}

	index, term, _ := kv.rf.Start(NewOperation)
	//log.Printf("DEBUG client %d : index %d\n", kv.me, index)
	kv.LogIndexNotice[index] = Notice

	kv.mu.Unlock()

	reply.Err = OK

	<-Notice
	curTerm, isLeader := kv.rf.GetState()
	if !isLeader || term != curTerm {
		reply.Err = ReElection
		return nil
	}

	flag := <-kv.CASNotice[args.ClientID]

	if flag {
		reply.Err = OK // The update has been completed in the dictionary according to the received request
	} else {
		reply.Err = CASFailure // CAS failed for various reasons
	}

	return nil
}

/*
 * @brief: Provides a more feature-rich CAS operation, see the top of the file.
 * @notes: The operation requires that the value corresponding to the key must be a number, and there is actually no protection mechanism now, which means that the object of the CAS operation may be modified to a non-numeric value at any time, so there is no way.
 * TODO This is a consideration for permissions later.
 */
func (ck *Clerk) CompareAndSwap(Key string, Old int, New int, Flag int) bool {
	cnt := len(ck.servers)

	for {
		args := &CompareAndSwapArgs{ClientID: ck.ClientID, SeqNo: ck.seq, Key: Key, Old: Old, New: New, Flag: Flag}

		reply := new(CompareAndSwapReply)

		ck.leader %= cnt

		if atomic.LoadInt32(&((*ck.serversIsOk)[ck.leader])) == 0 {
			ck.leader++
			continue
		}

		replyArrival := make(chan bool, 1)

		go func() {
			err := ck.servers[ck.leader].Call("RaftKV.CompareAndSwap", args, reply)
			flag := true
			if err != nil {
				log.Printf("ERROR : Create call error, Find the cause as soon as possible -> (%s).\n", err.Error())
				flag = false
			}
			replyArrival <- flag
		}()

		select {
		case ok := <-replyArrival:
			if ok && (reply.Err == OK) {
				ck.seq++
				return true
			} else if reply.Err == CurrentValueExceedsExpectedValue || reply.Err == CASFlagUndefined ||
				reply.Err == ValueCannotBeConvertedToNumber || reply.Err == CASFailure || reply.Err == Duplicate {
				// The other end CAS failed, the error type is the return value
				// log.Printf("INFO : CAS error key(%s) old(%d) new(%d) -> [%s]\n", Key, Old, New, reply.Err)
				ck.seq++
				return false
			}

			ck.leader++

		case <-time.After(200 * time.Millisecond):
			ck.leader++
		}
	}
}
