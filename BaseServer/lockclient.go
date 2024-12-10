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
 * @brief: The path of the file to be opened; open is just to create a file in a directory, it has nothing to do with lock permissions
 * @return: Returns a file descriptor
 * @notes: Obviously opening a file is meaningless, there is a lock for file operations, and for content operations, just use the absolute path as the key to get it directly
 */
func (ck *Clerk) Open(pathname string) (bool, *FileDescriptor) {
	cnt := len(ck.servers)

	for {
		args := &OpenArgs{PathName: pathname, ClientID: ck.ClientID, SeqNo: ck.seq}
		reply := new(OpenReply)

		ck.leader %= cnt

		if atomic.LoadInt32(&((*ck.serversIsOk)[ck.leader])) == 0 {
			ck.leader++ // Switch if cannot connect
			continue
		}

		replyArrival := make(chan bool, 1)
		go func() {
			err := ck.servers[ck.leader].Call("RaftKV.Open", args, reply)
			flag := true
			if err != nil {
				log.Printf("ERROR : Open call error, Find the cause as soon as possible -> (%s).\n", err.Error())
				flag = false
			}
			replyArrival <- flag
		}()
		select {
		case <-time.After(200 * time.Millisecond): // rpc timeout: 200ms
			ck.leader++
			continue
		case ok := <-replyArrival:
			if ok && (reply.Err == OK) {
				ck.seq++
				return true, &FileDescriptor{reply.ChuckSum, reply.InstanceSeq, pathname}
			} else if reply.Err == OpenError || reply.Err == Duplicate {
				// Failed to open file on the other side
				log.Printf("INFO : Open file(%s) error -> [%s]\n", pathname, reply.Err)
				ck.seq++
				return false, nil
			}
			ck.leader++
		}
	}
}

/*
 * @brief: Create a file under this file descriptor, there are three types: directory, temporary file, file
 * @param: Instance number and path name come from the file descriptor; file type; file name
 * @return: Returns whether the file was created successfully
 * @notes: For the return value, first judge the bool value and then judge the seq, if bool is false, seq is meaningless
 */
func (ck *Clerk) Create(fd *FileDescriptor, Type int, filename string) (bool, *FileDescriptor) {
	cnt := len(ck.servers)

	for {
		args := &CreateArgs{PathName: fd.PathName, ClientID: ck.ClientID, SeqNo: ck.seq,
			InstanceSeq: fd.InstanceSeq, FileType: Type, FileName: filename}

		reply := new(CreateReply)

		ck.leader %= cnt

		if atomic.LoadInt32(&((*ck.serversIsOk)[ck.leader])) == 0 {
			ck.leader++ // Switch if cannot connect
			continue
		}

		replyArrival := make(chan bool, 1)
		go func() {
			err := ck.servers[ck.leader].Call("RaftKV.Create", args, reply)
			flag := true
			if err != nil {
				log.Printf("ERROR : Create call error, Find the cause as soon as possible -> (%s).\n", err.Error())
				flag = false
			}
			replyArrival <- flag
		}()
		select {
		case <-time.After(200 * time.Millisecond): // rpc timeout: 200ms
			ck.leader++
			continue
		case ok := <-replyArrival:
			if ok && (reply.Err == OK) {
				ck.seq++
				return true, &FileDescriptor{reply.CheckSum, reply.InstanceSeq, fd.PathName + "/" + filename}
			} else if reply.Err == CreateError || reply.Err == Duplicate {
				// Failed to create file on the other side
				log.Printf("INFO : Create (%s/%s) error -> [%s]\n", fd.PathName, filename, reply.Err)
				ck.seq++
				return false, nil
			}
			ck.leader++
		}
	}
}

/*
 * @param: opType is the operation type, it can be delete or close
 */
func (ck *Clerk) Delete(pathname string, filename string, instanceseq uint64, opType int, checkSum uint64) bool {
	cnt := len(ck.servers)

	for {
		args := &CloseArgs{PathName: pathname, ClientID: ck.ClientID, SeqNo: ck.seq,
			InstanceSeq: instanceseq, FileName: filename, OpType: opType, Checksum: checkSum}

		//log.Printf("DEBUG : args.checkSum %d.\n", args.Checksum)

		reply := new(CloseReply)

		ck.leader %= cnt

		if atomic.LoadInt32(&((*ck.serversIsOk)[ck.leader])) == 0 {
			ck.leader++ // Switch if cannot connect
			continue
		}

		replyArrival := make(chan bool, 1)

		go func() {
			err := ck.servers[ck.leader].Call("RaftKV.Delete", args, reply)
			flag := true
			if err != nil {
				log.Printf("ERROR : Delete call error, Find the cause as soon as possible -> (%s).\n", err.Error())
				flag = false
			}
			replyArrival <- flag
		}()
		select {
		case <-time.After(200 * time.Millisecond): // rpc timeout: 200ms
			ck.leader++
			continue
		case ok := <-replyArrival:
			if ok && (reply.Err == OK) {
				ck.seq++
				return true
			} else if reply.Err == DeleteError || reply.Err == Duplicate {
				log.Printf("INFO : Delete (%s/%s) error -> [%s]\n", pathname, filename, reply.Err)
				ck.seq++
				return false
			}
			ck.leader++
		}
	}
}

/*
 * @brief: Lock the Filename under the Fd directory, you can add a read lock or a write lock, no need to open to lock
 * @param: Instance number and path name come from the file descriptor; file type; file name
 * @return: Returns whether the lock was successful;
 */
func (ck *Clerk) Acquire(pathname string, filename string, instanceseq uint64, LockType int, checksum uint64, timeout uint32) (bool, uint64) {
	cnt := len(ck.servers)

	for {
		args := &AcquireArgs{PathName: pathname, ClientID: ck.ClientID, SeqNo: ck.seq,
			InstanceSeq: instanceseq, FileName: filename, LockType: LockType, Checksum: checksum, TimeOut: timeout}

		reply := new(AcquireReply)

		ck.leader %= cnt

		if atomic.LoadInt32(&((*ck.serversIsOk)[ck.leader])) == 0 {
			ck.leader++ // Switch if cannot connect
			continue
		}

		replyArrival := make(chan bool, 1)
		go func() {
			err := ck.servers[ck.leader].Call("RaftKV.Acquire", args, reply)
			flag := true
			if err != nil { // TODO The client has a huge problem, there is no disconnection reconnection mechanism
				log.Printf("ERROR : Acquire call error, Find the cause as soon as possible -> (%s).\n", err.Error())
				flag = false
			}
			replyArrival <- flag
		}()
		select {
		case <-time.After(200 * time.Millisecond): // rpc timeout: 200ms
			ck.leader++
			continue
		case ok := <-replyArrival:
			if ok && (reply.Err == OK) {
				ck.seq++
				log.Printf("Acquire success, token is %d.\n", reply.Token)
				return true, reply.Token
			} else if reply.Err == AcquireError || reply.Err == Duplicate {
				// Failed to lock file on the other side
				log.Printf("INFO : Acquire (%s/%s) error -> [%s]\n", pathname, filename, reply.Err)
				ck.seq++
				return false, 0
			}
			ck.leader++
		}
	}
}

/*
 * @brief: Unlock a specific file, the token is needed because it marks the version of the lock, preventing a lock from being unlocked outside its defined timeout range, thereby unlocking a lock held by another node
 * @param: Path name and file name come from the file descriptor; instance number; token number
 * @return: Returns whether the unlock was successful;
 */
func (ck *Clerk) Release(pathname string, filename string, instanceseq uint64, token uint64, checksum uint64) bool {
	cnt := len(ck.servers)

	for {
		args := &ReleaseArgs{PathName: pathname, ClientID: ck.ClientID, SeqNo: ck.seq,
			InstanceSeq: instanceseq, FileName: filename, Token: token, CheckSum: checksum}

		log.Printf("DEBUG : Release client token is %d.\n", token)

		reply := new(ReleaseReply)

		ck.leader %= cnt

		if atomic.LoadInt32(&((*ck.serversIsOk)[ck.leader])) == 0 {
			ck.leader++
			continue
		}

		replyArrival := make(chan bool, 1)
		go func() {
			err := ck.servers[ck.leader].Call("RaftKV.Release", args, reply)
			flag := true
			if err != nil {
				log.Printf("ERROR : Release call error, Find the cause as soon as possible -> (%s).\n", err.Error())
				flag = false
			}
			replyArrival <- flag
		}()
		select {
		case <-time.After(200 * time.Millisecond):
			ck.leader++
			continue
		case ok := <-replyArrival:
			if ok && (reply.Err == OK) {
				ck.seq++
				return true
			} else if reply.Err == ReleaseError || reply.Err == Duplicate {
				log.Printf("INFO : Release (%s/%s) error -> [%s]\n", pathname, filename, reply.Err)
				ck.seq++
				return false
			}
			ck.leader++
		}
	}
}

/*
 * @brief: Attach the currently held token to check if the token is still valid
 * @notes: The key issue is to check the return invalid, in fact, it is valid at the moment of sending the data, but it does not affect correctness
 */
func (ck *Clerk) CheckToken(pathname string, filename string, token uint64) bool {
	cnt := len(ck.servers)

	for {
		args := &CheckTokenArgs{PathName: pathname, ClientID: ck.ClientID, SeqNo: ck.seq,
			FileName: filename, Token: token}

		reply := new(CheckTokenReply)

		ck.leader %= cnt

		if atomic.LoadInt32(&((*ck.serversIsOk)[ck.leader])) == 0 {
			ck.leader++
			continue
		}

		replyArrival := make(chan bool, 1)
		go func() {
			err := ck.servers[ck.leader].Call("RaftKV.CheckToken", args, reply)
			flag := true
			if err != nil {
				log.Printf("ERROR : CheckToken call error, Find the cause as soon as possible -> (%s).\n", err.Error())
				flag = false
			}
			replyArrival <- flag
		}()
		select {
		case <-time.After(200 * time.Millisecond):
			ck.leader++
			continue
		case ok := <-replyArrival:
			if ok && (reply.Err == OK) {
				ck.seq++
				return true
			} else if reply.Err == CheckTokenError || reply.Err == Duplicate {
				log.Printf("INFO : CheckToken (%s/%s) error -> [%s]\n", pathname, filename, reply.Err)
				ck.seq++
				return false
			}
			ck.leader++
		}
	}
}
