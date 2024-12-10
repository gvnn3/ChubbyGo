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
)

type FileOperation struct {
	pathToFileSystemNodePointer map[string]*FileSystemNode
	root                        *FileSystemNode // Root node
	/*
	 * This item is actually considering that the ChubbyGo file system is a virtual file system;
	 * The parsing of the file name does not need to follow POSIX, and it is likely to use the URL as the file name, so an option to determine whether the URL is legal is added to the configuration file;
	 * This item will be used when inserting new files into the file system;
	 * PS: Currently, I do not plan to add this function, because '/' is a valid character in the URL, which will cause problems when using file descriptors,
	 * In fact, it is easy to solve, just add a field to mark it. But at present, this function is not urgent.
	 * TODO Add it later when needed
	 */
	//fileNameIsUri				bool
}

func InitFileOperation() *FileOperation {
	Root := &FileOperation{}

	Root.pathToFileSystemNodePointer = make(map[string]*FileSystemNode)

	Root.root = InitRoot()

	//log.Printf("DEBUG : Current pathname is %s\n", Root.root.nowPath)

	return Root
}

// If this is placed in Kvraft, fileSystem operations will be very troublesome
var RootFileOperation = InitFileOperation()

// There is a lot of duplicate code here, and the reason for not modifying it is that the types are different in many places. Reducing the number of lines of code requires reflection, which will reduce readability and performance;
func (kv *RaftKV) Open(args *OpenArgs, reply *OpenReply) error {

	if atomic.LoadInt32(kv.ConnectIsok) == 0 {
		reply.Err = ConnectError
		return nil
	}

	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = NoLeader
		return nil
	}

	NewOperation := FileOp{Op: "Open", ClientID: args.ClientID, Clientseq: args.SeqNo, PathName: args.PathName}

	Notice := make(chan struct{})

	log.Printf("INFO : ClientId[%d], Open : pathname(%s))\n", args.ClientID, args.PathName)

	kv.mu.Lock()
	if dup, ok := kv.ClientSeqCache[int64(args.ClientID)]; ok {
		//log.Printf("DEBUG : args.SeqNo : %d , dup.Seq : %d\n", args.SeqNo, dup.Seq)
		if args.SeqNo <= dup.Seq {
			kv.mu.Unlock()
			log.Printf("WARNING : ClientId[%d], This request(Open -> pathname(%s)) is repeated.\n", args.ClientID, args.PathName)
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

		reply.ChuckSum = <-kv.ClientInstanceCheckSum[args.ClientID]
		reply.InstanceSeq = <-kv.ClientInstanceSeq[args.ClientID]

		if reply.ChuckSum == NoticeErrorValue || reply.InstanceSeq == NoticeErrorValue {
			reply.Err = OpenError
		}

	case <-kv.shutdownCh:
		return nil
	}

	return nil
}

func (kv *RaftKV) Create(args *CreateArgs, reply *CreateReply) error {

	if atomic.LoadInt32(kv.ConnectIsok) == 0 {
		reply.Err = ConnectError
		return nil
	}

	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = NoLeader
		return nil
	}

	Notice := make(chan struct{})

	NewOperation := FileOp{Op: "Create", ClientID: args.ClientID, Clientseq: args.SeqNo,
		PathName: args.PathName, FileName: args.FileName, InstanceSeq: args.InstanceSeq, LockOrFileOrDeleteType: args.FileType}

	log.Printf("INFO : ClientId[%d], Create : pathname(%s) filename(%s)\n", args.ClientID, args.PathName, args.FileName)

	kv.mu.Lock()
	if dup, ok := kv.ClientSeqCache[int64(args.ClientID)]; ok {
		if args.SeqNo <= dup.Seq {
			kv.mu.Unlock()
			log.Printf("WARNING : ClientId[%d], This request(Create -> pathname(%s) filename(%s)) is repeated.\n", args.ClientID, args.PathName, args.FileName)
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

		reply.CheckSum = <-kv.ClientInstanceCheckSum[args.ClientID]
		reply.InstanceSeq = <-kv.ClientInstanceSeq[args.ClientID]

		if reply.CheckSum == NoticeErrorValue || reply.InstanceSeq == NoticeErrorValue {
			reply.Err = CreateError
		}

	case <-kv.shutdownCh:
		return nil
	}

	return nil
}

func (kv *RaftKV) Delete(args *CloseArgs, reply *CloseReply) error {

	if atomic.LoadInt32(kv.ConnectIsok) == 0 {
		reply.Err = ConnectError
		return nil
	}

	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = NoLeader
		return nil
	}

	NewOperation := FileOp{Op: "Delete", ClientID: args.ClientID, Clientseq: args.SeqNo,
		PathName: args.PathName, FileName: args.FileName, InstanceSeq: args.InstanceSeq,
		LockOrFileOrDeleteType: args.OpType, CheckSum: args.Checksum}

	log.Printf("INFO : ClientId[%d], Delete : pathname(%s) filename(%s) checksum(%d)\n", args.ClientID, args.PathName, args.FileName, args.Checksum)

	Notice := make(chan struct{})

	kv.mu.Lock()
	if dup, ok := kv.ClientSeqCache[int64(args.ClientID)]; ok {
		if args.SeqNo <= dup.Seq {
			kv.mu.Unlock()
			log.Printf("WARNING : ClientId[%d], This request(Delete -> pathname(%s)) is repeated.\n", args.ClientID, args.PathName)
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

		NoticeError := <-kv.ClientInstanceSeq[args.ClientID]

		// When the return value is NoticeSuccess, there is no problem
		if NoticeError == NoticeErrorValue {
			reply.Err = DeleteError
		}

	case <-kv.shutdownCh:
		return nil
	}

	return nil
}

func (kv *RaftKV) Acquire(args *AcquireArgs, reply *AcquireReply) error {

	if atomic.LoadInt32(kv.ConnectIsok) == 0 {
		reply.Err = ConnectError
		return nil
	}

	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = NoLeader
		return nil
	}

	NewOperation := FileOp{Op: "Acquire", ClientID: args.ClientID, Clientseq: args.SeqNo,
		PathName: args.PathName, FileName: args.FileName, InstanceSeq: args.InstanceSeq,
		LockOrFileOrDeleteType: args.LockType, CheckSum: args.Checksum, TimeOut: args.TimeOut}

	log.Printf("INFO : ClientId[%d], Acquire : pathname(%s) filename(%s)\n", args.ClientID, args.PathName, args.FileName)

	Notice := make(chan struct{})

	kv.mu.Lock()
	if dup, ok := kv.ClientSeqCache[int64(args.ClientID)]; ok {
		if args.SeqNo <= dup.Seq {
			kv.mu.Unlock()
			log.Printf("WARNING : ClientId[%d], This request(Acquire -> pathname(%s)) is repeated.\n", args.ClientID, args.PathName)
			reply.Err = Duplicate
			return nil
		}
	}

	index, term, _ := kv.rf.Start(NewOperation)

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

		reply.Token = <-kv.ClientInstanceSeq[args.ClientID]
		//log.Printf("DEBUG : lockserver Acquire token is %d.\n", reply.Token)

		// When the return value is 1, there is no problem
		if reply.Token == NoticeErrorValue {
			reply.Err = AcquireError
		}

	case <-kv.shutdownCh:
		return nil
	}

	return nil
}

func (kv *RaftKV) Release(args *ReleaseArgs, reply *ReleaseReply) error {

	if atomic.LoadInt32(kv.ConnectIsok) == 0 {
		reply.Err = ConnectError
		return nil
	}

	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = NoLeader
		return nil
	}

	NewOperation := FileOp{Op: "Release", ClientID: args.ClientID, Clientseq: args.SeqNo,
		PathName: args.PathName, FileName: args.FileName, InstanceSeq: args.InstanceSeq, Token: args.Token, CheckSum: args.CheckSum}

	log.Printf("INFO : ClientId[%d], Release : pathname(%s) filename(%s) token(%d)\n", args.ClientID, args.PathName, args.FileName, args.Token)

	Notice := make(chan struct{})

	kv.mu.Lock()
	if dup, ok := kv.ClientSeqCache[int64(args.ClientID)]; ok {
		if args.SeqNo <= dup.Seq {
			kv.mu.Unlock()
			log.Printf("WARNING : ClientId[%d], This request(Release -> pathname(%s)) is repeated.\n", args.ClientID, args.PathName)
			reply.Err = Duplicate
			return nil
		}
	}

	index, term, _ := kv.rf.Start(NewOperation)

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

		NoticeError := <-kv.ClientInstanceSeq[args.ClientID]

		// When the return value is 1, there is no problem
		if NoticeError == NoticeErrorValue {
			reply.Err = ReleaseError
		}

	case <-kv.shutdownCh:
		return nil
	}

	return nil
}

func (kv *RaftKV) CheckToken(args *CheckTokenArgs, reply *CheckTokenReply) error {

	if atomic.LoadInt32(kv.ConnectIsok) == 0 {
		reply.Err = ConnectError
		return nil
	}

	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = NoLeader
		return nil
	}

	NewOperation := FileOp{Op: "CheckToken", ClientID: args.ClientID, Clientseq: args.SeqNo,
		PathName: args.PathName, FileName: args.FileName, Token: args.Token}

	log.Printf("INFO : ClientId[%d], CheckToken : pathname(%s) filename(%s)\n", args.ClientID, args.PathName, args.FileName)

	Notice := make(chan struct{})

	kv.mu.Lock()
	if dup, ok := kv.ClientSeqCache[int64(args.ClientID)]; ok {
		if args.SeqNo <= dup.Seq {
			kv.mu.Unlock()
			log.Printf("WARNING : ClientId[%d], This request(CheckToken -> pathname(%s) filename(%s)) is repeated.\n", args.ClientID, args.PathName, args.FileName)
			reply.Err = Duplicate
			return nil
		}
	}

	index, term, _ := kv.rf.Start(NewOperation)

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

		NoticeError := <-kv.ClientInstanceSeq[args.ClientID]

		// When the return value is 1, there is no problem
		if NoticeError == NoticeErrorValue {
			reply.Err = CheckTokenError
		}

	case <-kv.shutdownCh:
		return nil
	}

	return nil
}
