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

/* Code comment are all encoded in UTF-8.*/

package BaseServer

import (
	"fmt"
	"log"
	"strconv"
	"time"
)

type KvOp struct {
	Key       string
	Value     string
	Op        string // Represents a single operation string such as Get, Put, Append, etc.
	ClientID  uint64 // Each client's ID
	Clientseq int    // The current operation number on this ClientID
}

type FileOp struct {
	Op        string // Represents a single operation string such as open, create, etc.
	ClientID  uint64 // Each client's ID
	Clientseq int    // The current operation number on this ClientID

	InstanceSeq uint64 // Instance sequence number for each request, used to determine if the request is expired
	Token       uint64 // Lock version number

	LockOrFileOrDeleteType int // Lock type or file type or delete type, as they won't be used together

	FileName string // Path name when opening, file name otherwise
	PathName string // Path name
	CheckSum uint64 // Checksum
	TimeOut  uint32 // Lock timeout parameter

	// TODO: Permission control bits, not used yet, as it's unclear what form the permissions should take
	ReadAcl   *[]uint64
	WriteAcl  *[]uint64
	ModifyAcl *[]uint64
}

// Used for CAS (Compare-And-Swap)
type CASOp struct {
	ClientID  uint64 // Each client's ID
	Clientseq int    // The current operation number on this ClientID

	Key      string
	Old      string // Using string for old and new values to avoid conversion in the daemon thread, reducing critical section overhead
	New      string
	Boundary int // When CAS comparison fails, perform boundary comparison, and if it can accommodate an interval, perform the operation.
	Interval int
}

/*
 * @brief: To receive data from raft, responsible for converting commands received from applyCh into values in the database
 * and notifying the request channel to return data to the client upon receiving the command.
 */
func (kv *RaftKV) acceptFromRaftDaemon() {
	for {
		select {
		case <-kv.shutdownCh:
			log.Printf("INFO : [%d] is shutting down actively.\n", kv.me)
			return
		case msg, ok := <-kv.applyCh:
			if ok {

				// Received a snapshot
				if msg.UseSnapshot {
					kv.mu.Lock()
					kv.readSnapshot(msg.Snapshot)
					// We need to persist here, otherwise, if it crashes before the snapshot is generated, the data will be lost
					kv.persisterSnapshot(msg.Index)
					kv.mu.Unlock()
					continue
				}
				if msg.Command == nil {
					log.Printf("ERROR : [%d] accepted a package, msg.Command is null.\n", kv.me)
				}
				if msg.Command != nil && msg.Index > kv.snapshotIndex {

					// This parameter is used to take the raft snapshot operation out of the critical section of kv.mu
					var IsSnapShot bool = false
					var IsNeedSnapShot bool = kv.isUpperThanMaxraftstate()

					if cmd, ok := msg.Command.(KvOp); ok {
						kv.mu.Lock()
						// Obviously, it is executed only when it is a new user or the new operation seq is greater than the value in ClientSeqCache
						if dup, ok := kv.ClientSeqCache[int64(cmd.ClientID)]; !ok || dup.Seq < cmd.Clientseq {
							switch cmd.Op {
							case "Get":
								// No need to care about the bool return value because ChubbyGoMapGet uses "" as the false return value, which meets our expectations
								value, _ := kv.KvDictionary.ChubbyGoMapGet(cmd.Key)
								kv.ClientSeqCache[int64(cmd.ClientID)] = &LatestReply{Seq: cmd.Clientseq,
									Value: value}
							case "Put":
								kv.KvDictionary.ChubbyGoMapSet(cmd.Key, cmd.Value)
								kv.ClientSeqCache[int64(cmd.ClientID)] = &LatestReply{Seq: cmd.Clientseq}
							case "Append":
								kv.KvDictionary.ChubbyGoMapSet(cmd.Key, cmd.Value)
								kv.ClientSeqCache[int64(cmd.ClientID)] = &LatestReply{Seq: cmd.Clientseq}

							default:
								log.Printf("ERROR : [%d] received an invalid cmd %v.\n", kv.me, cmd)
							}
						} else {
							// This situation occurs when multiple clients use the same ClientID
							log.Println("ERROR : Multiple clients have the same ID !")
						}
					} else if cmd, ok := msg.Command.(FileOp); ok {
						kv.mu.Lock()

						if dup, ok := kv.ClientSeqCache[int64(cmd.ClientID)]; !ok || dup.Seq < cmd.Clientseq {
							if !ok { // Obviously, this user is new, we create an unbuffered channel
								kv.ClientInstanceCheckSum[cmd.ClientID] = make(chan uint64, 3)
								kv.ClientInstanceSeq[cmd.ClientID] = make(chan uint64, 3)
							}

							switch cmd.Op {
							case "Open":
								kv.ClientSeqCache[int64(cmd.ClientID)] = &LatestReply{Seq: cmd.Clientseq}
								node, ok := RootFileOperation.pathToFileSystemNodePointer[cmd.PathName]
								if ok { // This is the path name; when the path of Open exists, open it and return InstanceSeq
									seq, chuckSum := node.Open(cmd.PathName) // Return the InstanceSeq of this file
									node.OpenReferenceCount++
									log.Printf("INFO : [%d] Open file(%s) success, instanceSeq is %d, checksum is %s.\n", kv.me, cmd.PathName, seq, chuckSum)
									kv.ClientInstanceSeq[cmd.ClientID] <- seq
									kv.ClientInstanceCheckSum[cmd.ClientID] <- chuckSum
								} else {
									// Zero protocol is an error value, used as a notification mechanism, zero in checksum and seq are error values
									kv.ClientInstanceSeq[cmd.ClientID] <- NoticeErrorValue
									kv.ClientInstanceCheckSum[cmd.ClientID] <- NoticeErrorValue
									log.Printf("INFO : [%d] Open Not find path(%s)!\n", kv.me, cmd.PathName)
								}
							case "Create":
								kv.ClientSeqCache[int64(cmd.ClientID)] = &LatestReply{Seq: cmd.Clientseq}
								node, ok := RootFileOperation.pathToFileSystemNodePointer[cmd.PathName]
								if ok {
									seq, checksum, err := node.Insert(cmd.InstanceSeq, cmd.LockOrFileOrDeleteType, cmd.FileName, nil, nil, nil)
									if err == nil {
										log.Printf("INFO : [%d] Create file(%s) success, instanceSeq is %d, checksum is %d.\n", kv.me, cmd.FileName, seq, checksum)
										kv.ClientInstanceSeq[cmd.ClientID] <- seq
										kv.ClientInstanceCheckSum[cmd.ClientID] <- checksum
										break
									} else {
										log.Printf("INFO : [%d] Create file(%s) failure, %s\n", kv.me, cmd.FileName, err.Error())
									}
								} else {
									log.Printf("INFO : [%d] Create Not find path(%s)!\n", kv.me, cmd.PathName)
								}
								// Merge the above two conditions
								kv.ClientInstanceSeq[cmd.ClientID] <- NoticeErrorValue
								kv.ClientInstanceCheckSum[cmd.ClientID] <- NoticeErrorValue

							case "Delete":
								kv.ClientSeqCache[int64(cmd.ClientID)] = &LatestReply{Seq: cmd.Clientseq}
								node, ok := RootFileOperation.pathToFileSystemNodePointer[cmd.PathName]
								if ok {
									err := node.Delete(cmd.InstanceSeq, cmd.FileName, cmd.LockOrFileOrDeleteType, cmd.CheckSum)
									if err == nil {
										log.Printf("INFO : [%d] Delete file(%s) success.\n", kv.me, cmd.FileName)
										kv.ClientInstanceSeq[cmd.ClientID] <- NoticeSuccess // Special case, we need a notification mechanism
										break
									} else {
										log.Printf("INFO : [%d] Delete file(%s) failure, %s.\n", kv.me, cmd.FileName, err.Error())
									}
								} else {
									log.Printf("INFO : [%d] Delete Not find path(%s)!\n", kv.me, cmd.PathName)
								}

								kv.ClientInstanceSeq[cmd.ClientID] <- NoticeErrorValue

							case "Acquire":
								kv.ClientSeqCache[int64(cmd.ClientID)] = &LatestReply{Seq: cmd.Clientseq}
								node, ok := RootFileOperation.pathToFileSystemNodePointer[cmd.PathName]
								if ok {
									token, err := node.Acquire(cmd.InstanceSeq, cmd.FileName, cmd.LockOrFileOrDeleteType, cmd.CheckSum)
									if err == nil {
										var LockTypeName string
										if cmd.LockOrFileOrDeleteType == WriteLock {
											LockTypeName = "WriteLock"
										} else {
											LockTypeName = "ReadLock"
										}
										kv.ClientInstanceSeq[cmd.ClientID] <- token
										if cmd.TimeOut > 0 {
											go func() {
												// TODO Here we should also consider the round-trip delay of the data packet and the degree of clock synchronization between the two parties, but a larger server side does not affect correctness
												time.Sleep(time.Duration(cmd.TimeOut*2) * time.Millisecond)
												// TODO Obviously there is a race condition here, we will talk about it later when we change the architecture here, this is a difficult point among difficult points because kv.mu has become a performance bottleneck
												// Obviously here we need the node of the directory and the token of the file
												PathNode, PathOk := RootFileOperation.pathToFileSystemNodePointer[cmd.PathName]
												fileNode, FileOk := RootFileOperation.pathToFileSystemNodePointer[cmd.PathName+"/"+cmd.FileName]
												if PathOk && FileOk {
													// Use the latest values to ensure successful deletion
													PathNode.Release(fileNode.instanceSeq, cmd.FileName, fileNode.tokenSeq, fileNode.checksum)
												} else {
													log.Println("ERROR : delay delete failure.")
												}
											}()
										}
										log.Printf("INFO : [%d] Acquire file(%s) success, locktype is %s.\n", kv.me, cmd.FileName, LockTypeName)
										break
									} else {
										log.Printf("INFO : [%d] Acquire error (%s) -> %s!\n", kv.me, cmd.PathName, err.Error())
									}
								}
								kv.ClientInstanceSeq[cmd.ClientID] <- NoticeErrorValue

							case "Release":
								kv.ClientSeqCache[int64(cmd.ClientID)] = &LatestReply{Seq: cmd.Clientseq}
								node, ok := RootFileOperation.pathToFileSystemNodePointer[cmd.PathName]
								if ok {
									log.Printf("DEBUG : Release token is %d\n", cmd.Token)
									err := node.Release(cmd.InstanceSeq, cmd.FileName, cmd.Token, cmd.CheckSum)
									if err == nil {
										kv.ClientInstanceSeq[cmd.ClientID] <- NoticeSuccess // Notification mechanism
										// Only look at the reference count of the read lock, if it was a read lock before, this is correct, if it is a write lock, it is also zero after Release, which is correct; if it is wrong, it will not enter here
										log.Printf("INFO : [%d] Release file(%s) success, this file reference count is %d\n", kv.me, cmd.FileName, node.readLockReferenceCount)
										break
									} else {
										log.Printf("INFO : [%d] Release error(%s) -> %s!\n", kv.me, cmd.PathName, err.Error())
									}
								}

								kv.ClientInstanceSeq[cmd.ClientID] <- NoticeErrorValue

							case "CheckToken":
								kv.ClientSeqCache[int64(cmd.ClientID)] = &LatestReply{Seq: cmd.Clientseq}
								node, ok := RootFileOperation.pathToFileSystemNodePointer[cmd.PathName]
								if ok {
									err := node.CheckToken(cmd.Token, cmd.FileName)
									if err == nil {
										kv.ClientInstanceSeq[cmd.ClientID] <- NoticeSuccess // Notification mechanism
										// Only look at the reference count of the read lock, if it was a read lock before, this is correct, if it is a write lock, it is also zero after Release, which is correct; if it is wrong, it will not enter here
										log.Printf("INFO : [%d] CheckToken file(%s) success, this file reference count is %d\n", kv.me, cmd.FileName, node.readLockReferenceCount)
										break
									} else {
										log.Printf("INFO : [%d] CheckToken error(%s) -> %s!\n", kv.me, cmd.PathName, err.Error())
									}
								}

								kv.ClientInstanceSeq[cmd.ClientID] <- NoticeErrorValue
							}
						} else {
							log.Println("ERROR : Multiple clients have the same ID !")
						}
					} else if cmd, ok := msg.Command.(CASOp); ok { // CAS operation
						kv.mu.Lock()
						if dup, ok := kv.ClientSeqCache[int64(cmd.ClientID)]; !ok || dup.Seq < cmd.Clientseq {
							if !ok {
								kv.CASNotice[cmd.ClientID] = make(chan bool, 3)
							}
							value, err := kv.KvDictionary.ChubbyGoMapGet(cmd.Key)
							if !err { // Query failed
								kv.CASNotice[cmd.ClientID] <- false
							} else {
								if value == cmd.Old {
									// Comparison succeeded, directly convert
									kv.KvDictionary.ChubbyGoMapSet(cmd.Key, cmd.New)
									kv.CASNotice[cmd.ClientID] <- true
								} else if cmd.Interval != 0 {
									nowValue, flag := strconv.Atoi(value)
									if flag == nil { // Conversion succeeded
										if cmd.Boundary <= nowValue-cmd.Interval {
											kv.KvDictionary.ChubbyGoMapSet(cmd.Key, strconv.Itoa(nowValue-cmd.Interval))
											kv.CASNotice[cmd.ClientID] <- true
										} else if cmd.Boundary >= nowValue+cmd.Interval {
											kv.KvDictionary.ChubbyGoMapSet(cmd.Key, strconv.Itoa(nowValue+cmd.Interval))
											kv.CASNotice[cmd.ClientID] <- true
										} else {
											kv.CASNotice[cmd.ClientID] <- false
										}
									} else {
										kv.CASNotice[cmd.ClientID] <- false
									}
								} else {
									kv.CASNotice[cmd.ClientID] <- false
								}
							}
						}

					}

					// msg.IsSnapshot && kv.isUpperThanMaxraftstate()
					if IsNeedSnapShot { // kv.isUpperThanMaxraftstate()
						log.Printf("INFO : [%d] need to create a snapshot. maxraftstate(%d), nowRaftStateSize(%d).\n",
							kv.me, kv.maxraftstate, kv.persist.RaftStateSize())
						kv.persisterSnapshot(msg.Index) // Data before this index has been packaged into a snapshot

					}

					// Notify the server operation
					if notifyCh, ok := kv.LogIndexNotice[msg.Index]; ok && notifyCh != nil {
						close(notifyCh)
						delete(kv.LogIndexNotice, msg.Index)
						fmt.Printf("Notice index(%d).\n", msg.Index)
					}

					kv.mu.Unlock()

					if IsSnapShot {
						kv.rf.CreateSnapshots(msg.Index)
					}
				}
			}
		}
	}
}

/*
 * @notes: Because maxraftstate in this function is constant, and RaftStateSize() is protected by the lock in persist, there is no need to put it in the critical section
 */
func (kv *RaftKV) isUpperThanMaxraftstate() bool {
	if kv.maxraftstate <= 0 { // Do not execute snapshot when less than or equal to zero
		return false
	}

	NowRaftStateSize := kv.persist.RaftStateSize()

	// The latter actually stores the size of the Raft state, which is maintained in the Raft library during each persistence
	if kv.maxraftstate < NowRaftStateSize {
		return true
	}
	// The above two are extreme cases, we need to consider persisting snapshots when approaching the critical value, tentatively 15%
	var interval = kv.maxraftstate - NowRaftStateSize

	if interval < kv.maxraftstate/20*3 {
		return true
	}

	return false
}
