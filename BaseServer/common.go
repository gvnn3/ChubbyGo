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

// TODO There is an opportunity to change all of these to numbers, as each comparison requires a string comparison, which is less efficient
const (
	OK         = "OK"
	ErrNoKey   = "ErrNoKey"
	NoLeader   = "NoLeader"
	Duplicate  = "Duplicate"
	ReElection = "ReElection"

	ConnectError = "ConnectError" // The most special item used to determine whether the remote server is successfully connected. If the remote server has not yet connected to all servers, do nothing.

	OpenError       = "OpenError"
	DeleteError     = "DeleteError"
	CreateError     = "CreateError"
	AcquireError    = "AcquireError"
	ReleaseError    = "ReleaseError"
	CheckTokenError = "CheckTokenError"

	ValueCannotBeConvertedToNumber   = "ValueCannotBeConvertedToNumber"
	CurrentValueExceedsExpectedValue = "CurrentValueExceedsExpectedValue"
	CASFlagUndefined                 = "CASFlagUndefined"
	CASFailure                       = "CASFailure"
)

// Used for Delete RPC
const (
	Opdelete = iota // Delete permanent file/directory when reference count is zero
	Opclose         // Do not delete permanent file/directory when reference count is zero
)

const (
	NoticeErrorValue = 0
	NoticeSuccess    = 1
)

type Err string

type PutAppendArgs struct {
	Key      string
	Value    string
	Op       string
	ClientID uint64
	SeqNo    int
}

type PutAppendReply struct {
	Err Err // Defines six errors to indicate state transitions
}

type GetArgs struct {
	Key      string
	ClientID uint64
	SeqNo    int
}

type GetReply struct {
	Err   Err
	Value string
}

type OpenArgs struct {
	ClientID uint64
	SeqNo    int
	PathName string
}

type OpenReply struct {
	Err         Err
	ChuckSum    uint64
	InstanceSeq uint64
}

/*
 * @brief: After each successful open, a file descriptor is returned for subsequent operations, making the abstraction very logical for users.
 * @notes: Currently, everything is made public for debugging convenience.
 */
type FileDescriptor struct {
	ChuckSum    uint64
	InstanceSeq uint64
	PathName    string
}

type CreateArgs struct {
	ClientID    uint64
	SeqNo       int
	InstanceSeq uint64
	FileType    int
	PathName    string // Use PathName to parse the node on the server
	FileName    string // Then use FileName to create the file
}

type CreateReply struct {
	Err         Err
	InstanceSeq uint64
	CheckSum    uint64
}

type CloseArgs struct {
	ClientID    uint64
	SeqNo       int
	InstanceSeq uint64
	PathName    string
	FileName    string
	OpType      int
	Checksum    uint64
}

type CloseReply struct {
	Err Err
}

type AcquireArgs struct {
	ClientID    uint64
	SeqNo       int
	InstanceSeq uint64
	PathName    string
	FileName    string
	LockType    int
	Checksum    uint64
	TimeOut     uint32 // In milliseconds
}

type AcquireReply struct {
	Err   Err
	Token uint64
}

type ReleaseArgs struct {
	ClientID    uint64
	SeqNo       int
	InstanceSeq uint64
	PathName    string
	FileName    string
	Token       uint64
	CheckSum    uint64
}

type ReleaseReply struct {
	Err Err
}

type CheckTokenArgs struct {
	ClientID uint64
	SeqNo    int
	Token    uint64
	PathName string
	FileName string
}

type CheckTokenReply struct {
	Err Err
}

type CompareAndSwapArgs struct {
	ClientID uint64
	SeqNo    int
	Key      string
	Old      int
	New      int
	Flag     int
}

type CompareAndSwapReply struct {
	Err Err
}

/*
 * @brief: Misalignment is very annoying, go Ctrl+Alt+L will automatically push the comments back; considering that URLs may be used as file names
 */
var isUrlEffective = [128]bool{
	/*0   nul    soh    stx    etx    eot    enq    ack    bel     7*/
	false, false, false, false, false, false, false, false,
	/*8   bs     ht     nl     vt     np     cr     so     si     15*/
	false, false, false, false, false, false, false, false,
	/*16  dle    dc1    dc2    dc3    dc4    nak    syn    etb    23*/
	false, false, false, false, false, false, false, false,
	/*24  can    em     sub    esc    fs     gs     rs     us     31*/
	false, false, false, false, false, false, false, false,
	/*32  ' '    !      "      #     $     %     &     '          39*/
	false, false, false, true, true, true, true, false,
	/*40  (      )      *      +     ,     -     .     /          47*/
	false, false, false, true, true, true, true, true,
	/*48  0     1     2     3     4     5     6     7             55*/
	true, true, true, true, true, true, true, true,
	/*56  8     9     :     ;     <      =     >      ?           63*/
	true, true, true, true, false, true, false, true,
	/*64  @     A     B     C     D     E     F     G             71*/
	true, true, true, true, true, true, true, true,
	/*72  H     I     J     K     L     M     N     O             79*/
	true, true, true, true, true, true, true, true,
	/*80  P     Q     R     S     T     U     V     W             87*/
	true, true, true, true, true, true, true, true,
	/*88  X     Y     Z     [      \      ]      ^      _         95*/
	true, true, true, false, false, false, false, true,
	/*96  `      a     b     c     d     e     f     g           103*/
	false, true, true, true, true, true, true, true,
	/*104 h     i     j     k     l     m     n     o            113*/
	true, true, true, true, true, true, true, true,
	/*112 p     q     r     s     t     u     v     w            119*/
	true, true, true, true, true, true, true, true,
	/*120 x     y     z     {      |      }      ~      del      127*/
	true, true, true, false, false, false, false, false,
}

/*
 * @brief: Determine if the file name has an error
 */
func isEffective(ch byte) bool {
	if ch < 0 { //|| ch > 127{ 	显然不太可能
		return false
	} else {
		return isUrlEffective[ch]
	}
}
