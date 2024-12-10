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

package Persister

import (
	"io/ioutil"
	"log"
	"os"
	"sync"
	"time"
)

// Mimicking Redis AOF naming, but overall it looks more like RDB
const (
	Always   = iota // Every operation is flushed to disk, so no need for a daemon process
	Everysec        // Flush to disk every second, storing raftstate and snapshot in different files
	No              // No proactive flushing to disk
)

type Persister struct {
	mu        sync.Mutex // This lock is important because both raft and the daemon goroutine will operate on this struct, so it needs to be protected
	raftstate []byte
	snapshot  []byte

	// The persistence file name depends on the user, giving the user a high degree of freedom and facilitating testing
	SnapshotFileName    string // Snapshot persistence file name
	RaftstateFileName   string // Raft state persistence file name
	PersistenceStrategy int    // Corresponds to the three strategies
}

func MakePersister() *Persister {
	return &Persister{}
}

func (ps *Persister) Copy() *Persister {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	np := MakePersister()
	np.raftstate = ps.raftstate
	np.snapshot = ps.snapshot
	return np
}

func (ps *Persister) SaveRaftState(state []byte) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.raftstate = state

	// Strategy "always" means every operation is flushed to disk
	if ps.PersistenceStrategy == Always {
		WriteContentToFile(ps.raftstate, ps.RaftstateFileName, ps.PersistenceStrategy)
	}
}

func (ps *Persister) ReadRaftState() []byte {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return ps.raftstate
}

/*
 * @brief: Read RaftState from raftstate.hdb
 * @return: Returns the file content as []byte
 */
func (ps *Persister) ReadRaftStateFromFile() []byte {
	content, err := ReadContentFromFile(ps.RaftstateFileName)
	if err != nil {
		log.Println("ERROR : ", err.Error())
		return []byte{}
	}
	return content
}

func (ps *Persister) RaftStateSize() int {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return len(ps.raftstate)
}

// Save both Raft state and K/V snapshot as a single atomic action,
// to help avoid them getting out of sync.
func (ps *Persister) SaveStateAndSnapshot(state []byte, snapshot []byte) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.raftstate = state
	ps.snapshot = snapshot
}

func (ps *Persister) SaveSnapshot(snapshot []byte) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.snapshot = snapshot

	// Strategy "always" means every operation is flushed to disk
	if ps.PersistenceStrategy == Always {
		WriteContentToFile(ps.snapshot, ps.SnapshotFileName, ps.PersistenceStrategy)
	}
}

func (ps *Persister) ReadSnapshot() []byte {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return ps.snapshot
}

func (ps *Persister) SnapshotSize() int {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return len(ps.snapshot)
}

/*
 * @brief: Read the snapshot file based on SnapshotFileName
 */
func (ps *Persister) ReadSnapshotFromFile() []byte {
	content, err := ReadContentFromFile(ps.SnapshotFileName)
	if err != nil {
		log.Println("ERROR : ", err.Error())
		return []byte{}
	}
	return content
}

// Considering that the snapshot and basic configuration are placed in two files,
// the following two functions are not made member functions, which can reduce code duplication

/*
 * @brief: Write data to a file
 * @return: Returns an error type
 * @notes: The current persistence strategy is tentative, following the selected strategy to flush to disk.
 * A daemon goroutine is responsible for flushing to disk according to the strategy; both "no" and "everysec" will enter, "everysec" will flush to disk.
 */
// TODO Currently need to choose the persistence strategy and storage file name in json,
// remember to check the operating system's restrictions on file and folder names
// Need to run a daemon goroutine at the start of raft, responsible for periodic flushing to disk
// And read the file name when the server parses it, pass the data to persist, and persist pass it to Raft
func WriteContentToFile(vals []byte, outfile string, strategy int) error {
	file, err := os.Create(outfile)
	if err != nil {
		log.Println(err.Error())
		return err
	}

	number, errW := file.Write(vals)
	if errW != nil || number < len(vals) {
		log.Println(errW.Error())
		return errW
	}

	// TODO What to do if close fails? In what situations will close errors occur?
	errC := file.Close()
	if errC != nil {
		log.Println(errC.Error())
		return errC
	}

	// Call fsync to flush to disk
	if strategy == Everysec { //|| strategy == Always {	Modified to not start goroutine for Always
		file.Sync()
	}

	return nil
}

/*
 * @brief: Read data from a file
 * @return: Returns the file content as []byte and an error type
 * @notes: When using this function, you must first check err before using []byte.
 * It is called when the server starts, first reading the two files, putting them into persister,
 * and then passing them to Raft, which will automatically read them at startup.
 */
func ReadContentFromFile(filepath string) ([]byte, error) {
	var NULL []byte
	// Open the file
	fi, err := os.Open(filepath)
	if err != nil {
		log.Println(err.Error())
		return NULL, err
	}

	// Read the content
	res, errR := ioutil.ReadAll(fi)
	if err != nil {
		log.Println(errR.Error())
		return res, errR
	}

	errC := fi.Close()
	if errC != nil {
		log.Println(errC.Error())
		return res, errC
	}
	// No errors, exit directly
	return res, nil
}

/*
 * @brief: Responsible for persistence according to the strategy,
 * in fact, this daemon goroutine only executes the everysec strategy
 */
func PersisterDaemon(per *Persister) {
	for {

		per.mu.Lock() // Protect per.raftstate

		// Not only passing a per is to make this function reusable
		errRaftState := WriteContentToFile(per.raftstate, per.RaftstateFileName, per.PersistenceStrategy)
		per.mu.Unlock()

		if errRaftState != nil {
			log.Println("WARNING : ", errRaftState.Error())
		}
		per.mu.Lock() // Protect per.snapshot
		// log.Printf("DEBUG : snapshot length == %d\n", len(per.snapshot))	// Check if snapshot file size is 0 after crash restart
		errSnapshot := WriteContentToFile(per.snapshot, per.SnapshotFileName, per.PersistenceStrategy)
		per.mu.Unlock()

		if errSnapshot != nil {
			log.Println("WARNING : ", errRaftState.Error())
		}

		time.Sleep(1 * time.Second) // Persist every second
	}
}
