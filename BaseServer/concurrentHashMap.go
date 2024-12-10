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
 * The prototype of this code comes from: https://github.com/halfrost/Halfrost-Field/blob/master/contents/Go/go_map_bench_test/concurrent-map/concurrent_map.go
 * I modified the hash algorithm;
 */

package BaseServer

import (
	"encoding/json"
	"sync"
	"unsafe"

	"github.com/cespare/xxhash/v2"
)

/*
 * @notes: Rewriting it to prevent circular references
 */
func str2sbyte(s string) (b []byte) {
	*(*string)(unsafe.Pointer(&b)) = s                                                  // Assign the address of s to b
	*(*int)(unsafe.Pointer(uintptr(unsafe.Pointer(&b)) + 2*unsafe.Sizeof(&b))) = len(s) // Set the capacity to the length
	return
}

// TODO The number of buckets should be configurable. Because the performance of xxhash is excellent, more buckets are suitable for larger data.
var SHARD_COUNT = 32

type ConcurrentHashMap struct {
	ThreadSafeHashMap []*ConcurrentMapShared
	NowKeyNumber      uint64
	BucketNumber      uint32
}

type ConcurrentMapShared struct {
	items map[string]interface{}
	sync.RWMutex
}

/*
 * @brief: Create a new map
 */
func NewConcurrentMap() *ConcurrentHashMap {
	Map := ConcurrentHashMap{}
	Map.ThreadSafeHashMap = make([]*ConcurrentMapShared, SHARD_COUNT)
	for i := 0; i < SHARD_COUNT; i++ {
		Map.ThreadSafeHashMap[i] = &ConcurrentMapShared{items: make(map[string]interface{})}
	}
	Map.NowKeyNumber = 0
	Map.BucketNumber = uint32(SHARD_COUNT)

	return &Map
}

/*
 * @brief: Use the key to get the bucket in the hash table
 */
func (m ConcurrentHashMap) GetShard(key string) *ConcurrentMapShared {
	return m.ThreadSafeHashMap[uint(xxhash_key(key))%uint(m.BucketNumber)]
}

func (m ConcurrentHashMap) MSet(data map[string]interface{}) {
	for key, value := range data {
		shard := m.GetShard(key)
		shard.Lock()
		shard.items[key] = value
		shard.Unlock()
	}
}

// Sets the given value under the specified key.
func (m ConcurrentHashMap) Set(key string, value interface{}) {
	// Get map shard.
	shard := m.GetShard(key)
	shard.Lock()
	shard.items[key] = value
	shard.Unlock()
}

// Callback to return new element to be inserted into the map
// It is called while lock is held, therefore it MUST NOT
// try to access other keys in same map, as it can lead to deadlock since
// Go sync.RWLock is not reentrant
type UpsertCb func(exist bool, valueInMap interface{}, newValue interface{}) interface{}

// Insert or Update - updates existing element or inserts a new one using UpsertCb
func (m ConcurrentHashMap) Upsert(key string, value interface{}, cb UpsertCb) (res interface{}) {
	shard := m.GetShard(key)
	shard.Lock()
	v, ok := shard.items[key]
	res = cb(ok, v, value)
	shard.items[key] = res
	shard.Unlock()
	return res
}

// Sets the given value under the specified key if no value was associated with it.
func (m ConcurrentHashMap) SetIfAbsent(key string, value interface{}) bool {
	// Get map shard.
	shard := m.GetShard(key)
	shard.Lock()
	_, ok := shard.items[key]
	if !ok {
		shard.items[key] = value
	}
	shard.Unlock()
	return !ok
}

// Retrieves an element from map under given key.
func (m ConcurrentHashMap) Get(key string) (interface{}, bool) {
	// Get shard
	shard := m.GetShard(key)
	shard.RLock()
	// Get item from shard.
	val, ok := shard.items[key]
	shard.RUnlock()
	return val, ok
}

// Returns the number of elements within the map.
func (m ConcurrentHashMap) Count() uint64 {
	return m.NowKeyNumber
}

// Looks up an item under specified key
func (m ConcurrentHashMap) Has(key string) bool {
	// Get shard
	shard := m.GetShard(key)
	shard.RLock()
	// See if element is within shard.
	_, ok := shard.items[key]
	shard.RUnlock()
	return ok
}

// Removes an element from the map.
func (m ConcurrentHashMap) Remove(key string) {
	// Try to get shard.
	shard := m.GetShard(key)
	shard.Lock()
	delete(shard.items, key)
	shard.Unlock()
}

// Removes an element from the map and returns it
func (m ConcurrentHashMap) Pop(key string) (v interface{}, exists bool) {
	// Try to get shard.
	shard := m.GetShard(key)
	shard.Lock()
	v, exists = shard.items[key]
	delete(shard.items, key)
	shard.Unlock()
	return v, exists
}

// Checks if map is empty.
func (m ConcurrentHashMap) IsEmpty() bool {
	return m.Count() == 0
}

// Used by the Iter & IterBuffered functions to wrap two variables together over a channel,
type Tuple struct {
	Key string
	Val interface{}
}

// Returns an iterator which could be used in a for range loop.
//
// Deprecated: using IterBuffered() will get a better performance
func (m ConcurrentHashMap) Iter() <-chan Tuple {
	chans := snapshot(m)
	ch := make(chan Tuple)
	go fanIn(chans, ch)
	return ch
}

// Returns a buffered iterator which could be used in a for range loop.
func (m ConcurrentHashMap) IterBuffered() <-chan Tuple {
	chans := snapshot(m)
	total := 0
	for _, c := range chans {
		total += cap(c)
	}
	ch := make(chan Tuple, total)
	go fanIn(chans, ch)
	return ch
}

// Returns an array of channels that contains elements in each shard,
// which likely takes a snapshot of `m`.
// It returns once the size of each buffered channel is determined,
// before all the channels are populated using goroutines.
func snapshot(m ConcurrentHashMap) (chans []chan Tuple) {
	chans = make([]chan Tuple, SHARD_COUNT)
	wg := sync.WaitGroup{}
	wg.Add(SHARD_COUNT)
	// Foreach shard.
	for index, shard := range m.ThreadSafeHashMap {
		go func(index int, shard *ConcurrentMapShared) {
			// Foreach key, value pair.
			shard.RLock()
			chans[index] = make(chan Tuple, len(shard.items))
			wg.Done()
			for key, val := range shard.items {
				chans[index] <- Tuple{key, val}
			}
			shard.RUnlock()
			close(chans[index])
		}(index, shard)
	}
	wg.Wait()
	return chans
}

// fanIn reads elements from channels `chans` into channel `out`
func fanIn(chans []chan Tuple, out chan Tuple) {
	wg := sync.WaitGroup{}
	wg.Add(len(chans))
	for _, ch := range chans {
		go func(ch chan Tuple) {
			for t := range ch {
				out <- t
			}
			wg.Done()
		}(ch)
	}
	wg.Wait()
	close(out)
}

// Returns all items as map[string]interface{}
func (m ConcurrentHashMap) Items() map[string]interface{} {
	tmp := make(map[string]interface{})

	// Insert items to temporary map.
	for item := range m.IterBuffered() {
		tmp[item.Key] = item.Val
	}

	return tmp
}

// Iterator callback, called for every key, value found in
// maps. RLock is held for all calls for a given shard
// therefore callback sees consistent view of a shard,
// but not across the shards
type IterCb func(key string, v interface{})

// Callback based iterator, cheapest way to read
// all elements in a map.
func (m ConcurrentHashMap) IterCb(fn IterCb) {
	for idx := range m.ThreadSafeHashMap {
		shard := (m.ThreadSafeHashMap)[idx]
		shard.RLock()
		for key, value := range shard.items {
			fn(key, value)
		}
		shard.RUnlock()
	}
}

// Return all keys as []string
func (m ConcurrentHashMap) Keys() []string {
	count := m.Count()
	ch := make(chan string, count)
	go func() {
		// Traverse all shards.
		wg := sync.WaitGroup{}
		wg.Add(SHARD_COUNT)
		for _, shard := range m.ThreadSafeHashMap {
			go func(shard *ConcurrentMapShared) {
				// Traverse all key, value pairs.
				shard.RLock()
				for key := range shard.items {
					ch <- key
				}
				shard.RUnlock()
				wg.Done()
			}(shard)
		}
		wg.Wait()
		close(ch)
	}()

	// Generate keys array to store all keys
	keys := make([]string, 0, count)
	for k := range ch {
		keys = append(keys, k)
	}
	return keys
}

// Reveals ConcurrentMap "private" variables to json marshal.
func (m ConcurrentHashMap) MarshalJSON() ([]byte, error) {
	// Create a temporary map, which will hold all items spread across shards.
	tmp := make(map[string]interface{})

	// Insert items to temporary map.
	for item := range m.IterBuffered() {
		tmp[item.Key] = item.Val
	}
	return json.Marshal(tmp)
}

/*
 * @param: Extremely efficient hash algorithm
 */
func xxhash_key(key string) uint32 {
	xxh := xxhash.New()
	xxh.Write(str2sbyte(key))
	return uint32(xxh.Sum64())
}

/*
 * @brief: Used to compare with general ChubbyGoMap
 */
type BaseMap struct {
	sync.Mutex
	m map[string]interface{}
}

func NewBaseMap() *BaseMap {
	return &BaseMap{
		m: make(map[string]interface{}, 100),
	}
}

func (myMap *BaseMap) BaseStore(k string, v interface{}) {
	myMap.Lock()
	defer myMap.Unlock()
	myMap.m[k] = v
}

func (myMap *BaseMap) BaseGet(k string) interface{} {
	myMap.Lock()
	defer myMap.Unlock()
	if v, ok := myMap.m[k]; !ok {
		return -1
	} else {
		return v
	}
}

func (myMap *BaseMap) BaseDelete(k string) {
	myMap.Lock()
	defer myMap.Unlock()
	if _, ok := myMap.m[k]; !ok {
		return
	} else {
		delete(myMap.m, k)
	}
}
