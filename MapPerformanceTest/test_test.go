package main

import (
	"ChubbyGo/BaseServer"
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"
)

/*
 * @brief: This file is for performance comparison of Put, Get, concurrent insert and read mix, and delete operations
 * of BaseMap, sync.map, concurrentMap, and two types of ChubbyGoMap, providing the best choice for customers in different scenarios.
 */

/*
 * Loop count; average time spent per execution; total heap memory allocated per execution; number of heap allocations per execution;
 */

const ThreadNumber = 10

/*1.--------------Test Put operation--------------*/

// In this test, there are many conflicting keys, which is not beneficial for concurrent maps

// (BaseMap)
func BenchmarkPutKeyNoExist_BaseMap(b *testing.B) {
	Map := BaseServer.NewBaseMap()

	groups := sync.WaitGroup{}
	groups.Add(ThreadNumber)

	b.ResetTimer()

	for j := 0; j < ThreadNumber; j++ {
		go func() {
			for i := 0; i < b.N; i++ {
				Map.BaseStore(strconv.Itoa(i), "lizhaolong")
			}
			groups.Done()
		}()
	}
	groups.Wait()
}

// (ConcurrentMap)
func BenchmarkPutKeyNoExist_ConcurrentMap(b *testing.B) {
	Map := BaseServer.NewConcurrentMap()

	groups := sync.WaitGroup{}
	groups.Add(ThreadNumber)

	b.ResetTimer()

	for j := 0; j < ThreadNumber; j++ {
		go func() {
			for i := 0; i < b.N; i++ {
				Map.Set(strconv.Itoa(i), "lizhaolong")
			}
			groups.Done()
		}()
	}
	groups.Wait()
}

// (Sync.map)
func BenchmarkPutKeyNoExist_SyncMap(b *testing.B) {
	syncMap := &sync.Map{}

	groups := sync.WaitGroup{}
	groups.Add(ThreadNumber)

	b.ResetTimer()

	for j := 0; j < ThreadNumber; j++ {
		go func() {
			for i := 0; i < b.N; i++ {
				syncMap.Store(strconv.Itoa(i), "lizhaolong")
			}
			groups.Done()
		}()
	}
	groups.Wait()
}

// (chubbyGoMap.ConcurrentMap)
func BenchmarkPutKeyNoExist_ChubbyGo_ConcurrentMap(b *testing.B) {
	chubbyGoMap := BaseServer.NewChubbyGoMap(BaseServer.ConcurrentMap)

	groups := sync.WaitGroup{}
	groups.Add(ThreadNumber)

	b.ResetTimer()

	for j := 0; j < ThreadNumber; j++ {
		go func() {
			for i := 0; i < b.N; i++ {
				chubbyGoMap.ChubbyGoMapSet(strconv.Itoa(i), "lizhaolong")
			}
			groups.Done()
		}()
	}
	groups.Wait()
}

// (chubbyGoMap.Sync.map)
func BenchmarkPutKeyNoExist_ChubbyGo_SyncMap(b *testing.B) {
	chubbyGoMap := BaseServer.NewChubbyGoMap(BaseServer.SyncMap)

	groups := sync.WaitGroup{}
	groups.Add(ThreadNumber)

	b.ResetTimer()

	for j := 0; j < ThreadNumber; j++ {
		go func() {
			for i := 0; i < b.N; i++ {
				chubbyGoMap.ChubbyGoMapSet(strconv.Itoa(i), "lizhaolong")
			}
			groups.Done()
		}()
	}
	groups.Wait()
}

/*2.--------------Test Get operation--------------*/
// Read an existing key

// (BaseMap)
func BenchmarkGetKey_BaseMap(b *testing.B) {
	Map := BaseServer.NewBaseMap()

	groups := sync.WaitGroup{}
	groups.Add(ThreadNumber)

	rand.Seed(time.Now().Unix())
	// May generate (64,128) keys
	KeyNumber := rand.Intn(63)
	KeyNumber += 64
	for i := 0; i < KeyNumber; i++ {
		TempWord := "x " + strconv.Itoa(i) + " y"
		Map.BaseStore(TempWord, "lizhaolong")
	}

	b.ResetTimer()

	for j := 0; j < ThreadNumber; j++ {
		go func() {
			for i := 0; i < b.N; i++ {
				TempWord := "x " + strconv.Itoa(i%KeyNumber) + " y"
				Map.BaseGet(TempWord)
			}
			groups.Done()
		}()
	}
	groups.Wait()
}

// (ConcurrentMap)
func BenchmarkGetKey_ConcurrentMap(b *testing.B) {
	Map := BaseServer.NewConcurrentMap()

	groups := sync.WaitGroup{}
	groups.Add(ThreadNumber)

	rand.Seed(time.Now().Unix())
	// May generate (64,128) keys
	KeyNumber := rand.Intn(63)
	KeyNumber += 64
	for i := 0; i < KeyNumber; i++ {
		TempWord := "x " + strconv.Itoa(i) + " y"
		Map.Set(TempWord, "lizhaolong")
	}

	b.ResetTimer()

	for j := 0; j < ThreadNumber; j++ {
		go func() {
			for i := 0; i < b.N; i++ {
				TempWord := "x " + strconv.Itoa(i%KeyNumber) + " y"
				Map.Get(TempWord)
			}
			groups.Done()
		}()
	}
	groups.Wait()
}

// (Sync.map)
func BenchmarkGetKey_SyncMap(b *testing.B) {
	syncMap := &sync.Map{}

	groups := sync.WaitGroup{}
	groups.Add(ThreadNumber)

	rand.Seed(time.Now().Unix())
	// May generate (64,128) keys
	KeyNumber := rand.Intn(63)
	KeyNumber += 64
	for i := 0; i < KeyNumber; i++ {
		TempWord := "x " + strconv.Itoa(i) + " y"
		syncMap.Store(TempWord, "lizhaolong")
	}

	b.ResetTimer()

	for j := 0; j < ThreadNumber; j++ {
		go func() {
			for i := 0; i < b.N; i++ {
				TempWord := "x " + strconv.Itoa(i%KeyNumber) + " y"
				syncMap.Load(TempWord)
			}
			groups.Done()
		}()
	}
	groups.Wait()
}

// (chubbyGoMap.ConcurrentMap)
func BenchmarkGetKey_ChubbyGo_ConcurrentMap(b *testing.B) {
	chubbyGoMap := BaseServer.NewChubbyGoMap(BaseServer.ConcurrentMap)

	groups := sync.WaitGroup{}
	groups.Add(ThreadNumber)

	rand.Seed(time.Now().Unix())
	// May generate (64,128) keys
	KeyNumber := rand.Intn(63)
	KeyNumber += 64
	for i := 0; i < KeyNumber; i++ {
		TempWord := "x " + strconv.Itoa(i) + " y"
		chubbyGoMap.ChubbyGoMapSet(TempWord, "lizhaolong")
	}

	b.ResetTimer()

	for j := 0; j < ThreadNumber; j++ {
		go func() {
			for i := 0; i < b.N; i++ {
				TempWord := "x " + strconv.Itoa(i%KeyNumber) + " y"
				chubbyGoMap.ChubbyGoMapGet(TempWord)
			}
			groups.Done()
		}()
	}
	groups.Wait()
}

// (chubbyGoMap.Sync.map)
func BenchmarkGetKey_ChubbyGo_SyncMap(b *testing.B) {
	chubbyGoMap := BaseServer.NewChubbyGoMap(BaseServer.SyncMap)

	groups := sync.WaitGroup{}
	groups.Add(ThreadNumber)
	rand.Seed(time.Now().Unix())
	// May generate (64,128) keys
	KeyNumber := rand.Intn(63)
	KeyNumber += 64
	for i := 0; i < KeyNumber; i++ {
		TempWord := "x " + strconv.Itoa(i) + " y"
		chubbyGoMap.ChubbyGoMapSet(TempWord, "lizhaolong")
	}

	b.ResetTimer()

	for j := 0; j < ThreadNumber; j++ {
		go func() {
			for i := 0; i < b.N; i++ {
				TempWord := "x " + strconv.Itoa(i%KeyNumber) + " y"
				chubbyGoMap.ChubbyGoMapGet(TempWord)
			}
			groups.Done()
		}()
	}
	groups.Wait()
}

/*3.--------------Test concurrent Get and Put operations--------------*/

// (BaseMap)
func BenchmarkPutGet_BaseMap(b *testing.B) {
	Map := BaseServer.NewBaseMap()

	groups := sync.WaitGroup{}
	groups.Add(ThreadNumber)

	b.ResetTimer()

	for j := 0; j < ThreadNumber; j++ {
		go func() {
			for i := 0; i < b.N; i++ {
				Map.BaseStore(strconv.Itoa(i), "lizhaolong")
				Map.BaseGet(strconv.Itoa(i - 1))
			}
			groups.Done()
		}()
	}
	groups.Wait()
}

// (ConcurrentMap)
func BenchmarkPutGet_ConcurrentMap(b *testing.B) {
	Map := BaseServer.NewConcurrentMap()

	groups := sync.WaitGroup{}
	groups.Add(ThreadNumber)

	b.ResetTimer()

	for j := 0; j < ThreadNumber; j++ {
		go func() {
			for i := 0; i < b.N; i++ {
				Map.Set(strconv.Itoa(i), "lizhaolong")
				Map.Get(strconv.Itoa(i - 1))
			}
			groups.Done()
		}()
	}
	groups.Wait()
}

// (Sync.map)
func BenchmarkPutGet_SyncMap(b *testing.B) {
	syncMap := &sync.Map{}

	groups := sync.WaitGroup{}
	groups.Add(ThreadNumber)

	b.ResetTimer()

	for j := 0; j < ThreadNumber; j++ {
		go func() {
			for i := 0; i < b.N; i++ {
				syncMap.Store(strconv.Itoa(i), "lizhaolong")
				syncMap.Load(strconv.Itoa(i - 1))
			}
			groups.Done()
		}()
	}
	groups.Wait()
}

// (chubbyGoMap.ConcurrentMap)
func BenchmarkPutGet_ChubbyGo_ConcurrentMap(b *testing.B) {
	chubbyGoMap := BaseServer.NewChubbyGoMap(BaseServer.ConcurrentMap)

	groups := sync.WaitGroup{}
	groups.Add(ThreadNumber)

	b.ResetTimer()

	for j := 0; j < ThreadNumber; j++ {
		go func() {
			for i := 0; i < b.N; i++ {
				chubbyGoMap.ChubbyGoMapSet(strconv.Itoa(i), "lizhaolong")
				chubbyGoMap.ChubbyGoMapGet(strconv.Itoa(i - 1))
			}
			groups.Done()
		}()
	}
	groups.Wait()
}

// (chubbyGoMap.Sync.map)
func BenchmarkPutGet_ChubbyGo_SyncMap(b *testing.B) {
	chubbyGoMap := BaseServer.NewChubbyGoMap(BaseServer.SyncMap)

	groups := sync.WaitGroup{}
	groups.Add(ThreadNumber)

	b.ResetTimer()

	for j := 0; j < ThreadNumber; j++ {
		go func() {
			for i := 0; i < b.N; i++ {
				chubbyGoMap.ChubbyGoMapSet(strconv.Itoa(i), "lizhaolong")
				chubbyGoMap.ChubbyGoMapGet(strconv.Itoa(i - 1))
			}
			groups.Done()
		}()
	}
	groups.Wait()
}

/*3.--------------Test concurrent Delete operations--------------*/

func BenchmarkDelete_BaseMap(b *testing.B) {
	Map := BaseServer.NewBaseMap()

	groups := sync.WaitGroup{}
	groups.Add(ThreadNumber)

	b.ResetTimer()

	for j := 0; j < ThreadNumber; j++ {
		go func() {
			for i := 0; i < b.N; i++ {
				Map.BaseStore(strconv.Itoa(i), "lizhaolong")
				Map.BaseDelete(strconv.Itoa(i - 1))
			}
			groups.Done()
		}()
	}
	groups.Wait()
}

// (ConcurrentMap)
func BenchmarkDelete_ConcurrentMap(b *testing.B) {
	Map := BaseServer.NewConcurrentMap()

	groups := sync.WaitGroup{}
	groups.Add(ThreadNumber)

	b.ResetTimer()

	for j := 0; j < ThreadNumber; j++ {
		go func() {
			for i := 0; i < b.N; i++ {
				Map.Set(strconv.Itoa(i), "lizhaolong")
				Map.Remove(strconv.Itoa(i - 1))
			}
			groups.Done()
		}()
	}
	groups.Wait()
}

// (Sync.map)
func BenchmarkDelete_SyncMap(b *testing.B) {
	syncMap := &sync.Map{}

	groups := sync.WaitGroup{}
	groups.Add(ThreadNumber)

	b.ResetTimer()

	for j := 0; j < ThreadNumber; j++ {
		go func() {
			for i := 0; i < b.N; i++ {
				syncMap.Store(strconv.Itoa(i), "lizhaolong")
				syncMap.Delete(strconv.Itoa(i - 1))
			}
			groups.Done()
		}()
	}
	groups.Wait()
}

// (chubbyGoMap.ConcurrentMap)
func BenchmarkDelete_ChubbyGo_ConcurrentMap(b *testing.B) {
	chubbyGoMap := BaseServer.NewChubbyGoMap(BaseServer.ConcurrentMap)

	groups := sync.WaitGroup{}
	groups.Add(ThreadNumber)

	b.ResetTimer()

	for j := 0; j < ThreadNumber; j++ {
		go func() {
			for i := 0; i < b.N; i++ {
				chubbyGoMap.ChubbyGoMapSet(strconv.Itoa(i), "lizhaolong")
				chubbyGoMap.ChubbyGoMapDelete(strconv.Itoa(i - 1))
			}
			groups.Done()
		}()
	}
	groups.Wait()
}

// (chubbyGoMap.Sync.map)
func BenchmarkDelete_ChubbyGo_SyncMap(b *testing.B) {
	chubbyGoMap := BaseServer.NewChubbyGoMap(BaseServer.SyncMap)

	groups := sync.WaitGroup{}
	groups.Add(ThreadNumber)

	b.ResetTimer()

	for j := 0; j < ThreadNumber; j++ {
		go func() {
			for i := 0; i < b.N; i++ {
				chubbyGoMap.ChubbyGoMapSet(strconv.Itoa(i), "lizhaolong")
				chubbyGoMap.ChubbyGoMapDelete(strconv.Itoa(i - 1))
			}
			groups.Done()
		}()
	}
	groups.Wait()
}
