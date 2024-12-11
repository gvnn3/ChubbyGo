# Introduction
The selection of a thread-safe hashmap is a key point for performance improvement. There are three choices in front of us: BaseMap, sync.map, and concurrentMap.

1. **BaseMap**: There is only one lock in the map, and all operations go through this lock.
2. **sync.map**: A thread-safe hash map provided by Golang, implemented without locks.
3. **concurrentMap**: Each hash bucket has a lock, maximizing concurrency.

Generally, using the standard library directly will not cause too many problems. However, after briefly reviewing the implementation of sync.map, I believe this method is not efficient in cases with many writes because it involves many copies. For better performance, I tested the performance of these three solutions in different scenarios. I found that **BaseMap does not perform well in any situation, while the other two methods have their own advantages and disadvantages**. **Ultimately, I chose a version where customers can dynamically configure the selection of sync.map or concurrentMap in the configuration file based on different application conditions**. To achieve more elegant code and less space consumption, reflection is introduced to dynamically distinguish between these two types, which ultimately brings some efficiency loss but **increases flexibility**. I call it **ChubbyGoMap**.

Below is a performance comparison of the five types (BaseMap, sync.map, concurrentMap, and the two types of ChubbyGoMap) performing Put, Get, mixed insert and read, and delete operations, providing customers with the best choice for different scenarios:

The test code is located at /ChubbyGo/MapPerformanceTest/test_test.go:

Run the following command to execute the test:

```go
go test -v -run=^$ -bench . -benchmem
```

Explanation:

1. ns/op: **Average time spent per execution**;
2. b/op: **Total memory allocated on the heap per execution**;
3. allocs/op: **Number of memory allocations on the heap per execution**;

All data are averages of five tests.

## Put

|  | ns/op | b/op| allocs/op|
:-----:|:-----:|:-----:|:-----:|
BaseMap| 2505 | 205| 10
|ConcurrentMap|1257| 239|10
|SyncMap| 4546|500|32
|GoConcurrentMap|1838|423|20
|GoSyncMap|5096|662|42

## Get

|  | ns/op | b/op| allocs/op|
:-----:|:-----:|:-----:|:-----:|
BaseMap| 1654 | 0| 0
|ConcurrentMap|663| 77|10
|SyncMap| 533|4|1
|GoConcurrentMap|1048|76|10
|GoSyncMap|758|77|10

## Get&Put

|  | ns/op | b/op| allocs/op|
:-----:|:-----:|:-----:|:-----:|
BaseMap| 4565 | 266| 20
|ConcurrentMap|2926| 311|20
|SyncMap| 7149|836|42
|GoConcurrentMap|3021|477|30
|GoSyncMap|7275|938|52

## Delete

|  | ns/op | b/op| allocs/op|
:-----:|:-----:|:-----:|:-----:|
BaseMap| 4346 | 139| 19
|ConcurrentMap|1719| 152|19
|SyncMap| 7113|1012|48
|GoConcurrentMap|2634|305|29
|GoSyncMap|7272|1296|61

## Summary
From the above data, it can be seen that sync.map is inefficient in operations involving Put, but extremely efficient when there are many Get operations. This is easy to understand because the read operations in sync.map are completely lock-free. Interested friends can check the source code for more details.

Since the FastGet operation in ChubbyGo does not operate on ChubbyGoMap, combined with the above performance tests, we can conclude: **When using ChubbyGo, choose the SyncMap strategy for read-heavy and write-light scenarios, and use the ConcurrentMap strategy for other scenarios. This can be configured in the JSON configuration file**.
