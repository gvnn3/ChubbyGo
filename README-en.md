# ChubbyGo

![avatar](Pictures/ChubbyGo.jpg)
This cute Totoro's name is Go, and he may be a friend of Tux!

## 1. Description

To run ChubbyGo, you need to include the following third-party libraries:
1. github.com/sony/sonyflake
2. github.com/OneOfOne/xxhash

For Redis test code, you need to include the following third-party library:
1. github.com/redis/hiredis

---
1. ChubbyGo.pdf is the architecture diagram of ChubbyGo.
2. LockServer.pdf is the architecture diagram of the ChubbyGo lock service.

## 2. Introduction

ChubbyGo is a distributed lock service based on the **Raft protocol**, providing **coarse-grained locking** and **reliable storage** in loosely coupled distributed systems.

You may know that Chubby is actually a service described in Google's 2006 paper "The Chubby lock service for loosely-coupled distributed systems", but it is closed-source. Later, Yahoo Research donated a similar project to the Apache Foundation, named ZooKeeper, which officially became an Apache top-level project in November 2010.

ZooKeeper provides a higher level of abstraction, implementing a distributed coordination center, giving clients more freedom but also bringing complexity. For simpler usage, I implemented Chubby based on the paper and named it **ChubbyGo**.

ChubbyGo, like Chubby, aims to provide easy-to-understand semantics and small-scale reliable storage for medium-sized clients, with performance not being the main consideration. You can use ChubbyGo to accomplish the following tasks:
1. **Provide reliable leader election for multiple servers**, as at most one server can obtain the lock.
2. **Provide distributed lock service for resources that need protection across multiple hosts**.
3. **Allow the master to publish messages on ChubbyGo, and clients can use get(key) to get the message**, with the key being user-defined. It is recommended to use the path where the master obtains the lock as the key to ensure uniqueness.
4. It can store a small amount of information itself, so it can serve as a **nameserver**.
5. It provides get/set interfaces itself, so it can serve as a **reliable key-value server**.

## 3. Implementation
Due to my limited energy and skills, I am temporarily unable to implement all the details in the paper, and some places have chosen different strategies from the paper for a simpler and more straightforward implementation. This section is divided into four parts:
1. **Differences from the current design of Chubby**;
2. **Work that has been completed so far**;
3. **Problems that need to be modified and unimplemented functions that have been discovered**;
4. **Valid behaviors defined in the ChubbyGo file system**;

## 3. Implementation
Due to my limited energy and skills, I am temporarily unable to implement all the details in the paper, and some places have chosen different strategies from the paper for a simpler and more straightforward implementation. This section is divided into four parts:
1. **Differences from the current design of Chubby**;
2. **Work that has been completed so far**;
3. **Problems that need to be modified and unimplemented functions that have been discovered**;
4. **Valid behaviors defined in the ChubbyGo file system**;

### 3.1 Design Differences
1. Chubby uses the Paxos protocol, while ChubbyGo uses the Raft protocol.
2. Chubby uses a hierarchical namespace, while ChubbyGo uses a flat namespace.
3. Chubby supports fine-grained locking, while ChubbyGo supports coarse-grained locking.
4. Chubby supports ACLs (Access Control Lists), while ChubbyGo does not support ACLs.
5. Chubby supports session leases, while ChubbyGo does not support session leases.

### 3.2 Completed Work
1. Implemented the basic Raft protocol.
2. Implemented the basic lock service.
3. Implemented the basic key-value store.
4. Implemented the basic leader election.
5. Implemented the basic client-server communication.

### 3.3 Problems and Unimplemented Functions
1. The snapshot mechanism is not fully implemented.
2. The log compaction mechanism is not fully implemented.
3. The failure recovery mechanism is not fully implemented.
4. The performance optimization is not fully implemented.
5. The security mechanism is not fully implemented.

### 3.4 Valid Behaviors in ChubbyGo File System
1. Create a new file.
2. Delete a file.
3. Read a file.
4. Write to a file.
5. Lock a file.
6. Unlock a file.

## 4. Configuration File Description

### 4.1 server_config.json
1. **servers_address**: Addresses of peer servers.
2. **myport**: The port of the current server.
3. **maxreries**: Maximum number of retries for server startup.
4. **timeout_entry**: Interval time for each startup attempt, in milliseconds.
5. **maxraftstate**: Maximum total bytes for log storage. Log compression occurs when the threshold reaches 85%.
6. **snapshotfilename**: Filename for the snapshot.
7. **raftstatefilename**: Filename for Raft state persistence.
8. **persistencestrategy**: Persistence strategy, with three options: "always", "everysec", "no".
9. **chubbygomapstrategy**: Strategy for thread-safe hash map, with two options: "concurrentmap", "syncmap".

### 4.2 client_config.json
1. **client_address**: Addresses of peer servers.
2. **maxreries**: Maximum number of retries for the client to connect to more than half of the servers.

## 5. Deployment
The addresses of peer servers must be known among multiple servers, and this information should be written in the **"ChubbyGo/Config/server_config.json"** file under the **"servers_address"** field. The client should write the addresses of these N servers in the **"client_address"** field of the **"ChubbyGo/Config/client_config.json"** file. **N is recommended to be an odd number**.

Of course, other parameters can also be configured, as detailed in Section 4.

We use three processes to simulate three servers and one process to simulate a client, ensuring that ports 8900, 8901, and 8902 are not in use.

Server process A modifies the configuration file as follows:
```json
{
  "servers_address": ["localhost:8900","localhost:8901"],
  "myport" : ":8902",
  "maxreries" : 13,
  "timeout_entry" : 200,
  "maxraftstate" : 1000000,
  "snapshotfilename" : "Persister/snapshot1.hdb",
  "raftstatefilename" : "Persister/raftstate1.hdb",
  "persistencestrategy" : "everysec",
  "chubbygomapstrategy" : "concurrentmap"
}

Server process B modifies the configuration file as follows:

{
  "servers_address": ["localhost:8900","localhost:8902"],
  "myport" : ":8901",
  "maxreries" : 13,
  "timeout_entry" : 200,
  "maxraftstate" : 1000000,
  "snapshotfilename" : "Persister/snapshot2.hdb",
  "raftstatefilename" : "Persister/raftstate2.hdb",
  "persistencestrategy" : "everysec",
  "chubbygomapstrategy" : "concurrentmap"
}
Server process C modifies the configuration file as follows:

{
  "servers_address": ["localhost:8901","localhost:8902"],
  "myport" : ":8900",
  "maxreries" : 13,
  "timeout_entry" : 200,
  "maxraftstate" : 1000000,
  "snapshotfilename" : "Persister/snapshot3.hdb",
  "raftstatefilename" : "Persister/raftstate3.hdb",
  "persistencestrategy" : "everysec",
  "chubbygomapstrategy" : "concurrentmap"
}



## 6. Testing
The current test code is divided into several parts:
1. Performance comparison of BaseMap, Sync.Map, and ConcurrentMap, explaining the reason for ultimately choosing ChubbyGoMap, which provides the greatest flexibility with optimal performance.
2. Definition of three test files.
3. Performance comparison with Redis and ZooKeeper, mainly focusing on the response time for 1000 transactions requested by multiple client threads.

### 6.1 MapPerformanceTest
The test code is located at:
```shell
ChubbyGo/MapPerformanceTest/test_test.go

Execute the following command to run the test file:



cd MapPerformanceTest 
go test -v -run=^$ -bench . -benchmem
For detailed content, see:


[README.md](http://_vscodecontentref_/0)

## 6.2 Functional Testing
client_base.go: Concurrently executes get/set operations. You can configure the number of concurrent goroutines and choose between Get or FastGet.
lock_base.go: Executes all basic lock operations.
lock_expand.go: Concurrently requests read locks and write locks.
CAS_base.go: Concurrently executes three types of CAS (Compare-And-Swap) operations.

## 6.3 Performance Testing
ChubbyGo consists of three nodes: one Leader and two Followers.
Redis consists of four nodes: one Sentinel, one Leader, and two Followers.
ChubbyGo:

Threads	Total Requests	Total Time/ms	TPS	RT/ms
10	1000	2707.629	370	2.70
```				
## 7. Others
Here are some of my thoughts on ChubbyGo, including security validation and future prospects:
1. [《Using ChubbyGo! Join in ChubbyGo!》](https://blog.csdn.net/weixin_43705457/article/details/109446869)
2. [《Security Validation and Prospects of ChubbyGo》](https://blog.csdn.net/weixin_43705457/article/details/109458119)

## 8. References
1. "The Chubby lock service for loosely-coupled distributed systems"
2. "In Search of an Understandable Consensus Algorithm (Extended Version)"
3. "ZooKeeper: Wait-free coordination for Internet-scale systems"

## 9. Acknowledgements
Thanks to the teachers and seniors of the XiyouLinux Interest Group for their guidance and support.

Thanks to Teacher Song Hui for his valuable suggestions.

Thanks to Li Hao for providing a better server startup interval handling function.

Thanks to Li Yi for designing the first and second versions of the ChubbyGo logo, which led to the creation of the cute character Go.

![avatar](Pictures/XiyouLinuxGroup.png)