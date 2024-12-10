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

package Connect

import (
	"ChubbyGo/BaseServer"
	"log"
	"net"
	"net/rpc"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type ClientConfig struct {
	servers        []*rpc.Client     // Represents the connection handles of several other servers
	serversIsOk    []int32           // Indicates which servers can be connected at this time
	clk            *BaseServer.Clerk // An entity of a client
	nservers       int               // Number of connected servers
	ServersAddress []string          `json:"client_address"` // Read server addresses from the configuration file
	Maxreries      int               `json:"maxreries"`      // Maximum number of timeout reconnections
}

func CreateClient() *ClientConfig {
	cfg := &ClientConfig{}

	return cfg
}

/*
 * @brief: Get the addresses of other servers and establish RPC connections respectively
 * @return: Three return types: timeout; HttpError; success
 * @notes: The client also adopts retransmission because it is worried that direct connection will fail when the service is deployed; it is possible that the server cluster is not running enough N at this time, but it is still providing services, so the logic here is different from the server
 */

/*
 * October 23: The following issues have been fixed
 * During testing, a problem was found. When one of the three servers crashes, the client cannot connect in this way, so it needs to be modified here so that it can connect successfully when more than N/2, and the service is already offline when it is less than that, so there is no need to connect.
 * This problem is not simple, because if there is no connection in connectAll, the client code will always operate, which must be locked. We can introduce a flag map to mark which items of peers can be used,
 * In ConnectAll, after the client service has started, it is still trying to connect to the unconnected server. After the connection is successful, the map needs to be modified so that the client code can connect to this new server,
 * This map should be shared by ClientConfig and Clerk.‘
 * It seems that the client must always try to reconnect to the server, rather than doubling like the server, because the client will not reconnect after failure, which is inconsistent with expectations
 * Temporarily do not move here, because the connection is ok when the server is not down, first pass the test code and then optimize here to prevent errors here from causing difficult troubleshooting
 */

/*
 * 1. Finally decided to use a set of numbers to determine which peers can be reconnected, because each peer will only be modified once at most,
 * For the use of peer, it is generally used like this
 * if atomic.load(flag) == 1{
 *	  peer.call
 * }
 * When judged to be false, it will not operate. When it is true, the peer can already operate
 * 2. At present, the attitude towards downed servers is to reconnect, but this is based on the fact that most servers have been successfully connected, we can reconnect at will,
 * 3. The key to the problem is that most machines may not be able to connect successfully. At this time, all connections should be disconnected and exit directly.
 * 	  Suppose there are three servers, and one is successfully connected at present. At this time, it will block and wait for the connection to succeed, but in fact, we cannot know when these two servers can be successfully connected at this time.
 *    Then we can only predict. The tentative solution is to retry Maxreries times for two servers. There must be one that retries first and continues to retry, but make a mark.
 *    When the second machine also retries Maxreries times, the two will exit together and close the successfully connected server at the same time. If one is successful in the middle, the client starts to serve, and the remaining one should keep reconnecting
 */

// TODO This function is too complex, but there seems to be no better way at the moment
func (cfg *ClientConfig) connectAll() error {

	sem := make(Semaphore, cfg.nservers-1)
	sem_number := 0
	var HTTPError int32 = 0                        // Possible number of HTTPError occurrences
	var TimeOut []int                              // Number of timed-out servers
	var TimeoutMutex sync.Mutex                    // Protect Timeout
	var SucessConnect int32 = 0                    // Number of successful connections during the execution of this function, obviously greater than or equal to target to exit
	var StartServer uint32 = 0                     // For goroutines still running after this function exits, when the value is greater than 0, it runs permanently
	servers_length := len(cfg.ServersAddress)      // Number of servers to connect
	var target int32 = int32(servers_length/2 + 1) // End when target servers are successfully connected

	// Used to determine whether each item exceeds Maxreries, in fact, cfg.serversIsOk can be reused, because each item does not conflict, but I think it is better to divide the functions clearly
	PeerExceedMaxreries := make([]bool, servers_length) // Record the number of servers that have retransmitted more than Maxreries times

	for i := 0; i < servers_length; i++ {
		if atomic.LoadInt32(&HTTPError) > 0 { // TODO This version still does not tolerate this error, try to reproduce this error later
			break
		}
		client, err := rpc.DialHTTP("tcp", cfg.ServersAddress[i])
		/*
		 * There are three return situations here:
		 * net.Dial returns error: reconnect
		 * http.ReadResponse returns: HTTP error
		 * Normal return
		 */
		if err != nil {
			switch err.(type) {
			case *net.OpError: // Hooked with library implementation, may need to be modified here for different versions of the standard library
				sem_number++
				// The reconnection here is different from the server. We need to keep reconnecting when at least n/2+1 is connected. If it times out without reaching n/2+1, we need to exit
				go func(index int) {
					defer sem.P(1)
					number := 0
					Timeout := 0
					// When StartServer is set to 1, the second judgment condition is no longer considered. When most servers are successfully connected, StartServer is set to 1
					for atomic.LoadUint32(&StartServer) >= 1 || number <= cfg.Maxreries {

						if atomic.LoadInt32(&HTTPError) > 0 {
							return
						}

						// At least target servers have experienced Maxreries retransmissions
						// It is possible that one server retries Maxreries and then reconnects successfully, and other servers will retry indefinitely, so the judgment of StartServer should be added
						if atomic.LoadUint32(&StartServer) == 0 && judgeTrueNumber(PeerExceedMaxreries) >= target {
							break
						}

						log.Printf("%s : Reconnecting for the %d time\n", cfg.ServersAddress[index], number+1)

						number++
						if number >= cfg.Maxreries {
							number = 0
							// The reason for not using an int32 is that in extreme cases, a server retries twice Maxreries and will also exit
							PeerExceedMaxreries[index] = true
						}
						Timeout = ReturnInterval(number)

						time.Sleep(time.Duration(Timeout) * time.Millisecond)

						TempClient, err := rpc.DialHTTP("tcp", cfg.ServersAddress[index])
						if err != nil {
							switch err.(type) {
							case *net.OpError:
								// Just continue the loop
								continue
							default:
								atomic.AddInt32(&HTTPError, 1)
								return
							}
						} else {
							// cfg.mu.Lock()
							// defer cfg.mu.Unlock()	// No goroutine will touch this
							cfg.servers[index] = TempClient             // The order cannot be wrong, cannot be placed after the next line
							atomic.AddInt32(&cfg.serversIsOk[index], 1) // The client may have already started serving, so this function is still running, so it must be atomic
							atomic.AddInt32(&SucessConnect, 1)
							log.Printf("Successfully connected to %d\n", cfg.ServersAddress[i])
							return
						}
					}
					// Only after looping cfg.maxreries times without result will it run here
					// That is, connection timeout
					TimeoutMutex.Lock()
					defer TimeoutMutex.Unlock()
					TimeOut = append(TimeOut, index) // For easy logging
					return
				}(i)
				continue
			default:
				atomic.AddInt32(&HTTPError, 1)
			}
		} else {
			atomic.AddInt32(&SucessConnect, 1)
			atomic.AddInt32(&cfg.serversIsOk[i], 1) // Mark this peer as connectable
			log.Printf("Successfully connected to %d\n", cfg.ServersAddress[i])
			cfg.servers[i] = client
		}
	}

	if atomic.LoadInt32(&SucessConnect) < target {
		// There is no race condition here because SucessConnect can only increase, the worst case here is target - SucessConnect is less than zero, which does not matter
		sem.V(int(target - atomic.LoadInt32(&SucessConnect))) // As long as this number is reached, it proves that the server cluster can already provide services, and other goroutines are still connecting
	}

	TimeOutLength := len(TimeOut)
	if atomic.LoadInt32(&HTTPError) > 0 || TimeOutLength > 0 { // Release connection after failure
		log.Println("159 : ", atomic.LoadInt32(&HTTPError), TimeOutLength)
		for i := 0; i < servers_length; i++ {
			if atomic.LoadInt32(&(cfg.serversIsOk[i])) == 1 {
				cfg.servers[i].Close() // Close successfully connected connections
			}
		}
		if TimeOutLength > 0 {
			return ErrorInConnectAll(time_out)
		}
		return ErrorInConnectAll(http_error)
	} else {
		atomic.AddUint32(&StartServer, 1) // Represents that subsequent reconnections are infinite reconnections
		return nil                        // No errors occurred, success
	}
}

/*
 * @brief: Used to determine the number of goroutines that have reconnected Maxreries at this time, and exit directly when the threshold is exceeded, error is timeout
 * @return: Returns the number of trues in the array
 */
func judgeTrueNumber(array []bool) int32 {
	arrayLength := len(array)
	var res int32 = 0
	for i := 0; i < arrayLength; i++ {
		if array[i] {
			res++
		}
	}
	return res
}

/*
 * @brief: Start the service when calling this function,
 * @return: Three return types: path parsing error; connectAll connection problem; success
 */
func (cfg *ClientConfig) StartClient() error {
	var flag bool = false
	if len(ClientListeners) == 1 {
		// Correctly read the configuration file
		flag = ClientListeners[0]("Config/client_config.json", cfg)
		if !flag {
			log.Println("File parser Error!")
			return ErrorInStartServer(parser_error)
		}

		// Field format error extracted from json
		if ParserErr := cfg.checkJsonParser(); ParserErr != nil {
			log.Println(ParserErr.Error())
			return ErrorInStartServer(parser_error)
		}

		cfg.nservers = len(cfg.ServersAddress)
		cfg.servers = make([]*rpc.Client, cfg.nservers)
		cfg.serversIsOk = make([]int32, cfg.nservers)
	} else {
		log.Println("ClientListeners Error!")
		// This situation only occurs when the service is called without starting the init function of read_client_config.go
		return ErrorInStartServer(Listener_error)
	}

	if err := cfg.connectAll(); err != nil {
		log.Println(err.Error())
		return ErrorInStartServer(connect_error)
	}
	cfg.clk = BaseServer.MakeClerk(cfg.servers, &(cfg.serversIsOk))
	return nil
}

/*
 * @brief: Check whether the fields parsed from json meet the requirements
 * @return: Return true if the parsing is correct, false if there is an error
 */
func (cfg *ClientConfig) checkJsonParser() error {
	// When the configuration number is less than 7, the time left for other servers to start is too short, only about eight seconds
	// This value also defines the client connection timeout
	if cfg.Maxreries <= 7 {
		return ErrorInParserConfig(maxreries_to_small)
	}

	ServerAddressLength := len(cfg.ServersAddress)

	if ServerAddressLength <= 2 { // At least three, the client does not need to calculate itself
		return ErrorInParserConfig(serveraddress_length_to_small)
	}

	for _, AddressItem := range cfg.ServersAddress {
		if !ParserIP(AddressItem) {
			return ErrorInParserConfig(serveraddress_format_error)
		}
	}

	return nil
}

// --------------------------
// Two functions for DEBUG, found a huge problem with the snowflake algorithm, generated the same within the same process, causing testing problems

func (cfg *ClientConfig) GetUniqueFlake() uint64 {
	return cfg.clk.ClientID
}

func (cfg *ClientConfig) SetUniqueFlake(value uint64) {
	cfg.clk.ClientID = value
}

// --------------------------

func (cfg *ClientConfig) Put(key string, value string) {
	cfg.clk.Put(key, value)
}

func (cfg *ClientConfig) Append(key string, value string) {
	cfg.clk.Append(key, value)
}

func (cfg *ClientConfig) Get(key string) string {
	return cfg.clk.Get(key)
}

func (cfg *ClientConfig) Open(pathname string) (bool, *BaseServer.FileDescriptor) {
	return cfg.clk.Open(pathname)
}

func (cfg *ClientConfig) Create(fd *BaseServer.FileDescriptor, fileType int, filename string) (bool, *BaseServer.FileDescriptor) {
	return cfg.clk.Create(fd, fileType, filename)
}

func (cfg *ClientConfig) Delete(fd *BaseServer.FileDescriptor, opType int) bool {
	index := strings.LastIndex(fd.PathName, "/")

	return cfg.clk.Delete(fd.PathName[0:index], fd.PathName[index+1:], fd.InstanceSeq, opType, fd.ChuckSum)
}

func (cfg *ClientConfig) Acquire(fd *BaseServer.FileDescriptor, LockType int, Timeout uint32) (bool, uint64) {
	index := strings.LastIndex(fd.PathName, "/")

	RemainPath := fd.PathName[0:index]
	if RemainPath == "/ls" {
		log.Println("ERROR : Root node from every ChubbyGo cell can not be Acquired.")
		return false, 0
	}

	return cfg.clk.Acquire(fd.PathName[0:index], fd.PathName[index+1:], fd.InstanceSeq, LockType, fd.ChuckSum, Timeout)
}

func (cfg *ClientConfig) Release(fd *BaseServer.FileDescriptor, token uint64) bool {
	index := strings.LastIndex(fd.PathName, "/")
	return cfg.clk.Release(fd.PathName[0:index], fd.PathName[index+1:], fd.InstanceSeq, token, fd.ChuckSum)
}

/*
 * @param: Give the file name and the token in hand, and return whether the token is valid at this moment
 * @brief: Absolute paths and relative paths can be passed in, and it cannot be called relative paths
 */
func (cfg *ClientConfig) CheckToken(AbsolutePath string, token uint64) bool {
	index := strings.LastIndex(AbsolutePath, "/")
	return cfg.clk.CheckToken(AbsolutePath[0:index], AbsolutePath[index+1:], token)
}

func (cfg *ClientConfig) CheckTokenAt(pathname string, filename string, token uint64) bool {
	return cfg.clk.CheckToken(pathname, filename, token)
}

/*
 * @brief: FastGet does not provide any consistency guarantees, suitable for situations with many reads and few writes. At this time, changing the strategy of concurrent map to syncmap is more optimal, and performance can be improved by 20% to 30%
 */
func (cfg *ClientConfig) FastGet(key string) string {
	return cfg.clk.FastGet(key)
}

/*
 * @brief: Provide a more flexible CAS operation, the specific definition can be found at the beginning of compareSwap.go file
 */
func (cfg *ClientConfig) CompareAndSwap(Key string, Old int, New int, Flag int) bool {
	return cfg.clk.CompareAndSwap(Key, Old, New, Flag)
}

// --------------------------
// Use Listener mode to avoid circular references between /Connect and /Config

type ClientListener func(filename string, cfg *ClientConfig) bool

var ClientListeners []ClientListener

func RegisterRestClientListener(l ClientListener) {
	ClientListeners = append(ClientListeners, l)
}

// --------------------------
