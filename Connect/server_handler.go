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

package Connect

import (
	"ChubbyGo/BaseServer"
	"ChubbyGo/Flake"
	"ChubbyGo/Persister"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// The first version of creating connections still requires the servers to know each other's IP addresses, and the client also needs to know the server's address
// TODO Later, it can be changed to something like Redis, where the existence of other nodes is obtained through message interaction

type ServerConfig struct {
	peers     []*rpc.Client        // Represents the connection handles of several other servers
	me        uint64               // Later changed to a globally unique ID
	nservers  int                  // Indicates how many servers there are in total
	kvserver  *BaseServer.RaftKV   // A raftkv entity
	persister *Persister.Persister // Persistence entity
	mu        sync.Mutex           // Used to protect the variables of this structure
	// If not set to uppercase, it cannot be read from the configuration file
	MaxRaftState   int      `json:"maxraftstate"`    // Raft layer log compression limit
	Maxreries      int      `json:"maxreries"`       // Maximum number of timeout retries
	ServersAddress []string `json:"servers_address"` // Read the addresses of other servers from the configuration file
	MyPort         string   `json:"myport"`          // Own port number
	TimeOutEntry   int      `json:"timeout_entry"`   // Retransmission timeout interval defined in connectAll, in milliseconds
	// These three need to be passed to persister after parsing
	// The suffix hdb stands for "Honeycomb Database Backup file"
	SnapshotFileName    string `json:"snapshotfilename"`    // Snapshot persistence file name
	RaftstateFileName   string `json:"raftstatefilename"`   // Raft state persistence name
	PersistenceStrategy string `json:"persistencestrategy"` // Corresponds to three strategies, see persister.go, note that the configuration file cannot be misspelled
	ChubbyGoMapStrategy string `json:"chubbygomapstrategy"` // Corresponds to two strategies: syncmap, concurrentmap; see chubbygomap.go for details
}

/*
 * @brief: Get the addresses of other servers and establish RPC connections respectively
 * @return: Three return types: timeout; HttpError; success
 */
// Peers have a race condition; fix it by starting the service after the connection is complete
func (cfg *ServerConfig) connectAll() error {
	sem := make(Semaphore, cfg.nservers-1)
	sem_number := 0
	var HTTPError int32 = 0
	var TimeOut []int
	var timeout_mutex sync.Mutex

	servers_length := len(cfg.ServersAddress)
	for i := 0; i < servers_length; i++ {
		if atomic.LoadInt32(&HTTPError) > 0 {
			break
		}
		client, err := rpc.DialHTTP("tcp", cfg.ServersAddress[i])
		/*
		 * There are three return situations here:
		 * net.Dial returns error: retry
		 * http.ReadResponse returns: HTTP error
		 * Normal return
		 */
		if err != nil {
			switch err.(type) {
			case *net.OpError: // Hooked with library implementation, may need to be modified if the standard library implementation is not synchronized
				sem_number++
				// If there is a network problem, we have reason to retry, with a maximum number of MAXRERRIES, and the interval time doubles each time
				go func(index int) {
					defer sem.P(1)
					number := 0
					Timeout := cfg.TimeOutEntry
					for number < cfg.Maxreries {
						if atomic.LoadInt32(&HTTPError) > 0 {
							return
						}
						log.Printf("INFO : %s : Reconnecting for the %d time\n", cfg.ServersAddress[index], number+1)
						number++
						Timeout = Timeout * 2
						time.Sleep(time.Duration(Timeout) * time.Millisecond) // Exponential backoff for reconnection duration
						TempClient, err := rpc.DialHTTP("tcp", cfg.ServersAddress[index])
						if err != nil {
							switch err.(type) {
							case *net.OpError:
								// Continue the loop
								continue
							default:
								atomic.AddInt32(&HTTPError, 1)
								return
							}
						} else {
							// cfg.mu.Lock()
							// defer cfg.mu.Unlock()	// No goroutine will touch this
							log.Printf("INFO : %d successfully connected to %s\n", cfg.me, cfg.ServersAddress[index])
							cfg.peers[index] = TempClient
							return
						}
					}
					// Only after looping cfg.maxreries times without result will it reach here
					// That is, connection timeout
					timeout_mutex.Lock()
					defer timeout_mutex.Unlock()
					TimeOut = append(TimeOut, index) // For easy logging
					return
				}(i)
				continue
			default:
				atomic.AddInt32(&HTTPError, 1)
			}
		} else {
			log.Printf("INFO : %d successfully connected to %s\n", cfg.me, cfg.ServersAddress[i])
			cfg.peers[i] = client
		}
	}
	// Ensure that all goroutines finish before exiting, i.e., either all connections are successful or an error is reported
	sem.V(sem_number)

	TimeOutLength := len(TimeOut)
	if atomic.LoadInt32(&HTTPError) > 0 || TimeOutLength > 0 { // Release connections after failure
		for i := 0; i < servers_length; i++ {
			cfg.peers[i].Close() // Even if the connection was never established, calling close will only return ErrShutdown
		}
		if TimeOutLength > 0 {
			return ErrorInConnectAll(time_out)
		}
		return ErrorInConnectAll(http_error)
	} else {
		return nil // No errors occurred, success
	}
}

/*
 * @brief: Register raft and kvraft to RPC
 */
func (cfg *ServerConfig) serverRegisterFun() {
	// When registering RPC, as long as there are members in the structure that do not meet the requirements, logs will be printed, which is quite annoying
	// Register RaftKv to RPC
	err := rpc.Register(cfg.kvserver)
	if err != nil {
		// RPC will register all functions that meet the rules, and if there are functions that do not meet the rules, it will return err
		log.Println(err.Error())
	} else {
		log.Println("INFO : Kvserver has been successfully registered.")
	}

	// Register Raft to RPC
	err1 := rpc.Register(cfg.kvserver.GetRaft())
	if err1 != nil {
		log.Println(err1.Error())
	} else {
		log.Println("INFO : Raft has been successfully registered.")
	}

	// Register the services provided by mathutil to the HTTP protocol through functions, so that callers can use HTTP for data transmission
	rpc.HandleHTTP()

	// Listen on a specific port
	listen, err2 := net.Listen("tcp", cfg.MyPort)
	if err2 != nil {
		log.Println(err2.Error())
	}
	go func() {
		// Call the method to handle HTTP requests
		http.Serve(listen, nil)
	}()
}

/*
 * @brief: Check whether the fields parsed from json meet the requirements
 * @return: Returns true if parsed correctly, false if there is an error
 */
func (cfg *ServerConfig) checkJsonParser() error {
	// When the configuration number is less than 7, the time left for other servers to start is too short. When the configuration is 8, at least 51 seconds have passed (2^9-2^1)
	if cfg.Maxreries <= 7 {
		return ErrorInParserConfig(maxreries_to_small)
	}

	ServerAddressLength := len(cfg.ServersAddress)

	if ServerAddressLength < 2 { // At least three, and it is recommended to be an odd number, including yourself
		return ErrorInParserConfig(serveraddress_length_to_small)
	}

	for _, AddressItem := range cfg.ServersAddress {
		if !ParserIP(AddressItem) {
			return ErrorInParserConfig(serveraddress_format_error)
		}
	}

	if !parserMyPort(cfg.MyPort) {
		return ErrorInParserConfig(parser_port_error)
	}

	// TODO Check the convergence range of several functions
	// A timeout interval cannot be too large or too small. This interval can be guaranteed
	if cfg.TimeOutEntry <= 100 || cfg.TimeOutEntry >= 2000 {
		return ErrorInParserConfig(time_out_entry_error)
	}

	// /proc/cpuinfo https://blog.csdn.net/wswit/article/details/52665413 l2 cache
	if cfg.MaxRaftState < 0 || cfg.MaxRaftState > 2097152 {
		return ErrorInParserConfig(raft_maxraftstate_not_suitable)
	}

	// Parse whether SnapshotFileName meets the specifications
	if !ParserFileName(cfg.SnapshotFileName) {
		return ErrorInParserConfig(parser_snapshot_file_name)
	}

	// Parse whether RaftstateFileName meets the specifications
	if !ParserFileName(cfg.RaftstateFileName) {
		return ErrorInParserConfig(parser_raftstate_file_name)
	}

	// Parse whether PersistenceStrategy meets the specifications
	if !checkPersistenceStrategy(cfg.PersistenceStrategy) {
		return ErrorInParserConfig(parser_persistence_strategy)
	}

	// Parse whether ChubbyGoMapStrategy meets the specifications
	if !checkChubbyGoMapStrategy(cfg.ChubbyGoMapStrategy) {
		return ErrorInParserConfig(parser_chubbygomap_strategy)
	}

	return nil
}

/*
 * @brief: Start the service when this function is called,
 * @return: Three return types: path parsing error; connectAll connection problem; success
 */
func (cfg *ServerConfig) StartServer() error {
	var flag bool = false
	if len(ServerListeners) == 1 {
		// Correctly read the configuration file; note here, checkJsonParser checks the range and format of the string, and the parsing process will parse out errors between int and string
		flag = ServerListeners[0]("Config/server_config.json", cfg)
		if !flag { // File open failed
			log.Printf("Open config File Error!")
			return ErrorInStartServer(parser_error)
		}

		// Fields parsed from json are in the wrong format
		if ParserErr := cfg.checkJsonParser(); ParserErr != nil {
			log.Println(ParserErr.Error())
			return ErrorInStartServer(parser_error)
		}

		// Fill cfg.Persister
		cfg.fillPersister()

	} else {
		log.Printf("ERROR : [%d] ServerListeners Error!\n", cfg.me)
		// This situation only occurs when the service is called without starting the init function of read_server_config.go
		return ErrorInStartServer(Listener_error)
	}

	// The reason for initialization here is to let the registered structure be the structure that runs later
	cfg.kvserver = BaseServer.StartKVServerInit(cfg.me, cfg.persister, cfg.MaxRaftState, transformChubbyGomap2uint32(cfg.ChubbyGoMapStrategy))
	cfg.kvserver.StartRaftServer(&cfg.ServersAddress)

	cfg.serverRegisterFun()
	if err := cfg.connectAll(); err != nil {
		log.Println(err.Error())
		return ErrorInStartServer(connect_error)
	}
	cfg.kvserver.StartKVServer(cfg.peers) // Start the service
	log.Printf("INFO : [%d] The connection is successful and the service has started successfully!\n", cfg.me)
	return nil
}

/*
 * @brief: Return a Config structure to start a service
 * @param: nservers The number of machines required for this cluster, including itself, and each server must be configured correctly when initialized
 */
func CreatServer(nservers int) *ServerConfig {
	cfg := &ServerConfig{}

	cfg.nservers = nservers
	cfg.MaxRaftState = 0
	cfg.peers = make([]*rpc.Client, cfg.nservers-1) // Stores the RPC encapsulation of other servers except itself
	cfg.me = Flake.GetSonyflake()                   // The global ID needs to be passed to the raft layer
	cfg.persister = Persister.MakePersister()

	return cfg
}

// --------------------------
// Use the Listener pattern to avoid circular references between /Connect and /Config
// Of course, a good design might avoid this problem, such as changing the function parameters for reading configuration files to interfaces and using reflection to push

type ServerListener func(filename string, cfg *ServerConfig) bool

var ServerListeners []ServerListener

func RegisterRestServerListener(l ServerListener) {
	ServerListeners = append(ServerListeners, l)
}

// --------------------------

/*
 * @brief: Fill the three items parsed from json: SnapshotFileName, RaftstateFileName, PersistenceStrategy into Persister
 * @notes: This function is based on the completion of checkJsonParser, that is, the values in it should meet our requirements
 */
func (cfg *ServerConfig) fillPersister() {
	cfg.persister.RaftstateFileName = cfg.RaftstateFileName
	cfg.persister.SnapshotFileName = cfg.SnapshotFileName

	// Ignore case in the strategy item in the configuration file
	LowerStrategy := strings.ToLower(cfg.PersistenceStrategy)

	if LowerStrategy == "everysec" {
		cfg.persister.PersistenceStrategy = Persister.Everysec
	} else if LowerStrategy == "no" {
		cfg.persister.PersistenceStrategy = Persister.No
	} else {
		cfg.persister.PersistenceStrategy = Persister.Always
	}
}
