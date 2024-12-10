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
	"math"
	"net"
	"strconv"
	"strings"
	"unsafe"
)

// -------------------------------------------

// string to bytes
func Str2sbyte(s string) (b []byte) {
	*(*string)(unsafe.Pointer(&b)) = s                                                  // Assign the address of s to b
	*(*int)(unsafe.Pointer(uintptr(unsafe.Pointer(&b)) + 2*unsafe.Sizeof(&b))) = len(s) // Modify capacity to length
	return
}

// []byte to string
func Sbyte2str(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

// -------------------------------------------

type ErrorInConnectAll int8 // Specially used for the return value of connectAll because there are three types of errors that cannot be represented by bool

const (
	time_out = iota
	http_error
)

func (err ErrorInConnectAll) Error() string {
	var ans string
	switch err {
	case time_out:
		ans = "Connection to a target server greater than or equal to timeout."
		break
	case http_error:
		ans = "rpc.DialHTTP return HTTP error."
		break
	default:

	}
	return ans
}

// -------------------------------------------

type ErrorInStartServer int8 // Specially used for the return value of StartServer and StartClient because there are four types of errors that cannot be represented by bool

const (
	parser_error = iota
	connect_error
	Listener_error
)

func (err ErrorInStartServer) Error() string {
	var ans string
	switch err {
	case parser_error:
		ans = "Error parsing configuration file."
		break
	case connect_error:
		ans = "Connection error, Refer to ErrorInConnectAll for specific error types."
		break
	case Listener_error:
		ans = "Listener error, Guess that the init function of read_c/s_config.go is not started at the call site."
		break
	default:

	}
	return ans
}

// -------------------------------------------

type ErrorInParserConfig int8

const (
	maxreries_to_small = iota
	serveraddress_length_to_small
	serveraddress_format_error
	parser_port_error
	time_out_entry_error
	raft_maxraftstate_not_suitable
	parser_snapshot_file_name
	parser_raftstate_file_name
	parser_persistence_strategy
	parser_chubbygomap_strategy
)

func (err ErrorInParserConfig) Error() string {
	var ans string
	switch err {
	case maxreries_to_small:
		ans = "Maxreries Less than or equal to 7."
		break
	case serveraddress_length_to_small:
		ans = "Serveraddress length Less than or equal to 2."
		break
	case serveraddress_format_error:
		ans = "Format error in ip address parsing."
		break
	case parser_port_error:
		ans = "Parser port error."
		break
	case time_out_entry_error:
		ans = "Time out entry Too small or too big."
		break
	case raft_maxraftstate_not_suitable:
		ans = "raft maxraftstate not suitable, Less than zero or greater than 2MB."
		break
	case parser_snapshot_file_name:
		ans = "Format error in parsing snapshot file."
		break
	case parser_raftstate_file_name:
		ans = "Format error in parsing raftstate file."
		break
	case parser_persistence_strategy:
		ans = "No such persistence strategy."
		break
	case parser_chubbygomap_strategy:
		ans = "no such chubbygomap strategy."
		break
	default:
	}
	return ans
}

/*
 * @brief: Parse MyPort
 * @return: Returns true if correct; otherwise false
 */
func parserMyPort(MyPort string) bool {
	if len(MyPort) < 0 { // Must be parsed
		return false
	}

	var data []byte = Str2sbyte(MyPort)

	if data[0] != ':' { // The first character must be ':'
		return false
	}

	var str string = string(data[1:])
	// TODO The data type of the tcp port is unsigned short, so the maximum number of local ports is only 65536, which may need to be modified in the future
	if port, err := strconv.Atoi(str); err != nil || port > 65536 || port < 0 {
		return false
	}
	return true
}

/*
 * @brief: Parse the ip address item extracted from json
 * @return: Returns true if correct; otherwise false
 * @notes: The ip parsing function of the net package is a bit troublesome, it can only parse "ip/port" or "ip", not "ip:port"
 */
func ParserIP(address string) bool {
	data := Str2sbyte(address)
	var index int = -1
	for i := len(data) - 1; i >= 0; i-- { // Parse out the last ':'
		if data[i] == ':' {
			index = i
			break
		}
	}
	if index == -1 { // Failed to parse ':'
		return false
	}

	ip := data[:index]
	port := data[index+1:]

	ParserRes := net.ParseIP(Sbyte2str(ip))                                // Parse ip "localhost" cannot be parsed by ParseIP
	if ParserRes == nil && strings.ToLower(Sbyte2str(ip)) != "localhost" { // Ignore case
		return false
	}

	if po, err := strconv.Atoi(Sbyte2str(port)); err != nil || po > 65536 || po < 0 { // Port parsing failed or range error
		return false
	}

	return true
}

/*
 * @brief: Parse the file name extracted from json
 * @return: Returns true if correct; otherwise false, does not distinguish between various error types here
 * @notes: The following can be configured according to different machines, I did not find an interface in Golang that can directly obtain the following values, so it is manually configured
 *	Linux uses getconf PATH_MAX /usr to get the path length limit; 4096
 * 	getconf NAME_MAX /usr to get the file name length limit; 255
 *	Another point is my personal requirement, the suffix must be hdb, just so proud
 * File name restrictions: https://en.wikipedia.org/wiki/Filename
 * The current file name restrictions found are that "/" and " " are not allowed, and "-" cannot be the first character
 */
func ParserFileName(pathname string) bool {
	Length := len(pathname)

	if Length > 4096 {
		return false
	}

	index1 := -1 // Indicates suffix
	index2 := 0  // Indicates file name

	for i := Length - 1; i >= 0; i-- {
		if pathname[i] == '.' {
			index1 = i
		} else if pathname[i] == '/' {
			index2 = i
			break
		}
	}

	// No suffix
	if index1 == -1 {
		return false
	}

	// Check suffix
	// a . h d b
	// 0 1 2 3 4
	if Length-index1 != 4 {
		return false
	} else { // Simple and effective, no fancy stuff
		if pathname[index1+1] != 'h' || pathname[index1+2] != 'd' || pathname[index1+3] != 'b' {
			return false
		}
	}

	// Check file name; 255 see function comments
	if index1-index2-1 > 255 {
		return false
	}

	// TODO Currently only checking the last file name, detecting the format here is to detect errors earlier, because the protocol has already started when opening this file, which will cause persistent data reading to fail; it can also use the default file
	if pathname[index2+1] == '-' {
		return false
	}

	// Because of the above parsing process, there cannot be '/' in the data here, just check ' '.
	for i := index2 + 1; i < index1; i++ {
		if pathname[i] == ' ' {
			return false
		}
	}

	return true
}

/*
 * @brief: Server connection interval function proposed by Li Hao
 * @return: Milliseconds, no need to convert by the caller
 * @notes: TODO Currently it does not meet expectations; it can be modified later,
 */
/*
 * @example: 0 	  695  893  1005 1083 	// It can be seen that the slope of the first few retries is still too steep, and the latter is too gentle
			 1142 1190 1230 1265 1295
			 1322 1347 1369 1389 1408
			 1426 1442 1457 1472 1485
*/
func ReturnInterval(n int) int {
	molecular := math.Log(float64(n+1)) / math.Log(math.E)
	temp := math.Log(float64(n+2)) / math.Log(1.5)
	denominator := math.Log(temp) / math.Log(math.E)
	res := molecular / denominator
	return int(res * 1000)
}

/*
 * @brief: Parse whether the persistence strategy in the file is correct, currently there are only three valid strategies
 */
func checkPersistenceStrategy(strategy string) bool {
	lower := strings.ToLower(strategy)

	if lower != "everysec" && lower != "no" && lower != "always" {
		return false
	}

	return true
}

/*
 * @brief: Parse the map strategy of chubbygomap
 */
func checkChubbyGoMapStrategy(strategy string) bool {
	lower := strings.ToLower(strategy)

	if lower != "syncmap" && lower != "concurrentmap" {
		return false
	}

	return true
}

/*
 * @brief: Already checked by checkjson, there are only two possible situations
 */
func transformChubbyGomap2uint32(strategy string) uint32 {
	lower := strings.ToLower(strategy)
	if lower == "syncmap" {
		return BaseServer.SyncMap
	} else {
		return BaseServer.ConcurrentMap
	}
}
