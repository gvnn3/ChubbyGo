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

package Config

import (
	"ChubbyGo/Connect"
	"encoding/json"
	"log"
	"os"
	"sync"
)

var Server_file_locker sync.Mutex // config file locker

func init() {
	Connect.RegisterRestServerListener(LoadServerConfig)
}

func LoadServerConfig(filename string, cfg *Connect.ServerConfig) bool {

	Server_file_locker.Lock()
	data, err := os.ReadFile(filename) // read config file
	Server_file_locker.Unlock()
	if err != nil {
		log.Printf("read json file error, %s\n", err.Error())
		return false
	}
	// Unmarshal prefers exact matches for structures, but also accepts inexact matches
	err = json.Unmarshal(data, &cfg)
	// It's ridiculous that it can't parse arrays without double quotes
	// fmt.Printf("DEBUG : data %s : %s : %s : %s\n", data, cfg.SnapshotFileName, cfg.RaftstateFileName, cfg.PersistenceStrategy)
	if err != nil {
		log.Println("unmarshal json file error")
		return false
	}
	return true
}
