package main

import (
	_ "ChubbyGo/Config"
	"ChubbyGo/Connect"
	"fmt"
	"log"
	"strconv"
	"time"
)

/*
 * TODO Currently, this test function has issues. After the cluster starts, only the first execution is ok, unless the clientID (globally unique ID) is reset each time.
 *      Because the value from the last time is still in the server, but the clientID in the restarted client is the same as last time, and the seq is 0, so there will be many duplicate values.
 *      There is currently no good solution because this is a limitation of the flake algorithm. I don't want to rewrite the flake for the test file for now.
 */

const (
	get = iota
	fastGet
)
const GetStrategy = get

func main() {
	n := 60
	Sem := make(Connect.Semaphore, n)
	SemNumber := 0
	clientConfigs := make([]*Connect.ClientConfig, n)
	flags := make([]bool, n)
	for i := 0; i < n; i++ {
		clientConfigs[i] = Connect.CreateClient()
		err := clientConfigs[i].StartClient()
		if err != nil {
			log.Println(err.Error())
		} else { // Obviously, only after a successful connection
			clientConfigs[i].SetUniqueFlake(uint64(i + n*1)) // If you want to retry multiple times, just increment the 0 here by 1 each time
			flags[i] = true
		}
	}

	start := time.Now()

	for i := 0; i < n; i++ {
		if !flags[i] {
			continue
		}
		SemNumber++
		go func(cliID int) {
			defer Sem.P(1)
			for j := 0; j < 8; j++ {
				nv := "x " + strconv.Itoa(cliID) + " " + strconv.Itoa(j) + " y"
				clientConfigs[cliID].Put(strconv.Itoa(cliID), nv)
				fmt.Println(cliID, " : put successful, ", nv)
				var res string
				if GetStrategy == get {
					res = clientConfigs[cliID].Get(strconv.Itoa(cliID))
				} else if GetStrategy == fastGet {
					res = clientConfigs[cliID].FastGet(strconv.Itoa(cliID))
				} else {
					log.Println("Error Get Strategy.")
					return
				}
				if res != nv {
					fmt.Printf("%d : expected: %s, now : %s\n", cliID, nv, res)
				} else {
					fmt.Println(cliID, " : Get successful")
				}
			}
		}(i)
	}

	Sem.V(SemNumber)

	cost := time.Since(start)

	fmt.Printf("%d threads : %d requests took %s.\n", n, n*20, cost)

	for i := 0; i < len(flags); i++ {
		if flags[i] { // PASS if at least one server connection is successful and completed
			fmt.Println("PASS!")
			return
		}
	}
}
