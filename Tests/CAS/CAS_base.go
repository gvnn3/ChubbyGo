package main

import (
	"ChubbyGo/BaseServer"
	_ "ChubbyGo/Config"
	"ChubbyGo/Connect"
	"fmt"
	"log"
)

// Test the basic functions of CAS operations, including normal CAS, increment and decrement with specified boundaries and spans.

func main() {
	n := 10 // n greater than zero
	Sem := make(Connect.Semaphore, n-1)
	SemNumber := 0

	clientConfigs := make([]*Connect.ClientConfig, n)
	for i := 0; i < n; i++ {
		clientConfigs[i] = Connect.CreateClient()
		err := clientConfigs[i].StartClient()
		if err != nil {
			log.Println(err.Error())
		} else {
			clientConfigs[i].SetUniqueFlake(uint64(i + n*30))
		}
	}

	// A standard counter creation method during a flash sale
	clientConfigs[0].Put("lizhaolong", "8")

	log.Println("Starting decrement CAS operation.")

	// One goroutine will fail
	for i := 1; i < n; i++ {
		SemNumber++
		go func(index int) {
			defer Sem.P(1)
			// Decrement the value of the key "lizhaolong" when it is greater than old(0), decrement by New(1) each time,
			ok := clientConfigs[index].CompareAndSwap("lizhaolong", 0, 1, BaseServer.Sub)
			if ok {
				fmt.Printf("Decrement index[%d] CAS successful.\n", index)
			} else {
				fmt.Printf("Decrement index[%d] CAS failure.\n", index)
			}
		}(i)
	}

	Sem.V(SemNumber)

	log.Println("Starting increment CAS operation.")
	SemNumber = 0

	// One goroutine will fail to increment
	for i := 1; i < n; i++ {
		SemNumber++
		go func(index int) {
			defer Sem.P(1)
			// Increment the value of the key "lizhaolong" when it is less than old(8), increment by new(1) each time,
			ok := clientConfigs[index].CompareAndSwap("lizhaolong", 8, 1, BaseServer.Add)
			if ok {
				fmt.Printf("Increment index[%d] CAS successful.\n", index)
			} else {
				fmt.Printf("Increment index[%d] CAS failure.\n", index)
			}
		}(i)
	}

	Sem.V(SemNumber)

	log.Println("Starting normal CAS operation.")

	SemNumber = 0

	// Only one thread can execute successfully
	for i := 1; i < n; i++ {
		SemNumber++
		go func(index int) {
			defer Sem.P(1)
			// If the old value is old(8), replace it with New(10)
			ok := clientConfigs[index].CompareAndSwap("lizhaolong", 8, 10, BaseServer.Cas)
			if ok {
				fmt.Printf("Normal CAS index[%d] CAS successful.\n", index)
			} else {
				fmt.Printf("Normal CAS index[%d] CAS failure.\n", index)
			}
		}(i)
	}

	Sem.V(SemNumber)

	res := clientConfigs[0].Get("lizhaolong")

	// Should get 10
	fmt.Printf("res : %s.\n", res)
}
