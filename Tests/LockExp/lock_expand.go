package main

import (
	"ChubbyGo/BaseServer"
	_ "ChubbyGo/Config"
	"ChubbyGo/Connect"
	"fmt"
	"log"
	"time"
)

/*
 * The test is divided into two parts, the first part is for write lock testing, and the second part is for read lock testing
 */

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
			clientConfigs[i].SetUniqueFlake(uint64(i + n*20))
		}
	}

	ok, fd := clientConfigs[0].Open("/ls/ChubbyCell_lizhaolong")
	if ok {
		fmt.Printf("Get fd success, instanceSeq is %d\n", fd.InstanceSeq)
	} else {
		fmt.Printf("Error!")
	}

	filename := "text.txt"
	// Create a file in the opened folder
	flag, fileFd := clientConfigs[0].Create(fd, BaseServer.PermanentFile, filename)
	if flag {
		fmt.Printf("Create file(%s) success, instanceSeq is %d\n", filename, fileFd.InstanceSeq)
	} else {
		fmt.Printf("Create Error!")

		// To prevent someone from defining the Delete parameter at the bottom as Opclose and not finding the error reason and blaming me
		flag = clientConfigs[0].Delete(fileFd, BaseServer.Opdelete)
		if flag {
			fmt.Printf("Delete (%s) success.\n", fileFd.PathName)
		} else {
			fmt.Println("Release Error!")
		}
		return
	}

	// Only one of the multiple goroutines can successfully acquire the write lock, but this is not absolute; here there will be N-1 goroutines entering
	for i := 1; i < n; i++ {
		SemNumber++
		go func(index int) {
			defer Sem.P(1)
			flag, Fd := clientConfigs[index].Open("/ls/ChubbyCell_lizhaolong/text.txt")
			if flag {
				fmt.Printf("Get fd success, instanceSeq is %d, checksum is %d.\n", Fd.InstanceSeq, Fd.ChuckSum)
			} else {
				fmt.Println("Error!")
			}

			time.Sleep(1 * time.Second)

			Isok, token := clientConfigs[index].Acquire(Fd, BaseServer.WriteLock, 0)
			if Isok {
				fmt.Printf("Acquire (%s) success, Token is %d\n", filename, token)
				//time.Sleep(1*time.Second)

				Isok = clientConfigs[index].Release(Fd, token)
				if Isok {
					fmt.Printf("Release (%s) success.\n", Fd.PathName)
				} else {
					fmt.Println("Release Error!")
				}
			} else {
				fmt.Println("Acquire Error!")
			}

			// Close operation, do not delete the file, this Delete may be confusing, but to make the function implementation simpler, adding parameters is a good choice
			flag = clientConfigs[index].Delete(Fd, BaseServer.Opclose)
			if flag {
				fmt.Printf("Delete (%s) success.\n", Fd.PathName)
			} else {
				fmt.Println("Release Error!")
			}
		}(i)
	}

	Sem.V(SemNumber)

	fmt.Println("----------------------------------------------")

	// Use client 0 to request the lock again, the token obtained should be 3, for design reasons, the initial token is set to 2
	Isok, token := clientConfigs[0].Acquire(fileFd, BaseServer.WriteLock, 0)
	if Isok {
		fmt.Printf("Acquire (%s) success, Token is %d\n", filename, token)
		//time.Sleep(1*time.Second)

		Isok = clientConfigs[0].Release(fileFd, token)
		if Isok {
			fmt.Printf("Release (%s) success.\n", fileFd.PathName)
		} else {
			fmt.Println("Release Error!")
		}
	} else {
		fmt.Println("Acquire Error!")
	}

	fmt.Println("----------------------------------------------")
	SemNumber = 0

	for i := 1; i < n; i++ {
		SemNumber++
		go func(index int) {
			defer Sem.P(1)
			flag, Fd := clientConfigs[index].Open("/ls/ChubbyCell_lizhaolong/text.txt")
			if flag {
				fmt.Printf("Get fd success, instanceSeq is %d, checksum is %d.\n", Fd.InstanceSeq, Fd.ChuckSum)
			} else {
				fmt.Println("Error!")
			}

			var Isok bool
			var token uint64

			time.Sleep(1 * time.Second)

			// If the read lock is acquired first, there will be 4 successes; if the write lock is acquired, there will be 1 success
			if index&1 == 0 {
				Isok, token = clientConfigs[index].Acquire(Fd, BaseServer.ReadLock, 0)
			} else {
				Isok, token = clientConfigs[index].Acquire(Fd, BaseServer.WriteLock, 0)
			}

			if Isok {
				fmt.Printf("Acquire (%s) success, Token is %d\n", filename, token)
				//time.Sleep(1*time.Second)

				Isok = clientConfigs[index].Release(Fd, token)
				if Isok {
					fmt.Printf("Release (%s) success.\n", Fd.PathName)
				} else {
					fmt.Println("Release Error!")
				}
			} else {
				fmt.Println("Acquire Error!")
			}

			// Close operation, do not delete the file, this Delete may be confusing, but to make the function implementation simpler, adding parameters is a good choice
			flag = clientConfigs[index].Delete(Fd, BaseServer.Opclose)
			if flag {
				fmt.Printf("Delete (%s) success.\n", Fd.PathName)
			} else {
				fmt.Println("Delete Error!")
			}
		}(i)
	}

	Sem.V(SemNumber)

	// Change to Opclose and run again, you will find that the file creation failed; setting delete here is to run this program multiple times
	flag = clientConfigs[0].Delete(fileFd, BaseServer.Opdelete)
	if flag {
		fmt.Printf("Delete (%s) success.\n", fileFd.PathName)
	} else {
		fmt.Println("Release Error!")
	}

}
