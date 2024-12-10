package main

import (
	"ChubbyGo/BaseServer"
	_ "ChubbyGo/Config"
	"ChubbyGo/Connect"
	"fmt"
	"log"
	"time"
)

// The test is divided into two sections: basic lock operations and delayed locking

func main() {
	clientConfigs := Connect.CreateClient()
	err := clientConfigs.StartClient()
	if err != nil {
		log.Println(err.Error())
	}
	clientConfigs.SetUniqueFlake(uint64(0)) // Manually modify this value for multiple tests

	// Open a file and get a handle
	ok, fd := clientConfigs.Open("/ls/ChubbyCell_lizhaolong")
	if ok {
		fmt.Printf("Get fd success, instanceSeq is %d\n", fd.InstanceSeq)
	} else {
		fmt.Printf("Error!\n")
	}

	filename := "text.txt"
	// Create a file in the opened folder
	ok, fileFd := clientConfigs.Create(fd, BaseServer.PermanentFile, filename)
	if ok {
		fmt.Printf("Create file(%s) success, instanceSeq is %d, checksum is %d.\n", filename, fileFd.InstanceSeq, fileFd.ChuckSum)
	} else {
		fmt.Printf("Create Error!\n")
	}

	// Delete the handle, note that the handle is only created by create and deleted by delete
	ok = clientConfigs.Delete(fileFd, BaseServer.Opdelete)
	if ok {
		fmt.Printf("Delete file(%s) success\n", filename)
	} else {
		fmt.Printf("Delete Error!\n")
	}

	// Create the file a second time, the returned instanceSeq is 1
	ok, fileFd = clientConfigs.Create(fd, BaseServer.PermanentFile, filename)
	if ok {
		fmt.Printf("Create file(%s) success, instanceSeq is %d, checksum is %d.\n", filename, fileFd.InstanceSeq, fileFd.ChuckSum)
	} else {
		fmt.Printf("Create Error!\n")
	}

	// Lock the newly created file
	ok, token := clientConfigs.Acquire(fileFd, BaseServer.ReadLock, 0)
	if ok {
		fmt.Printf("Acquire (%s) success, Token is %d\n", filename, token)
	} else {
		fmt.Printf("Acquire Error!\n")
	}

	// Obviously, it's a bit silly to add a read lock after adding a read lock
	ok, token = clientConfigs.Acquire(fileFd, BaseServer.ReadLock, 0)
	if ok {
		fmt.Printf("Acquire (%s) success, Token is %d\n", filename, token)
	} else { // Obviously, you can't add a read lock after adding a write lock
		fmt.Printf("ReadLock Error!\n")
	}

	// Delete the file with the token you locked yourself
	ok = clientConfigs.Release(fileFd, token)
	if ok {
		fmt.Printf("release (%s) success.\n", filename)
	} else {
		fmt.Printf("Release Error!\n")
	}

	ok = clientConfigs.Release(fileFd, token)
	if ok {
		fmt.Printf("release (%s) success.\n", filename)
	} else {
		fmt.Printf("Release Error!\n")
	}

	ok, token = clientConfigs.Acquire(fileFd, BaseServer.WriteLock, 1000)
	if ok {
		fmt.Printf("Acquire (%s) success, Token is %d\n", filename, token)
	} else { // Obviously, you can't add a read lock after adding a write lock
		fmt.Printf("WriteLock Error!\n")
	}
	// There will be problems when requesting data with this token after timeout, TODO but the request data with token has not been implemented yet

	// Fail first
	ok, token = clientConfigs.Acquire(fileFd, BaseServer.WriteLock, 0)
	if ok {
		fmt.Printf("Acquire (%s) success, Token is %d\n", filename, token)
	} else { // Obviously, you can't add a read lock after adding a write lock
		fmt.Printf("WriteLock Error!\n")
	}

	// After 2000ms, you can lock again successfully because the previous lock has timed out
	time.Sleep(2000 * time.Millisecond)

	ok, token = clientConfigs.Acquire(fileFd, BaseServer.WriteLock, 0)
	if ok {
		fmt.Printf("Acquire (%s) success, Token is %d\n", filename, token)
	} else { // Obviously, you can't add a read lock after adding a write lock
		fmt.Printf("WriteLock Error!\n")
	}

	ok = clientConfigs.Release(fileFd, token)
	if ok {
		fmt.Printf("release (%s) success.\n", filename)
	} else {
		fmt.Printf("Release Error!\n")
	}

	// Using the token that has been unlocked above should return false
	ok = clientConfigs.CheckToken(fileFd.PathName, token)
	if ok {
		fmt.Printf("CheckToken error! filename(%s)\n", fileFd.PathName)
	} else {
		fmt.Printf("CheckToken success!\n")
	}

	// Finally, delete the file to facilitate testing lock_expand.go
	ok = clientConfigs.Delete(fileFd, BaseServer.Opdelete)
	if ok {
		fmt.Printf("Delete file(%s) success\n", filename)
	} else {
		fmt.Printf("Delete Error!\n")
	}

	return
}
