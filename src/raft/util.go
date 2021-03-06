package raft

import "fmt"

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	/*
		if Debug > 0 {
			log.Printf(format, a...)
		}
	*/
	if Debug > 0 {
		fmt.Printf(format, a...)
	}
	return
}
