package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

// Task struct
type MRTask struct {
	TaskType    string // Map / Reduce / Wait
	Index       int
	NReduce     int
	NFile       int
	MapFile     string
	ReduceFiles []string
	Status      string // false for unassigned, true for assigned
}

// MapReduceArgs struct
type MapReduceArgs struct {
	InfoType string //
	Index    int
}

// MapReduceReply struct
type MapReduceReply struct {
	ReplyTask MRTask
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
