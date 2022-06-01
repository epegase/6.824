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

type TaskType int

const (
	TaskTypeNone TaskType = iota
	TaskTypeMap
	TaskTypeReduce
	TaskTypeSleep
	TaskTypeExit
)

// finished task
type TaskArgs struct {
	DoneType TaskType
	Id       int
	Files    []string
}

// new task
type TaskReply struct {
	Type    TaskType
	Id      int
	Files   []string
	NReduce int
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
