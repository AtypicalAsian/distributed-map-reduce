package mr

//
// RPC definitions.
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

type TaskType int

const (
	MapTask TaskType = iota
	ReduceTask
	WaitTask // nothing available yet; ask again shortly
	ExitTask // job is finished; worker should exit
)

// Sent by a worker asking for a task. The coordinator decides what to assign,
// so no fields are needed.
type TaskRequestArgs struct {
}

type TaskRequestReply struct {
	Filename string // input file (map tasks only)
	TaskType TaskType
	NReduce  int // number of buckets to partition into (map tasks)
	NMap     int // number of map tasks, i.e. intermediate files to read (reduce tasks)
	TaskID   int
}

// Sent by a worker when it has finished a task.
type TaskResultArgs struct {
	TaskID   int
	TaskType TaskType
}

type TaskResultReply struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
