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

// Define the type of task that the worker can perform
type TaskType int

const (
	MapTask TaskType = iota
	ReduceTask
	WaitTask
	ExitTask
)

// Used when a worker asks for a task from the coordinator
type TaskRequestArgs struct {
}

// Used when the coordinator replies to a worker with a task
type TaskRequestReply struct {
	Filename string   // name of file to process
	TaskType TaskType // type of task (MAP or REDUCE)
	NReduce  int      // how many buckets for reduce (for MAP tasks)
	NMap     int      // how many intermediate files to read from (for REDUCE tasks)
	TaskID   int      // unique ID for the task
}

// Used when a worker tells the coordinator it has finished a task
type TaskResultArgs struct {
	TaskID   int      // unique ID for the task
	TaskType TaskType // type of task (MAP or REDUCE)
}

// Used when the coordinator replies to a worker after it has finished a task
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
