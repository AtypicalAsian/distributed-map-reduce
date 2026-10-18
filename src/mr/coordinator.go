package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

// Used as shared state between the coordinator and workers. Keeps track of the tasks and their statuses.
type Coordinator struct {
	mapTasks    []Task // list of tasks for map tasks
	reduceTasks []Task // list of tasks for reduce tasks
	nReduce     int
	mutex       sync.Mutex // lock to protect shared state from race conditions
}

type Task struct {
	taskID     int
	taskStatus TaskStatus
	startTime  time.Time
	filename   string // for map tasks only
}

type TaskStatus int

const (
	Idle TaskStatus = iota
	InProgress
	Completed
)

//Need to implement
// 1. Assign Task RPC handler
// 2. Report task RPC handler
// 3. Done() method to check if all tasks are done
// 4. Failure detection: if a job isn't done in 10 seconds, mark it as failed and reassign
// 5. State management: keep track of which tasks are assigned, completed, and failed.
// 		Only hand out reduce tasks when all map tasks are completed
// 5. Locking: mutex to lock the shared state to prevent race conditions when updating shared state

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.

	c.server()
	return &c
}
