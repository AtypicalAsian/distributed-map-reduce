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

// Used as shared state. Keeps track of the tasks and their statuses.
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

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// AssignTask RPC Handler
func (c *Coordinator) AssignTask(args *TaskRequestArgs, reply *TaskRequestReply) error {
	//Acquire lock
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// MAP PHASE

	// Go through map tasks ID, if a task is idle OR in progress for more than 10 seconds
	// -> assign to a new worker + mark task as in progress + set start time to now
	for i, task := range c.mapTasks {
		if task.taskStatus == Idle || (task.taskStatus == InProgress && time.Since(task.startTime) > 10*time.Second) {
			// Assign task to worker
			reply.Filename = task.filename
			reply.TaskType = MapTask
			reply.NReduce = c.nReduce
			reply.NMap = len(c.mapTasks)
			reply.TaskID = task.taskID

			// Mark task as in progress and set start time to now
			c.mapTasks[i].taskStatus = InProgress
			c.mapTasks[i].startTime = time.Now()

			return nil
		}
	}

	// If all tasks pending (not complete) -> no more to assign -> wait the worker
	for _, task := range c.mapTasks {
		if task.taskStatus != Completed { // if any task is still not completed but none left to give out
			reply.TaskType = WaitTask
			return nil
		}
	}

	// If all map tasks completed -> exit loop and move to reduce phase

	// REDUCE PHASE
	// Go through reduce tasks ID, if a task is idle OR in progress for more than 10 seconds
	// -> assign to a new worker + mark task as in progress + set start time to now
	for i, task := range c.reduceTasks {
		if task.taskStatus == Idle || (task.taskStatus == InProgress && time.Since(task.startTime) > 10*time.Second) {
			// Assign to new worker
			reply.TaskType = ReduceTask
			reply.TaskID = task.taskID
			reply.NMap = len(c.mapTasks)

			// Mark task as in progress + set start time to Now
			c.reduceTasks[i].taskStatus = InProgress
			c.reduceTasks[i].startTime = time.Now()

			return nil
		}
	}

	// If no more reduce tasks to assign + not all completed -> wait the worker
	for _, task := range c.reduceTasks {
		if task.taskStatus != Completed {
			reply.TaskType = WaitTask
			return nil
		}
	}

	// If all reduce tasks completed -> Reply ExitTask to worker
	reply.TaskType = ExitTask
	return nil
}

// ReportTask RPC Handler
func (c *Coordinator) ReportTask(args *TaskResultArgs, reply *TaskResultReply) error {
	//Acquire Lock
	c.mutex.Lock()
	defer c.mutex.Unlock()

	//Mark Task as Completed
	switch args.TaskType {
	case MapTask:
		c.mapTasks[args.TaskID].taskStatus = Completed
	case ReduceTask:
		c.reduceTasks[args.TaskID].taskStatus = Completed
	}

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

	//Acquire Lock (since it may read shared state while RPC handlers may be changing it)
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// Only need to check reduce tasks before reduce tasks can't start unless all map tasks complete
	for _, task := range c.reduceTasks {
		if task.taskStatus != Completed {
			return ret
		}
	}
	ret = true
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Create map tasks (one per input file)
	c.mapTasks = make([]Task, len(files))
	for i, file := range files {
		c.mapTasks[i].filename = file
		c.mapTasks[i].taskID = i
	}

	// Create reduce tasks
	c.reduceTasks = make([]Task, nReduce)
	for i := range nReduce {
		c.reduceTasks[i].taskID = i
	}

	// Set nReduce
	c.nReduce = nReduce

	c.server()
	return &c
}
