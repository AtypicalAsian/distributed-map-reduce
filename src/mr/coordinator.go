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

// Coordinator tracks every map and reduce task. Its state is shared by
// concurrent RPC handlers, so all access goes through mutex.
type Coordinator struct {
	mapTasks    []Task // one per input file
	reduceTasks []Task // one per bucket
	nReduce     int
	mutex       sync.Mutex
}

type Task struct {
	taskID     int
	taskStatus TaskStatus
	startTime  time.Time
	filename   string // map tasks only
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

// AssignTask hands the calling worker one task. Tasks are available if they
// are idle, or if they have been in progress for over 10s (worker presumed dead).
// Reduce tasks are only handed out once every map task has completed.
func (c *Coordinator) AssignTask(args *TaskRequestArgs, reply *TaskRequestReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// Map phase
	for i, task := range c.mapTasks {
		if task.taskStatus == Idle || (task.taskStatus == InProgress && time.Since(task.startTime) > 10*time.Second) {
			reply.Filename = task.filename
			reply.TaskType = MapTask
			reply.NReduce = c.nReduce
			reply.NMap = len(c.mapTasks)
			reply.TaskID = task.taskID

			c.mapTasks[i].taskStatus = InProgress
			c.mapTasks[i].startTime = time.Now()

			return nil
		}
	}

	// All map tasks are taken but some are still running: wait before reducing.
	for _, task := range c.mapTasks {
		if task.taskStatus != Completed {
			reply.TaskType = WaitTask
			return nil
		}
	}

	// Reduce phase
	for i, task := range c.reduceTasks {
		if task.taskStatus == Idle || (task.taskStatus == InProgress && time.Since(task.startTime) > 10*time.Second) {
			reply.TaskType = ReduceTask
			reply.TaskID = task.taskID
			reply.NMap = len(c.mapTasks)

			c.reduceTasks[i].taskStatus = InProgress
			c.reduceTasks[i].startTime = time.Now()

			return nil
		}
	}

	for _, task := range c.reduceTasks {
		if task.taskStatus != Completed {
			reply.TaskType = WaitTask
			return nil
		}
	}

	reply.TaskType = ExitTask
	return nil
}

// ReportTask marks a task completed. Duplicate reports (from a slow worker
// whose task was reassigned) are harmless.
func (c *Coordinator) ReportTask(args *TaskResultArgs, reply *TaskResultReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

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

	c.mutex.Lock()
	defer c.mutex.Unlock()

	// Reduce tasks can't start until all map tasks finish,
	// so checking reduce tasks alone is enough.
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

	c.mapTasks = make([]Task, len(files))
	for i, file := range files {
		c.mapTasks[i].filename = file
		c.mapTasks[i].taskID = i
	}

	c.reduceTasks = make([]Task, nReduce)
	for i := range nReduce {
		c.reduceTasks[i].taskID = i
	}

	c.nReduce = nReduce

	c.server()
	return &c
}
