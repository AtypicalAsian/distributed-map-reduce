package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//

func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		args := TaskRequestArgs{}
		reply := TaskRequestReply{}
		ok := call("Coordinator.AssignTask", &args, &reply)
		if !ok {
			return // something went wrong with call to coordinator -> assume job is done.
		}

		switch reply.TaskType {
		case MapTask:
			err := RunMapTask(mapf, reply.Filename, reply.TaskID, reply.NReduce)
			if err != nil {
				log.Printf("Error running map task: %v", err)
				continue
			}

			// Call coordinator to tell it that the task is done
			resultArgs := TaskResultArgs{
				TaskID:   reply.TaskID,
				TaskType: MapTask,
			}
			resultReply := TaskResultReply{}
			call("Coordinator.ReportTask", &resultArgs, &resultReply)
		case ReduceTask:
			err := RunReduceTask(reducef, reply.TaskID, reply.NMap)
			if err != nil {
				log.Printf("Error running reduce task: %v", err)
				continue
			}
			// Call coordinator to tell it that the task is done
			resultArgs := TaskResultArgs{
				TaskID:   reply.TaskID,
				TaskType: ReduceTask,
			}
			resultReply := TaskResultReply{}
			call("Coordinator.ReportTask", &resultArgs, &resultReply)
		case WaitTask:
			// Wait for a second before asking for a new task
			time.Sleep(time.Second)
		case ExitTask:
			return
		default:
			log.Fatalf("Unknown task type: %v", reply.TaskType)
		}
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func RunMapTask(mapf func(string, string) []KeyValue, filename string, taskId int, nReduce int) error {
	// Read input
	content, err := os.ReadFile(filename)
	if err != nil {
		return fmt.Errorf("cannot read %v: %v", filename, err)
	}
	contents := string(content)

	// Call mapf on input -> get a list of KeyValue pairs
	kvPairs := mapf(filename, contents)

	// Compute each key's hash -> Partition KV list into nReduce buckets
	buckets := make([][]KeyValue, nReduce)
	for _, kv := range kvPairs {
		bucketIndex := ihash(kv.Key) % nReduce
		buckets[bucketIndex] = append(buckets[bucketIndex], kv)
	}
	// Write each bucket to a separate intermediate file in format mr-X-Y
	// X = taskId, Y = bucket index
	for y, bucket := range buckets {
		tmp, err := os.CreateTemp(".", "mr-tmp-*")
		if err != nil {
			return fmt.Errorf("cannot create temp file: %v", err)
		}

		enc := json.NewEncoder(tmp)
		for _, kv := range bucket {
			if err := enc.Encode(&kv); err != nil {
				tmp.Close()
				return fmt.Errorf("cannot encode kv: %v", err)
			}
		}

		if err := tmp.Close(); err != nil {
			return fmt.Errorf("cannot close temp file: %v", err)
		}

		// we don't create the intermediate file with the correct name at first to avoid
		// the possibility of another Reduce worker reading an incomplete file.
		name := fmt.Sprintf("mr-%d-%d", taskId, y)
		if err := os.Rename(tmp.Name(), name); err != nil {
			return fmt.Errorf("cannot rename %v to %v: %v", tmp.Name(), name, err)
		}
	}

	return nil
}

func RunReduceTask(reducef func(string, []string) string, taskId int, nMap int) error {
	// Read all intermediate files for this reduce task
	// Bucket Y = task ID for reduce worker
	var kva []KeyValue
	for x := 0; x < nMap; x++ {
		filename := fmt.Sprintf("mr-%d-%d", x, taskId)
		file, err := os.Open(filename)
		if err != nil {
			return fmt.Errorf("cannot open %v: %v", filename, err)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}

	// Group values by key
	// If we sort the kva slice by key, the same keys will be adjacent -> can group them via a scan
	sort.Slice(kva, func(i, j int) bool { return kva[i].Key < kva[j].Key })

	// Call reducef for each key group and write to output file
	tmp, err := os.CreateTemp(".", "mr-tmp-*")
	if err != nil {
		return fmt.Errorf("cannot create temp file: %v", err)
	}

	iterator := 0
	for iterator < len(kva) {
		j := iterator + 1

		// Collect all values for the same key
		values := []string{}
		for j < len(kva) && kva[j].Key == kva[iterator].Key {
			j++
		}
		for k := iterator; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		currKey := kva[iterator].Key
		output := reducef(currKey, values)

		// this is the correct format for each line of Reduce output
		fmt.Fprintf(tmp, "%v %v\n", currKey, output)
		iterator = j
	}

	if err := tmp.Close(); err != nil {
		return fmt.Errorf("cannot close temp file: %v", err)
	}

	// rename only once fully written, so no one sees a partial output file
	name := fmt.Sprintf("mr-out-%d", taskId)
	if err := os.Rename(tmp.Name(), name); err != nil {
		return fmt.Errorf("cannot rename %v to %v: %v", tmp.Name(), name, err)
	}

	return nil

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
