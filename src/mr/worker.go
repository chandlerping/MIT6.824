package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// ByKey is for sorting by key.
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
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

	// uncomment to send the Example RPC to the master.

	// CallExample()
	flag := 0

	for {
		args := MapReduceArgs{}
		args.InfoType = "Request"
		reply := MapReduceReply{}

		resp := call("Master.MapReduceHandler", &args, &reply)

		if !resp {
			break
		}

		task := reply.ReplyTask

		switch task.TaskType {
		case "Map":
			MapTask(task, mapf)
		case "Reduce":
			ReduceTask(task, reducef)
		case "Wait":
			WaitTask()
		case "Kill":
			flag = 1
		}

		if flag == 1 {
			fmt.Println("Killed")
			break
		}
	}
}

// MapTask does the map work
func MapTask(task MRTask, mapf func(string, string) []KeyValue) {
	filename := task.MapFile
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}

	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()

	kva := mapf(filename, string(content))
	kvaa := make([][]KeyValue, task.NReduce)

	for _, kv := range kva {
		id := ihash(kv.Key) % task.NReduce
		// id := 0
		// fmt.Println(id)
		kvaa[id] = append(kvaa[id], kv)
	}

	for i := 0; i < task.NReduce; i++ {
		intermediateFilename := GenerateIntermediate(task.Index, i)
		intermediateFile, err := os.Create(intermediateFilename)
		if err != nil {
			log.Fatalf("cannot open %v", intermediateFilename)
		}
		enc := json.NewEncoder(intermediateFile)
		for _, kv := range kvaa[i] {
			err2 := enc.Encode(&kv)
			if err2 != nil {
				log.Fatalf("error when writing map results")
			}
		}
		intermediateFile.Close()
	}

	SendFinish(task.Index)
}

// ReduceTask does the reduce work
func ReduceTask(task MRTask, reducef func(string, []string) string) {
	reduceId := task.Index
	var intermediate []KeyValue

	for i := 0; i < task.NFile; i++ {
		intermediateFilename := GenerateIntermediate(i, reduceId)
		intermediateFile, err := os.Open(intermediateFilename)
		if err != nil {
			log.Fatalf("cannot open %v", intermediateFilename)
		}
		dec := json.NewDecoder(intermediateFile)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
	}

	sort.Sort(ByKey(intermediate))

	oname := "mr-out-" + strconv.Itoa(reduceId)
	ofile, _ := os.Create(oname)

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ofile.Close()

	SendFinish(task.Index)
}

// WaitTask waits
func WaitTask() {
	time.Sleep(1 * time.Second)
}

// GenerateIntermediate returns the filename of an intermediate file
func GenerateIntermediate(i int, j int) string {
	return "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(j) + ".json"
}

// SendFinish sends the finish signal
func SendFinish(id int) {
	args := MapReduceArgs{}
	args.InfoType = "Finish"
	args.Index = id
	reply := MapReduceReply{}
	call("Master.MapReduceHandler", &args, &reply)
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
