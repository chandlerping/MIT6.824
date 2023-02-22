package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

// Master struct
type Master struct {
	// Your definitions here.
	NReduce int
	NFile   int

	MapTasks    []MRTask
	ReduceTasks []MRTask

	MapFinish    int
	ReduceFinish int

	mu sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

// MapReduceHandler handles the rpc requests
func (m *Master) MapReduceHandler(args *MapReduceArgs, reply *MapReduceReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if args.InfoType == "Request" {
		if m.MapFinish < m.NFile {
			for i := 0; i < m.NFile; i++ {
				if m.MapTasks[i].Status == "Unassigned" {
					m.MapTasks[i].Status = "Assigned"
					reply.ReplyTask = m.MapTasks[i]
					go m.checkTimeout("Map", i, 10)
					return nil
				}
			}
			reply.ReplyTask.TaskType = "Wait"
			return nil
		} else if m.ReduceFinish < m.NReduce {
			for i := 0; i < m.NReduce; i++ {
				if m.ReduceTasks[i].Status == "Unassigned" {
					m.ReduceTasks[i].Status = "Assigned"
					reply.ReplyTask = m.ReduceTasks[i]
					go m.checkTimeout("Reduce", i, 10)
					return nil
				}
			}
			reply.ReplyTask.TaskType = "Wait"
			return nil
		} else {
			reply.ReplyTask.TaskType = "Kill"
			return nil
		}
	} else if args.InfoType == "Finish" {
		if m.MapFinish < m.NFile {
			m.MapTasks[args.Index].Status = "Finished"
			m.MapFinish++
		} else if m.ReduceFinish < m.NReduce {
			m.ReduceTasks[args.Index].Status = "Finished"
			m.ReduceFinish++
		}
	}
	return nil
}

// checkTimeout checks whether the workers are off
func (m *Master) checkTimeout(taskType string, id int, sec int) {
	// fmt.Println("Will it be executed?")

	time.Sleep(time.Duration(sec) * time.Second)

	// fmt.Println("Yes, it is!")

	m.mu.Lock()
	defer m.mu.Unlock()

	if taskType == "Map" && m.MapTasks[id].Status == "Assigned" {
		m.MapTasks[id].Status = "Unassigned"
	} else if taskType == "Reduce" && m.ReduceTasks[id].Status == "Assigned" {
		m.ReduceTasks[id].Status = "Unassigned"
	}
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	// ret := false

	// Your code here.
	return m.ReduceFinish == m.NReduce
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	fmt.Println("nReduce", nReduce)

	// Your code here.
	// fmt.Println(files)
	m.NReduce = nReduce
	m.NFile = len(files)
	m.MapFinish = 0
	m.ReduceFinish = 0

	for i := 0; i < m.NFile; i++ {
		mapTask := MRTask{}
		mapTask.Index = i
		mapTask.TaskType = "Map"
		mapTask.MapFile = files[i]
		mapTask.NReduce = m.NReduce
		mapTask.NFile = m.NFile
		mapTask.Status = "Unassigned"
		m.MapTasks = append(m.MapTasks, mapTask)
	}

	for i := 0; i < nReduce; i++ {
		reduceTask := MRTask{}
		reduceTask.Index = i
		reduceTask.TaskType = "Reduce"
		reduceTask.NFile = m.NFile
		reduceTask.Status = "Unassigned"
		for j := 0; j < m.NFile; j++ {
			IntermediateFile := GenerateIntermediate(j, i)
			reduceTask.ReduceFiles = append(reduceTask.ReduceFiles, IntermediateFile)
		}
		m.ReduceTasks = append(m.ReduceTasks, reduceTask)
	}

	m.server()
	return &m
}
