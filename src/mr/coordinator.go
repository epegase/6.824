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

type MapTask struct {
	id      int       // task id
	file    string    // map task input file
	startAt time.Time // task start time (for timeout)
	done    bool      // is task finished
}

type ReduceTask struct {
	id      int       // task id
	files   []string  // reduce task input files (M files)
	startAt time.Time // task start time (for timeout)
	done    bool      // is task finished
}

type Coordinator struct {
	// Your definitions here.
	mutex        sync.Mutex   // lock to protect shared data below
	mapTasks     []MapTask    // all map tasks
	mapRemain    int          // # of remaining map tasks
	reduceTasks  []ReduceTask // all reduce tasks
	reduceRemain int          // # of remaining reduce tasks
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) GetTask(args *TaskArgs, reply *TaskReply) error {
	// lock to protect shared data
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// record result from worker for its finished task
	switch args.DoneType {
	case TaskTypeMap:
		if !c.mapTasks[args.Id].done {
			c.mapTasks[args.Id].done = true
			// distribute reduce files from map task result to each corresponding reduce task
			for reduceId, file := range args.Files {
				if len(file) > 0 {
					c.reduceTasks[reduceId].files = append(c.reduceTasks[reduceId].files, file)
				}
			}
			c.mapRemain--
		}
	case TaskTypeReduce:
		if !c.reduceTasks[args.Id].done {
			c.reduceTasks[args.Id].done = true
			c.reduceRemain--
		}
	}

	/**
	log.Printf("Remaining map: %d, reduce: %d\n", c.mapRemain, c.reduceRemain)
	defer log.Printf("Distribute task %v\n", reply)
	*/

	now := time.Now()
	timeoutAgo := now.Add(-10 * time.Second)
	if c.mapRemain > 0 { // has remaining map task unfinished
		for idx := range c.mapTasks {
			t := &c.mapTasks[idx]
			// unfinished and timeout
			if !t.done && t.startAt.Before(timeoutAgo) {
				reply.Type = TaskTypeMap
				reply.Id = t.id
				reply.Files = []string{t.file}
				reply.NReduce = len(c.reduceTasks)

				t.startAt = now

				return nil
			}
		}
		// find no unfinished map task, ask worker to sleep for a while
		reply.Type = TaskTypeSleep
	} else if c.reduceRemain > 0 { // has remaining reduce task unfinished
		for idx := range c.reduceTasks {
			t := &c.reduceTasks[idx]
			// unfinished and timeout
			if !t.done && t.startAt.Before(timeoutAgo) {
				reply.Type = TaskTypeReduce
				reply.Id = t.id
				reply.Files = t.files

				t.startAt = now

				return nil
			}
		}
		// find no unfinished reduce task, ask worker to sleep for a while
		reply.Type = TaskTypeSleep
	} else { // both map phase and reduce phase finished, ask worker to terminate itself
		reply.Type = TaskTypeExit
	}
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	// Your code here.
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return c.mapRemain == 0 && c.reduceRemain == 0
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		mapTasks:     make([]MapTask, len(files)),
		reduceTasks:  make([]ReduceTask, nReduce),
		mapRemain:    len(files),
		reduceRemain: nReduce,
	}

	// Your code here.
	/**
	log.Printf(
		"Coordinator has %d map tasks and %d reduce tasks to distribute\n",
		c.mapRemain,
		c.reduceRemain,
	)
	*/

	// init map tasks
	for i, f := range files {
		c.mapTasks[i] = MapTask{id: i, file: f, done: false}
	}
	// init reduce tasks
	for i := 0; i < nReduce; i++ {
		c.reduceTasks[i] = ReduceTask{id: i, done: false}
	}

	c.server()
	return &c
}
