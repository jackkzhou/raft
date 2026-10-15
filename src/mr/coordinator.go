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

type Coordinator struct {
	mu          sync.Mutex
	files       []string
	nReduce     int
	mapState    []TaskState // one entry per map task
	reduceState []TaskState // one entry per reduce task
	mapStart    []time.Time // when each map task was last handed out
	reduceStart []time.Time // when each reduce task was last handed out
	mapsDone    int         // how many maps are Completed
	reducesDone int         // how many reduces are Completed
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) RequestTask(args *Args, reply *Reply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.mapsDone < len(c.files) {
		for i, val := range c.mapState {
			if val == Idle || (val == InProgress && time.Since(c.mapStart[i]) > 10*time.Second) {
				c.mapState[i] = InProgress
				c.mapStart[i] = time.Now()
				reply.NReduce = c.nReduce
				reply.TaskNum = i
				reply.Filename = c.files[i]
				reply.Type = MapTask
				return nil
			}
		}
		reply.Type = WaitTask
		return nil
	} else if c.reducesDone < c.nReduce {
		for i, val := range c.reduceState {
			if val == Idle || (val == InProgress && time.Since(c.reduceStart[i]) > 10*time.Second) {
				c.reduceState[i] = InProgress
				c.reduceStart[i] = time.Now()
				reply.Type = ReduceTask
				reply.TaskNum = i
				reply.NMap = len(c.mapState)
				return nil
			}
		}
		reply.Type = WaitTask
		return nil
	}

	reply.Type = ExitTask
	return nil
}

func (c *Coordinator) TaskDone(args *DoneArgs, reply *DoneReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if args.Type == MapTask {
		if c.mapState[args.TaskNum] != Completed {
			c.mapState[args.TaskNum] = Completed
			c.mapsDone++
		}
	} else if args.Type == ReduceTask {
		if c.reduceState[args.TaskNum] != Completed {
			c.reduceState[args.TaskNum] = Completed
			c.reducesDone++
		}
	}
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server(sockname string) {
	rpc.Register(c)
	rpc.HandleHTTP()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatalf("listen error %s: %v", sockname, e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.reducesDone == c.nReduce
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.files = files
	c.nReduce = nReduce
	c.mapState = make([]TaskState, len(files))
	c.reduceState = make([]TaskState, nReduce)
	c.mapStart = make([]time.Time, len(files))
	c.reduceStart = make([]time.Time, nReduce)

	// Your code here.

	c.server(sockname)
	return &c
}
