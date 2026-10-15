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

var coordSockName string // socket for coordinator

// main/mrworker.go calls this function.
func Worker(sockname string, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	coordSockName = sockname

	for {
		reply := RPCCall()
		switch reply.Type {
		case MapTask:
			doMap(reply, mapf)
			reportDone(MapTask, reply.TaskNum)
		case ReduceTask:
			doReduce(reply, reducef)
			reportDone(ReduceTask, reply.TaskNum)
		case WaitTask:
			time.Sleep(time.Second)
		case ExitTask:
			return
		}
	}
}

func RPCCall() Reply {
	args := Args{}
	reply := Reply{}
	ok := call("Coordinator.RequestTask", &args, &reply)
	if ok {
		return reply
	}
	// coordinator unreachable: assume the job is done
	return Reply{Type: ExitTask}
}

func doMap(reply Reply, mapf func(string, string) []KeyValue) {
	content, err := os.ReadFile(reply.Filename)
	if err != nil {
		log.Fatalf("cannot read %v", reply.Filename)
	}
	kva := mapf(reply.Filename, string(content))

	buckets := make([][]KeyValue, reply.NReduce)
	for _, kv := range kva {
		y := ihash(kv.Key) % reply.NReduce
		buckets[y] = append(buckets[y], kv)
	}

	for y, bucket := range buckets {
		tmp, err := os.CreateTemp(".", "mr-tmp-*")
		if err != nil {
			log.Fatalf("cannot create temp file")
		}
		enc := json.NewEncoder(tmp)
		for _, kv := range bucket {
			if err := enc.Encode(&kv); err != nil {
				log.Fatalf("cannot encode to %v", tmp.Name())
			}
		}
		tmp.Close()
		filename := fmt.Sprintf("mr-%d-%d", reply.TaskNum, y)
		os.Rename(tmp.Name(), filename)
	}
}

func doReduce(reply Reply, reducef func(string, []string) string) {
	var kva []KeyValue
	for x := 0; x < reply.NMap; x++ {
		filename := fmt.Sprintf("mr-%d-%d", x, reply.TaskNum)
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
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

	sort.Slice(kva, func(i, j int) bool {
		return kva[i].Key < kva[j].Key
	})

	oname := fmt.Sprintf("mr-out-%d", reply.TaskNum)
	tmp, err := os.CreateTemp(".", "mr-tmp-*")
	if err != nil {
		log.Fatalf("cannot create temp file")
	}

	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)
		fmt.Fprintf(tmp, "%v %v\n", kva[i].Key, output)
		i = j
	}

	tmp.Close()
	os.Rename(tmp.Name(), oname)
}

func reportDone(t TaskType, taskNum int) {
	args := DoneArgs{Type: t, TaskNum: taskNum}
	reply := DoneReply{}
	call("Coordinator.TaskDone", &args, &reply)
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	c, err := rpc.DialHTTP("unix", coordSockName)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	if err := c.Call(rpcname, args, reply); err == nil {
		return true
	}
	log.Printf("%d: call failed err %v", os.Getpid(), err)
	return false
}
