package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
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

	// Your worker implementation here.

	reply := RPCCall()
	if reply.Filename != "" {
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
			filename := fmt.Sprintf("mr-%d-%d", reply.TaskNum, y)
			file, err := os.Create(filename)
			if err != nil {
				log.Fatalf("cannot create %v", filename)
			}
			enc := json.NewEncoder(file)
			for _, kv := range bucket {
				err := enc.Encode(&kv)
				if err != nil {
					log.Fatalf("cannot encode")
				}
			}
			file.Close()

		}

		fmt.Printf("Filename: %v KVAL: %v\n", reply.Filename, len(kva))
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func RPCCall() Reply {
	args := Args{}

	reply := Reply{}

	ok := call("Coordinator.RequestTask", &args, &reply)
	if ok {
		return reply
	} else {
		return Reply{}
	}

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.

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
