package mr

// Add your RPC definitions here.
type TaskState int

const (
	Idle       TaskState = iota // not handed out yet
	InProgress                  // handed to a worker, not reported done
	Completed                   // worker reported it finished
)

// worker → coordinator: "I finished this task"
type DoneArgs struct {
	Type    TaskType
	TaskNum int
}

type DoneReply struct {
}

type TaskType int

const (
	MapTask TaskType = iota
	ReduceTask
	WaitTask
	ExitTask
)

type Args struct {
}

type Reply struct {
	Type     TaskType
	Filename string
	TaskNum  int
	NReduce  int
	NMap     int
}
