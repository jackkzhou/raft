package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

// Add your RPC definitions here.

type Args struct {
}

type Reply struct {
	Filename string
	TaskNum  int
	NReduce  int
}
