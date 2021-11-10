package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"fmt"
	"os"
	"time"
)
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

const (
	MAP    = "MAP"
	REDUCE = "REDUCE"
)

type Task struct {
	Type string // Task type Map or Reduce

	Index        int //
	MapInputFile string

	WorkerId string
	Deadline time.Time // set timeout
}

type ApplyForTaskArgs struct {
	WorkerID string

	LastTaskType  string
	LastTaskIndex int
}

type ApplyForTaskReply struct {
	TaskType  string // TaskType Map or Reduce
	TaskIndex int

	MapInputFile string // input file of task
	MapNum       int    // Map number
	ReduceNum    int    // Reduce number
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

func tmpMapOutputFile(worker string, mapIndex int, reduceIndex int) string {
	return fmt.Sprintf("tmp-worker-%s-%d-%d", worker, mapIndex, reduceIndex)
}

func finalMapOutputFile(mapIndex int, reduceIndex int) string {
	return fmt.Sprintf("mr-%d-%d", mapIndex, reduceIndex)
}

func tmpReduceOutputFile(worker string, reduceIndex int) string {
	return fmt.Sprintf("tmp-worker-%s-%d", worker, reduceIndex)
}

func finalReduceOutputFile(reduceIndex int) string {
	return fmt.Sprintf("mr-out-%d", reduceIndex)
}
