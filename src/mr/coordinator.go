package mr

import (
	"fmt"
	"log"
	"math"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	lock           sync.Mutex
	state          string // if state is "" means to exit.
	nMap           int    // nMap is the number of Map task.
	nReduce        int    // nReduce is the number of Reduce task.
	tasks          map[string]Task
	availableTasks chan Task
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
//func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
//	reply.Y = args.X + 1
//	return nil
//}

func (c *Coordinator) ApplyForTask(args *ApplyForTaskArgs, reply *ApplyForTaskReply) error {
	if args.LastTaskType != "" {
		c.lock.Lock()
		lastTaskID := GenTaskID(args.LastTaskType, args.LastTaskIndex)
		if task, exists := c.tasks[lastTaskID]; exists && task.WorkerId == args.WorkerID {
			log.Printf(
				"Mark %s task %d as finished on worker %s\n",
				task.Type, task.Index, args.WorkerID)
			if args.LastTaskType == MAP {
				for ri := 0; ri < c.nReduce; ri++ {
					err := os.Rename(
						tmpMapOutputFile(args.WorkerID, args.LastTaskIndex, ri),
						finalMapOutputFile(args.LastTaskIndex, ri))
					if err != nil {
						log.Fatalf(
							"Failed to mark map output file `%s` as final: %e",
							tmpMapOutputFile(args.WorkerID, args.LastTaskIndex, ri), err)
					}
				}
			} else if args.LastTaskType == REDUCE {
				err := os.Rename(
					tmpReduceOutputFile(args.WorkerID, args.LastTaskIndex),
					finalReduceOutputFile(args.LastTaskIndex))
				if err != nil {
					log.Fatalf(
						"Failed to mark reduce output file `%s` as final: %e",
						tmpReduceOutputFile(args.WorkerID, args.LastTaskIndex), err)
				}
			}
			delete(c.tasks, lastTaskID)

			if len(c.tasks) == 0 {
				c.transit()
			}
		}
		c.lock.Unlock()
	}
	task, ok := <-c.availableTasks
	if !ok {
		return nil
	}

	c.lock.Lock()
	defer c.lock.Unlock()
	log.Printf("Assign %s task %d to worker %s\n", task.Type, task.Index, args.WorkerID)
	task.WorkerId = args.WorkerID
	task.Deadline = time.Now().Add(10 * time.Second)
	c.tasks[GenTaskID(task.Type, task.Index)] = task
	reply.TaskType = task.Type
	reply.TaskIndex = task.Index
	reply.MapInputFile = task.MapInputFile
	reply.MapNum = c.nMap
	reply.ReduceNum = c.nReduce
	return nil
}

func (c *Coordinator) transit() {
	if c.state == MAP {
		log.Printf("All MAP tasks finished. Transit to REDUCE stage\n")
		c.state = REDUCE

		// 生成 Reduce Task
		for i := 0; i < c.nReduce; i++ {
			task := Task{
				Type:  REDUCE,
				Index: i,
			}
			c.tasks[GenTaskID(task.Type, task.Index)] = task
			c.availableTasks <- task
		}
	} else if c.state == REDUCE {
		log.Printf("All REDUCE tasks finished. Prepare to exit\n")
		close(c.availableTasks)
		c.state = "" // 使用空字符串标记作业完成
	}
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
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.state == ""
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		state:          MAP,
		nMap:           len(files),
		nReduce:        nReduce,
		tasks:          make(map[string]Task),
		availableTasks: make(chan Task, int(math.Max(float64(len(files)), float64(nReduce)))),
	}
	for i, file := range files {
		task := Task{
			Type:         MAP,
			Index:        i,
			MapInputFile: file,
		}
		c.tasks[GenTaskID(task.Type, task.Index)] = task
		c.availableTasks <- task
	}
	log.Printf("Coordinator start\n")
	c.server()

	go func() {
		for {
			time.Sleep(500 * time.Millisecond)
			c.lock.Lock()
			for _, task := range c.tasks {
				if task.WorkerId != "" && time.Now().After(task.Deadline) {
					log.Printf(
						"Found timed-out %s task %d previously on worker %s. Prepare to re-assign",
						task.Type, task.Index, task.WorkerId)
					task.WorkerId = ""
					c.availableTasks <- task
				}
			}
			c.lock.Unlock()
		}
	}()
	return &c
}

func GenTaskID(t string, index int) string {
	return fmt.Sprintf("%s-%d", t, index)
}
