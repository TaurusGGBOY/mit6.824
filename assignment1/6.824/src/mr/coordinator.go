package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"

// TODO 这个Coordinator有什么好定义的？
type Coordinator struct {
	// Your definitions here.
	mapTasks                   map[string]map[int]int
	reduceTasks                map[string]map[int]int
	reduceTaskNumber           int
	mapWaitingResponseQueue    map[string]map[int]int
	reduceWaitingResponseQueue map[string]map[int]int
	mapTaskNumber              int
	phase                      TaskPhase
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) requestTask(args *RequestWorker, t *Task) error {
	if c.Done() {
		t.alive = false
		return nil
	}
	if c.phase == mapPhase {
		for filename, tasks := range c.mapTasks {
			for _, value := range tasks {
				t.phase = c.phase
				t.fileName = filename
				t.taskNumber = value
				t.alive = true
				t.nMap = c.mapTaskNumber
				break
			}
			break
		}
	} else if c.phase == reducePhase {
		for filename, tasks := range c.reduceTasks {
			for _, value := range tasks {
				t.phase = c.phase
				t.fileName = filename
				t.taskNumber = value
				t.alive = true
				t.nReduce = c.reduceTaskNumber
				break
			}
			break
		}
	}
	return nil
}

// 响应任务
func (c *Coordinator) responseTask(args *Task, reply *ResponseTaskReply) error {
	if args.phase == mapPhase {
		delete(c.mapWaitingResponseQueue[args.fileName], args.taskNumber)
		if len(c.mapWaitingResponseQueue[args.fileName]) == 0 {
			delete(c.mapWaitingResponseQueue, args.fileName)
		}
		if len(c.mapWaitingResponseQueue) == 0 {
			c.phase = reducePhase
		}
	} else if args.phase == reducePhase {
		delete(c.reduceWaitingResponseQueue[args.fileName], args.taskNumber)
		if len(c.reduceWaitingResponseQueue[args.fileName]) == 0 {
			delete(c.reduceWaitingResponseQueue, args.fileName)
		}
	}
	return nil
}

func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
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
	if len(c.mapTasks) == 0 && len(c.reduceTasks) == 0 && len(c.mapWaitingResponseQueue) == 0 && len(c.reduceWaitingResponseQueue) == 0 {
		return true
	}
	return false
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
// TODO 输入是一个文件列表和reduce任务数量？ 返回一个Coordinator
// 是我这边coordinator进行分割？
// 还是说这个已经分割好了 直接可以分发给worker？
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.mapTaskNumber = 100
	c.reduceTaskNumber = nReduce
	c.phase = mapPhase
	c.mapTasks = map[string]map[int]int{}
	c.reduceTasks = map[string]map[int]int{}
	c.mapWaitingResponseQueue = map[string]map[int]int{}
	c.reduceWaitingResponseQueue = map[string]map[int]int{}
	// TODO 对files里面的文件进行分割

	c.server()
	return &c
}
