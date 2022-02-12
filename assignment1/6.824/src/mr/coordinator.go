package mr

import (
	"log"
	"strconv"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

var lock sync.Mutex

// TODO 这个Coordinator有什么好定义的？
type Coordinator struct {
	// Your definitions here.
	mapTasks                   map[int]string
	reduceTasks                map[int]string
	reduceTaskNumber           int
	mapWaitingResponseQueue    map[int]string
	reduceWaitingResponseQueue map[int]string
	singleFileWordNumber       int
	totalMapTasks              int
	phase                      TaskPhase
	assignWorkerID             int
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) RequestTask(args *RequestWorker, t *Task) error {
	//log.Printf("收到worker的请求 id为：%v", args.Id)
	//defer log.Printf("处理worker的请求结束 id为：%v,分配的任务符号为:%v，分配的文件名为:%v", args.Id, t.TaskNumber, t.FileName)
	if c.Done() {
		//log.Printf("所有任务完成")
		t.Alive = false
		return nil
	}
	lock.Lock()
	defer lock.Unlock()
	if c.phase == MapPhase {
		t.Phase = c.phase
		t.Alive = true
		t.NMap = c.singleFileWordNumber
		t.NReduce = c.reduceTaskNumber
		if len(c.mapTasks) == 0 {
			t.TaskNumber = -1
		}
		for taskNumber, filename := range c.mapTasks {
			t.FileName = filename
			t.TaskNumber = taskNumber
			delete(c.mapTasks, t.TaskNumber)
			c.mapWaitingResponseQueue[t.TaskNumber] = t.FileName
			break
		}
	} else if c.phase == ReducePhase {
		// TODO 对于reduce来说 直接给一个Y让他读取所有的X-Y吗？
		t.Phase = c.phase
		t.Alive = true
		t.NMap = c.totalMapTasks
		t.NReduce = c.reduceTaskNumber
		if len(c.reduceTasks) == 0 {
			t.TaskNumber = -1
		}
		for taskNumber, filename := range c.reduceTasks {
			t.FileName = filename
			t.TaskNumber = taskNumber
			delete(c.reduceTasks, t.TaskNumber)
			c.reduceWaitingResponseQueue[t.TaskNumber] = t.FileName
			break
		}
	}
	d := time.Duration(time.Second * 10)
	timer := time.NewTimer(d)
	go func() {
		<-timer.C
		if c.phase == MapPhase {
			value, ok := c.mapWaitingResponseQueue[t.TaskNumber]
			if ok {
				delete(c.mapWaitingResponseQueue, t.TaskNumber)
				c.mapTasks[t.TaskNumber] = value
			}
		} else if c.phase == ReducePhase {
			value, ok := c.reduceWaitingResponseQueue[t.TaskNumber]
			if ok {
				delete(c.reduceWaitingResponseQueue, t.TaskNumber)
				c.reduceTasks[t.TaskNumber] = value
			}
		}
	}()
	return nil
}

// 响应任务
func (c *Coordinator) ResponseTask(args *Task, reply *ResponseTaskReply) error {
	lock.Lock()
	//log.Printf("收到work完成报告 文件名：%v 任务号：%v", args.FileName, args.TaskNumber)
	//log.Printf("mapTask有任务：%d,reduceTask有任务:%d，waitingmap:%d，waitingreduce:%d", len(c.mapTasks), len(c.reduceTasks), len(c.mapWaitingResponseQueue), len(c.reduceWaitingResponseQueue))

	defer lock.Unlock()
	if args.Phase == MapPhase {
		delete(c.mapWaitingResponseQueue, args.TaskNumber)
		if len(c.mapTasks) == 0 && len(c.mapWaitingResponseQueue) == 0 {
			c.phase = ReducePhase
		}
	} else if args.Phase == ReducePhase {
		delete(c.reduceWaitingResponseQueue, args.TaskNumber)
	}
	return nil
}

func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) Register(args *RequestWorker, reply *RequestWorker) error {
	lock.Lock()
	defer lock.Unlock()
	reply.Id = c.assignWorkerID
	c.assignWorkerID = c.assignWorkerID + 1
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
	lock.Lock()
	defer lock.Unlock()
	if len(c.mapTasks) <= 0 && len(c.reduceTasks) <= 0 && len(c.mapWaitingResponseQueue) <= 0 && len(c.reduceWaitingResponseQueue) <= 0 {
		return true
	}
	return false
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// NReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.singleFileWordNumber = 4000
	c.reduceTaskNumber = nReduce
	c.totalMapTasks = len(files)
	c.phase = MapPhase
	c.mapTasks = map[int]string{}
	c.reduceTasks = map[int]string{}
	c.mapWaitingResponseQueue = map[int]string{}
	c.reduceWaitingResponseQueue = map[int]string{}
	c.assignWorkerID = 0

	for i, file := range files {
		c.mapTasks[i] = file
	}

	for i := 0; i < nReduce; i++ {
		c.reduceTasks[i] = "mr-reduce-" + strconv.Itoa(i)
	}

	c.server()
	return &c
}
