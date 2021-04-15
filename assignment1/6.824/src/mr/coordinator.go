package mr

import (
	"fmt"
	"io/ioutil"
	"log"
	"strconv"
	"strings"
	"unicode"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

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
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) RequestTask(t *Task) error {
	if c.Done() {
		t.Alive = false
		return nil
	}
	if c.phase == MapPhase {
		for taskNumber, filename := range c.mapTasks {
			t.Phase = c.phase
			t.FileName = filename
			t.TaskNumber = taskNumber
			t.Alive = true
			t.NMap = c.singleFileWordNumber
			break
		}
		delete(c.mapTasks, t.TaskNumber)
		c.mapWaitingResponseQueue[t.TaskNumber] = t.FileName
	} else if c.phase == ReducePhase {
		// TODO 对于reduce来说 直接给一个Y让他读取所有的X-Y吗？
		for taskNumber, filename := range c.reduceTasks {
			t.Phase = c.phase
			t.FileName = filename
			t.TaskNumber = taskNumber
			t.Alive = true
			t.NMap = c.totalMapTasks
			t.NReduce = c.reduceTaskNumber
			break
		}
		delete(c.reduceTasks, t.TaskNumber)
		c.reduceWaitingResponseQueue[t.TaskNumber] = t.FileName
	}
	return nil
}

// 响应任务
func (c *Coordinator) ResponseTask(args *Task, reply *ResponseTaskReply) error {
	if args.Phase == MapPhase {
		delete(c.mapWaitingResponseQueue, args.TaskNumber)
		if len(c.mapWaitingResponseQueue) == 0 {
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
// NReduce is the number of reduce tasks to use.
//
// TODO 输入是一个文件列表和reduce任务数量？ 返回一个Coordinator
// 是我这边coordinator进行分割？
// 还是说这个已经分割好了 直接可以分发给worker？
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.singleFileWordNumber = 4000
	c.reduceTaskNumber = nReduce
	c.phase = MapPhase
	c.mapTasks = map[int]string{}
	c.reduceTasks = map[int]string{}
	c.mapWaitingResponseQueue = map[int]string{}
	c.reduceWaitingResponseQueue = map[int]string{}
	// 对files里面的文件进行分割
	// 记录第几个count
	count := 0
	oname := "split-"
	midWords := []string{}
	c.totalMapTasks = 0
	reduceFileName := ""

	for _, filename := range files {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
		// function to detect word separators.
		ff := func(r rune) bool { return !unicode.IsLetter(r) }
		// split contents into an array of words.
		words := strings.FieldsFunc(string(content), ff)

		// 将每个文件分成10个小块
		for _, word := range words {
			midWords = append(midWords, word)
			if len(midWords) >= c.singleFileWordNumber {
				c.totalMapTasks = (count - 1) / c.singleFileWordNumber
				reduceFileName = oname + strconv.Itoa(c.totalMapTasks)
				c.reduceTasks[c.totalMapTasks] = reduceFileName
				ofile, _ := os.Create(reduceFileName)
				fmt.Fprintf(ofile, "%v ", midWords)
				midWords = make([]string, 0)
			}
			count = count + 1
		}

	}
	if len(midWords) >= 0 {
		c.totalMapTasks = count / c.singleFileWordNumber
		reduceFileName = oname + strconv.Itoa(c.totalMapTasks)
		c.reduceTasks[c.totalMapTasks] = reduceFileName
		ofile, _ := os.Create(oname + strconv.Itoa(count/c.singleFileWordNumber))
		fmt.Fprintf(ofile, "%v ", midWords)
	}
	log.Printf("开始监听……")
	c.server()
	return &c
}
