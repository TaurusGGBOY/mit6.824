package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"

// TODO 这个Coordinator有什么好定义的？
type Coordinator struct {
	// Your definitions here.
	workers []worker
	reduceTasks []int

}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
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
// TODO 维护一个任务列表
func (c *Coordinator) Done() bool {
	ret := true

	// Your code here.


	return ret
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


	c.server()
	return &c
}

