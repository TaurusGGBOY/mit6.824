package mr

import (
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"strings"
	"unicode"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// 添加了mapAndReduceWorker结构体
type MapAndReduceWorker struct {
	Id      int
	Mapf    func(string, string) []KeyValue
	Reducef func(string, []string) string
}

// 枚举阶段
type TaskPhase int

const (
	MapPhase     TaskPhase = 0
	ReducePhase  TaskPhase = 1
	WaitingPhase TaskPhase = 1
)

// 添加了Task结构体
type Task struct {
	FileName   string
	TaskNumber int
	Phase      TaskPhase
	NReduce    int
	NMap       int
	Alive      bool
}

type RequestWorker struct {
	Id int
}

type ResponseTaskReply struct {
	State int
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
// 初始化worker 传入的是map reduce两个函数
// map传入一个文件名 一个文件内容 返回一个键值数组
// reduce传入一个键值 一个值字符串 代表有多少个键 返回键的个数
// TODO 初始化的时候就是把这俩存起来？
// TODO 还要判断是map还是reduce？还是说map完了就进行reduce？
// TODO
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	// TODO 向coordinator申请任务 所以要带上uuid？
	// TODO 自己写rpc
	// 然后运行map任务？但是reduce任务？
	w := MapAndReduceWorker{}
	w.Mapf = mapf
	w.Reducef = reducef

	// 注册
	w.register()
	// 运行
	w.run()
	// uncomment to send the Example RPC to the coordinator.
	//CallExample()

}

func (w *MapAndReduceWorker) register() {
	//TODO 向coordinator发送注册rpc
}

func (w *MapAndReduceWorker) run() {
	// 一个循环
	for {
		// 循环请求任务
		t := w.requestTask()
		if !t.Alive {
			fmt.Printf("not Alive, quit")
			return
		}
		w.doTask(t)
	}

	// 循环做任务
}

func (w *MapAndReduceWorker) requestTask() Task {
	args := RequestWorker{}
	args.Id = w.Id
	t := Task{}
	call("coordinator.requestTask", &args, &t)
	return t
}

func (w *MapAndReduceWorker) doTask(t Task) {
	if t.Phase == MapPhase {
		w.doMapTask(t)
	} else if t.Phase == ReducePhase {
		w.doReduceTask(t)
	} else {
		fmt.Printf("do task error/n")
	}
}

func (w *MapAndReduceWorker) doMapTask(t Task) {
	// 读取文件

	file, err := os.Open(t.FileName)
	if err != nil {
		log.Fatalf("cannot open %v", t.FileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", t.FileName)
	}
	file.Close()

	//运行map
	kva := w.Mapf(t.FileName, string(content))

	// 将map结果按照hash的结果放在slice中
	reduce := make([][]KeyValue, t.NReduce)
	for _, kv := range kva {
		reduce[ihash(kv.Key)&(t.NReduce-1)] = append(reduce[ihash(kv.Key)&t.NReduce], kv)
	}

	// 将这个slice写成文件输出就可以了 命名是什么%v %v
	for index, ys := range reduce {
		if len(ys) > 0 {
			reduceFileName := "mr-" + strconv.Itoa(t.TaskNumber) + "-" + strconv.Itoa(index)
			ofile, _ := os.Create(reduceFileName)
			for _, y := range ys {
				fmt.Fprintf(ofile, "%v %v\n", y.Key, y.Value)
			}
			ofile.Close()
		}
	}

	// TODO向coordinator报告已经处理完毕
	w.responseTask(t)
}

func (w *MapAndReduceWorker) doReduceTask(t Task) {
	// TODO reduce是将hash相同的处理了还是说是根据任务分配的？
	intermediate := []KeyValue{}

	for i := 0; i <= t.NMap; i++ {
		filename := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(t.TaskNumber)
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", t.FileName)
			continue
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", t.FileName)
			continue
		}
		file.Close()
		ff := func(r rune) bool { return !unicode.IsLetter(r) }
		// 将读取的content转换为KeyValue
		// split contents into an array of words.
		words := strings.FieldsFunc(string(content), ff)
		for _, w := range words {
			kvArr := strings.Split(w, " ")
			kv := KeyValue{kvArr[0], kvArr[1]}
			intermediate = append(intermediate, kv)
		}
	}
	// TODO 给intermediate排序
	sort.Sort(ByKey(intermediate))

	// TODO 再按照mapf的代码来
	oname := "mr-out-" + strconv.Itoa(t.TaskNumber)
	ofile, _ := os.Create(oname)
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := w.Reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	ofile.Close()

	w.responseTask(t)
}

func (w *MapAndReduceWorker) responseTask(t Task) {
	reply := ResponseTaskReply{}
	call("coordinator.responseTask", t, &reply)
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// rpcname：注册的类名.方法名
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
