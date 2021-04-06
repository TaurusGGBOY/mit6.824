## 思路

1. 按照老师给的提示是从`mr/worker.go`的`worker()`开始写，下面发送一个RPC给协调者请求一个任务，然后修改协调者的反馈是反馈文件名，是还没开始的map任务的文件名？然后修改工人去阅读文件，调用Map函数，就像在mrsequential.go

## 流程

1. worker发送RPC给Coordinator请求任务
2. coordinator响应还没有map的文件名
3. worker收到文件名，读文件
4. work调用map函数
5. map的结果发送给coordinator
6. coordinator在收到所有的结果之后调用reduce函数