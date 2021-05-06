## Assignment1

#### 问题探索日志

##### 问题1

```shell
[root@localhost main]# sh test-mr.sh
*** Starting wc test.
2021/04/19 06:29:22 dialing:dial unix /var/tmp/824-mr-0: connect: connection refused
2021/04/19 06:29:22 dialing:dial unix /var/tmp/824-mr-0: connect: connection refused
2021/04/19 06:29:22 dialing:dial unix /var/tmp/824-mr-0: connect: connection refused
2021/04/19 06:29:22 rpc.Register: method "Done" has 1 input parameters; needs exactly three
sort: cannot read: mr-out*: No such file or directory
cmp: EOF on mr-wc-all
--- wc output is not the same as mr-correct-wc.txt
--- wc test: FAIL
```

探索路径

+ google了，无

+ 查看一下脚本

+ ```shell
  echo '***' Starting wc test.
  
  timeout -k 2s 180s ../mrcoordinator ../pg*txt &
  pid=$!
  
  # give the coordinator time to create the sockets.
  sleep 1
  
  # start multiple workers.
  timeout -k 2s 180s ../mrworker ../../mrapps/wc.so &
  timeout -k 2s 180s ../mrworker ../../mrapps/wc.so &
  timeout -k 2s 180s ../mrworker ../../mrapps/wc.so &
  
  ```

+ 开局开启coordinator，1秒之后开启worker，改成5s试试

原因

`开局让coordinator承担了split的任务，导致sleep一秒之后，coordinator还没有启动……`

#### 结局

![](https://gitee.com/agaogao/photobed/raw/master/img/20210419215923.png)

## Assignment2A

### 1 Require

implement selection and heart beat

### 2 TODO

+ [ ] Define a struct to hold information about each log entry in Figure 2
+ [ ] RequestVoteArgs
+ [ ] RequestVoteReply
+ [ ] make() start a go routine to start election
+ [ ] RequestVote()
+ [ ] AppendEntries struct
+ [ ] Leader sends AppendEntries as heartbeat
+ [ ] Heartbeat receiver handle
+ [ ] Random timeout
+ [ ] GetState()
+ [ ] rf.Kill()

### 3 Tips

+ Read paper's Figure 2 about election
+ Heartbeat no more than 0.1s
+ Selection finishes in 5s
+ Heartbeat may larger than 150ms-300ms
+ Use time.Sleep()
+ RPC only capital letters
+ go test -run 2A -race

### 4 Problems 