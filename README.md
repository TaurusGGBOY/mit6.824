仅展示部分文档

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

##### cost time

20h

#### 结局

![](https://gitee.com/agaogao/photobed/raw/master/img/20210419215923.png)

## Assignment2A

### 1 Require

implement selection and heart beat

### 2 TODO

+ [x] Define a struct to hold information about each log entry in Figure 2
+ [x] RequestVoteArgs
+ [x] RequestVoteReply
+ [x] make() start a go routine to start election
+ [ ] RequestVote()
+ [x] AppendEntries struct
+ [ ] Leader sends AppendEntries as heartbeat
+ [ ] Heartbeat receiver handle
+ [ ] Random timeout
+ [x] GetState()
+ [ ] rf.Kill()

### 3 Tips

+ Read paper's Figure 2 about election
+ Heartbeat no more than 0.1s
+ Selection finishes in 5s
+ Heartbeat may larger than 150ms-300ms
+ Use time.Sleep()
+ RPC only capital letters
+ go test -run 2A -race
+ set selection timeout 400ms

### 4 Problems and Solve

1. can't rpc RequestVote function
2. there sometimes are 3 leaders at one time
    + consider one is leader and vote for another one, and he become leader and not send heartbeat yet
    + there is no timeout for a selection
    + solution: old leader received old vote from others
        + received vote then judge if it's legal
3. election need timetout scheme
   + first, add timetout scheme, when it comes to 150ms-300ms, judge if win
   + second, add waitgroup scheme, if get all vote reply, then judge if win
4. selection reply need to judge if it's from currentterm
5. leader win the selection, but leader term is less than a candidate
   + so what to do for this follower?
   + leader should update term when vote and heartbeat
### 5 Result
![](https://gitee.com/agaogao/photobed/raw/master/img/20210601225039.png)

### 6 Cost time
20-30h

## 2B

### 1 Require

implement append log

### 2 TODO

+ [x] Define a struct to hold information about each log entry in Figure 2

### 3 Tips

+ [ ] figure2

### 4 Problems and Solve

+ [x] is log entry like appmsg
   + [x] no apply msg is not log entry
+ [x] what is command interface{}?
   + [x] don't need to care just store it
+ [ ] will start function waiting for log commit end?
+ [ ] logic
   + [ ] leader: send log rpc to client
   + [ ] client: state machine add command
   + [ ] leader: wait for entry all replicated, apply the entry to state machine
      + [ ] send commit to client
   + [ ] leader: retry infinitely if followers not apply entry
+ [ ] leader: apply and commit entry once apply by majority of servers.
   + [ ] add all prev log created by prev leaders
+ [ ] leader: track highest index committed
   + [ ] heartbeat will take this index
   + [ ] client: learn this index and apply state machine
   + [ ] client run command not change state machine?
   + [ ] client receieves index and update state machine
      + [ ] what if client has long distence with leader? how can he reach the index lastest?
+ [ ] client receive commit index, if index no this entry, then not commit
   + [ ] it guarantees client run command must be committed
   + [ ] it guarantees follower log is same with the leader
+ [ ] if clients logs different with the leader, get rid of logs from clients
   + [ ] return reject and delete log after the next index from heartbeat
   + [ ] leader retry with nextindex--, but not delete log
   + [ ] how they check if logs are consistency?
   + [ ] can check term to optimization communication time
+ [ ] so majority of servers apply can be commit 
   + [ ] what about the minority of servers not be apply log? Ask log while send appendentryRPC?
+ [ ] same index and same term can ensure same command?
+ [x] printf shows followers' logs have been commited and applied to state machine, but in test script, rf.log has been not applied.
   + [ ] if there is some special points in snapshot scheme?
      + [ ] yes applyCh
   + [ ] lastApplied is not really applied to state mechine?
      + [ ] yes appyCh
+ [ ] fail test2B TestFailAgree2B
   + [ ] one follower down
      + [ ] leader can't receive heartbeat
      + [ ] leader dead lock
      + [ ] follower think leader is down
      + [ ] follower start selection
      + [ ] follower request vote
      + [ ] no one response
+ [ ] there are some probility that old leader rejoin and quit leader and then give leader to new one, new leader send commitIndex that allow old leader update last apply
    + [ ] check information send to follower, lastapplymsg should be the same
### 5 Result

![](https://gitee.com/agaogao/photobed/raw/master/img/20210726201812.png)

### 6 Cost time

60-70h

### 7improve points

+ Once follower is down, leader will try appendentry rpc infinitely and won't stop.
+ One time can append a piece of command
+ logs delete if not match
+ first reboot of leader may re transfer all log to others
+ once follower down, selection will hold if follower recovers. It costs a lot.

## 2C

### 1 Require

implement persist

### 2 TODO

+ [x] Define a struct to hold information about each log entry in Figure 2

### 3 Tips

+ [ ] figure8

### 4 Problems and Solve
+ [ ] limit 4096*2 goroutine
  + [ ] how can I see the dump of go process
    + [ ] pprof
  + [ ] i can see that in syncTicker go func1 that rpc won't quit after a long time 
    + [ ] i choose channel + new timer to deal with it **wrong**
    + [ ] solution: set a timeout, if not reply or timeout, don't send append to this server
  + [ ] how to kill go routine when don't need
    + [ ] a timer deal with timeout timer
+ [ ] wrong in back nextindex
  + [ ] there are some wrong in both follower and leader
+ [ ] someone become leader but not quit after receiver heartbeat
+ [ ] heartbeat and synclog may be conflict 
    + [ ] delete heartbeat scheme
+ [ ] important: only last log.term == current term then update commitIndex
    + [ ] it's the method to ensure all the log are most up-to-date
    + [ ] otherwise when in selection there may be some out-date-candidate will win this selection
+ [ ] how to avoid frequently selection?
+ [ ] implement kill method can avoid too much go routine
+ [ ] receive heartbeat and vote should reset selection time
+ [ ] after 999 iter my code remind to election frequently
    + [ ] update term once receive heartbeat or requestvote
    + [ ] it's normal that 20 terms to elect a leader

### 5 Result

![](https://gitee.com/agaogao/photobed/raw/master/img/20210806002358.png)

### 6 Cost time

30-40h

### 7improve points

+ Once follower is down, leader will try appendentry rpc infinitely and won't stop.

+ in recover phase, frequently change leader may cause slow consistence 

+ there is still 1/3 test will go wrong

  + 2021/08/05 09:22:18 apply error: commit index=129 server=2 5918 != server=1 4089

    + reorder rpc can cause commit mess
  + config.go:552: one(9909) failed to reach agreement
    +  lose rpc and reorder can cause slow convergence

## 2D

### 1 Require

implement snamshot

### 2 TODO

+ [ ] Snapshot(index int, snapshot []byte)  communicate to Raft?
+ [ ] CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool communicate to Raft?
  + [ ] refuse old snapshot return false to refuse snapshot
  + [ ] check if an snapshot must be installed or not
+ [ ] InstallSnapshot RPC install snapshot
  + [ ] don't implement figure 13 offset
+ [ ] InstallSnapshot handler -> applyCh send the snapshot by putting snapshot in ApplyMsg -> reader invokes CondInstallSnapshot -> sender ? update log 
+ [ ] no longer reference discard log
+ [ ] change the index scheme of log
+ [ ] lastIncludedTerm/lastIncludedIndex (consider whether this should be persisted).
+ [ ] SaveStateAndSnapshot() to store snapshot
+ [ ] test 2 costs 240s real timeand 90s CPU time

### 3 Tips

+ [ ] section 7

### 4 Problems and Solve

+ [ ] 

### 5 Result



### 6 Cost time



### 7improve points
