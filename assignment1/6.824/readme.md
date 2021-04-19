### Assignment1

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