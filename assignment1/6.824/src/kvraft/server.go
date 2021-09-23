package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"bytes"
	"errors"
	"log"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = true
const ApplyTimeout = 800 * time.Millisecond

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key     string
	Value   string
	Type    string
	ClerkId int64
	TransId int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	db          map[string]string
	lastTransId map[int64]int
	notifyCh    map[int]chan raft.ApplyMsg

	// 3B
	snapshotIndex int
	persister     *raft.Persister
	newestIndex   int
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	_, isleader := kv.rf.GetState()
	if !isleader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	// have commit can direct reply
	if transId, ok := kv.lastTransId[args.ClerkId]; ok && args.TransactionId <= transId {
		DPrintf("get op.transId %d, lastTransId %d\n", args.TransactionId, transId)
		reply.Err = "OK"
		value := kv.db[args.Key]
		reply.Value = value
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	cmd := Op{
		Key:     args.Key,
		Value:   "",
		Type:    "Get",
		ClerkId: args.ClerkId,
		TransId: args.TransactionId,
	}
	reply.Value = ""
	index, _, isleader := kv.rf.Start(cmd)
	if !isleader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	ch := make(chan raft.ApplyMsg, 1)
	kv.notifyCh[index] = ch
	kv.mu.Unlock()
	select {
	case msg := <-ch:
		op := msg.Command.(Op)
		kv.mu.Lock()
		delete(kv.notifyCh, index)
		if op.ClerkId != args.ClerkId || op.TransId != args.TransactionId {
			reply.Err = ErrWrongLeader
			kv.mu.Unlock()
			return
		}
		DPrintf("index:%d, apply success and notify get\n", index)

		value, ok := kv.db[cmd.Key]
		if !ok {
			reply.Err = ErrNoKey
			kv.mu.Unlock()
			return
		}
		reply.Err = OK
		reply.Value = value
		DPrintf("index:%d, apply success and notify get value %v\n", index, msg)
		kv.mu.Unlock()
		return
	case <-time.After(ApplyTimeout):
		kv.mu.Lock()
		delete(kv.notifyCh, index)
		kv.mu.Unlock()
		reply.Err = ErrWrongLeader
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	DPrintf("goroutine:%d", runtime.NumGoroutine())
	_, isleader := kv.rf.GetState()
	if !isleader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	// have put or append
	if transId, ok := kv.lastTransId[args.ClerkId]; ok && args.TransactionId <= transId {
		DPrintf("put append op.transId %d, lastTransId %d\n", args.TransactionId, transId)
		reply.Err = OK
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	cmd := Op{
		Key:     args.Key,
		Value:   args.Value,
		Type:    args.Op,
		ClerkId: args.ClerkId,
		TransId: args.TransactionId,
	}

	//now := time.Now()
	index, _, isleader := kv.rf.Start(cmd)

	if !isleader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	ch := make(chan raft.ApplyMsg, 1)
	kv.notifyCh[index] = ch
	kv.mu.Unlock()

	select {
	case msg := <-ch:
		kv.mu.Lock()
		delete(kv.notifyCh, index)
		op := msg.Command.(Op)
		// important: may a wrong leader to reply, but receive this can
		if op.ClerkId != args.ClerkId || op.TransId != args.TransactionId {
			reply.Err = ErrWrongLeader
			DPrintf("I'm not leader now %d\n", kv.me)
			kv.mu.Unlock()
			return
		}
		DPrintf("index:%d apply success and notify putAppend\n", index)
		reply.Err = OK

		kv.mu.Unlock()
		//DPrintf("cost time :%v", time.Since(now))
		return
	case <-time.After(ApplyTimeout):
		kv.mu.Lock()
		delete(kv.notifyCh, index)
		kv.mu.Unlock()
		DPrintf("timeout change leader: %d\n", kv.me)
		reply.Err = ErrWrongLeader
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
	DPrintf("kill rf.me: %d\n", kv.me)
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// TODO listen applyCh
func (kv *KVServer) listen() {
	for {
		for msg := range kv.applyCh {
			// TODO snapshot
			if msg.CommandValid {
				DPrintf("Receive apply msg:%v\n", msg)
				op := msg.Command.(Op)
				kv.mu.Lock()

				lastTransId, ok1 := kv.lastTransId[op.ClerkId]
				if ok1 && op.TransId <= lastTransId {
					DPrintf("op.transId %d, lastTransId %d\n", op.TransId, lastTransId)
					kv.mu.Unlock()
					continue
				}

				// important: it must be here to ensure that follower can change their db
				if op.Type == "Put" {
					kv.db[op.Key] = op.Value
					//DPrintf("put %s\n", kv.db[op.Key])
				} else if op.Type == "Append" {
					// if no value then empty string
					value := kv.db[op.Key]
					kv.db[op.Key] = value + op.Value
					//DPrintf("append %s\n", kv.db[op.Key])
				}
				kv.lastTransId[op.ClerkId] = op.TransId
				// follower may not wait for channel msg
				sendMsg(kv.notifyCh[msg.CommandIndex], msg)

				kv.newestIndex = max(kv.newestIndex, msg.CommandIndex)
				kv.snapshotNow(msg)

				kv.mu.Unlock()
			} else if msg.SnapshotValid {
				kv.mu.Lock()
				DPrintf("Receive snapshot apply msg %v\n", msg)
				if msg.SnapshotIndex >= kv.snapshotIndex {
					if kv.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot) {
						DPrintf("condition install succcess %v\n", msg)
						kv.readSnapshot(msg.Snapshot)
					} else {
						DPrintf("condition install fail %v\n", msg)
					}
				}
				kv.mu.Unlock()
			}
		}
	}
}

func send(ch chan bool) {
	//DPrintf("ch sending len:%d, cap:%d, empty:%v\n", len(ch), cap(ch), ch)
	if ch == nil {
		return
	}
	select {
	case <-ch:
		//DPrintf("have ch\n")
	default:
		//DPrintf("default ch\n")
	}
	//DPrintf("send ch len:%d, cap:%d\n", len(ch), cap(ch))
	if cap(ch) == 0 {
		return
	}
	ch <- true
	//DPrintf("send ch finish len:%d\n", len(ch))
}

func sendMsg(ch chan raft.ApplyMsg, msg raft.ApplyMsg) {
	if ch == nil {
		return
	}
	select {
	case <-ch:
	default:
	}
	if cap(ch) == 0 {
		return
	}
	ch <- msg
	//DPrintf("send ch finish len:%d\n", len(ch))
}

func (kv *KVServer) generateSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.snapshotIndex)
	e.Encode(kv.db)
	return w.Bytes()
}

func (kv *KVServer) readSnapshot(data []byte) error {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return errors.New("no data")
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var snapshotIndex int
	var db map[string]string
	if d.Decode(&snapshotIndex) != nil || d.Decode(&db) != nil {
		return errors.New("read wrong")
	} else {
		DPrintf("recover snapshotIndex:%d", kv.snapshotIndex)
		kv.snapshotIndex = snapshotIndex
		kv.db = db
		return nil
	}
}

func (kv *KVServer) shouldSnapshot() bool {
	DPrintf("max:%d, raftsize:%d ", kv.maxraftstate, kv.persister.RaftStateSize())
	return kv.maxraftstate != -1 && kv.persister.RaftStateSize() >= kv.maxraftstate
}

func max(a int, b int) int {
	if a > b {
		return a
	} else {
		return b
	}
}

func min(a int, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}

func (kv *KVServer) snapshotNow(msg raft.ApplyMsg) {
	if kv.shouldSnapshot() {
		snapshot := kv.generateSnapshot()
		if kv.rf.Snapshot(kv.newestIndex, snapshot) {
			DPrintf("snapshot successs napshotindex:%v\n", msg)
			kv.snapshotIndex = kv.newestIndex
		} else {
			DPrintf("snapshotfail snapshotindex:%v\n", msg)
		}
	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := KVServer{
		me:            me,
		maxraftstate:  maxraftstate,
		db:            map[string]string{},
		lastTransId:   map[int64]int{},
		applyCh:       make(chan raft.ApplyMsg),
		notifyCh:      map[int]chan raft.ApplyMsg{},
		snapshotIndex: 0,
		persister:     persister,
	}

	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	err := kv.readSnapshot(kv.persister.ReadSnapshot())
	if err != nil {
		DPrintf("error %v", err)
	}
	kv.newestIndex = kv.snapshotIndex
	go kv.listen()
	return &kv
}
