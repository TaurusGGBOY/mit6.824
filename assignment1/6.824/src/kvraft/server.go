package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"log"
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
	db       map[string]string
	transIds map[int64]int
	notifyCh map[int]chan bool
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// TODO first no idempotence
	cmd := Op{
		Key:     args.Key,
		Value:   "",
		Type:    "Get",
		ClerkId: args.ClerkId,
		TransId: args.TransactionId,
	}
	reply.Value = ""
	for {
		index, _, isleader := kv.rf.Start(cmd)
		if !isleader {
			reply.Err = ErrWrongLeader
			return
		}

		kv.mu.Lock()
		ch := make(chan bool, 1)
		kv.notifyCh[index] = ch
		kv.mu.Unlock()
		select {
		case <-ch:
			kv.mu.Lock()
			DPrintf("index:%d, apply success and notify get %v\n", index, cmd)
			value, ok := kv.db[args.Key]
			if !ok {
				reply.Err = ErrNoKey
				return
			}
			reply.Err = OK
			reply.Value = value
			kv.mu.Unlock()
			return
		case <-time.After(ApplyTimeout):
		}
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// TODO first no idempotence.
	cmd := Op{
		Key:     args.Key,
		Value:   args.Value,
		Type:    args.Op,
		ClerkId: args.ClerkId,
		TransId: args.TransactionId,
	}
	for {

		index, _, isleader := kv.rf.Start(cmd)

		if !isleader {
			reply.Err = ErrWrongLeader
			return
		}
		kv.mu.Lock()
		ch := make(chan bool, 1)
		kv.notifyCh[index] = ch
		kv.mu.Unlock()

		select {
		case <-ch:
			kv.mu.Lock()
			DPrintf("index:%d apply success and notify putAppend %v\n", index, cmd)
			reply.Err = OK
			kv.mu.Unlock()
			return
		case <-time.After(ApplyTimeout):
		}
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
			DPrintf("Receive apply msg %v\n", msg)
			op := msg.Command.(Op)
			if op.TransId < kv.transIds[op.ClerkId] {
				continue
			}
			kv.mu.Lock()
			value, ok := kv.db[op.Key]
			if !ok || op.Type == "Put" {
				kv.db[op.Key] = op.Value
			} else if op.Type == "Append" {
				kv.db[op.Key] = value + op.Value
			}

			kv.transIds[op.ClerkId]++
			send(kv.notifyCh[msg.CommandIndex])
			DPrintf("send apply msg %v finish\n", msg)
			kv.mu.Unlock()
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

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.db = map[string]string{}
	kv.transIds = map[int64]int{}
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.notifyCh = map[int]chan bool{}

	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	go kv.listen()
	return kv
}
