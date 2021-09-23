package kvraft

import (
	"6.824/labrpc"
	"sync"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	clerkId       int64
	prevLeaderId  int
	transactionId int
	mu            sync.Mutex
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.clerkId = nrand()
	ck.prevLeaderId = 0
	ck.transactionId = 0
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	args := GetArgs{
		Key:           key,
		ClerkId:       ck.clerkId,
		TransactionId: ck.transactionId,
	}

	for {
		ck.mu.Unlock()
		reply := GetReply{
			Err:   "",
			Value: "",
		}
		ok := ck.servers[ck.prevLeaderId].Call("KVServer.Get", &args, &reply)
		// ou will have to modify this function.
		ck.mu.Lock()
		if !ok || reply.Err == "" {
			ck.prevLeaderId = (ck.prevLeaderId + 1) % len(ck.servers)
			continue
		}
		if reply.Err == OK {
			DPrintf("ok reply\n")
			ck.transactionId++
			return reply.Value
		}
		if reply.Err == ErrNoKey {
			DPrintf("errnokey\n")
			ck.transactionId++
			return ""
		}
		if reply.Err == ErrWrongLeader {
			DPrintf("clerk:%d, wrong leader:%d\n", ck.clerkId, ck.prevLeaderId)
			ck.prevLeaderId = (ck.prevLeaderId + 1) % len(ck.servers)
			continue
		}
	}
	return ""
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.mu.Lock()
	defer ck.mu.Unlock()
	args := PutAppendArgs{
		Key:           key,
		Value:         value,
		Op:            op,
		TransactionId: ck.transactionId,
		ClerkId:       ck.clerkId,
	}

	for {
		ck.mu.Unlock()
		reply := PutAppendReply{
			Err: "",
		}
		ok := ck.servers[ck.prevLeaderId].Call("KVServer.PutAppend", &args, &reply)
		// You will have to modify this function.
		ck.mu.Lock()
		if !ok || reply.Err == "" {
			DPrintf("clerkId:%d, timeout, prev:%d\n", ck.clerkId, ck.prevLeaderId)
			ck.prevLeaderId = (ck.prevLeaderId + 1) % len(ck.servers)
			continue
		}
		if reply.Err == OK {
			DPrintf("put append ok reply args:%v\n", args)
			ck.transactionId++
			return
		}
		if reply.Err == ErrWrongLeader {
			DPrintf("clerkId:%d, ErrWrongLeader, prev:%d\n", ck.clerkId, ck.prevLeaderId)
			ck.prevLeaderId = (ck.prevLeaderId + 1) % len(ck.servers)
			continue
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
