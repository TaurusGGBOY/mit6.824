package kvraft

import (
	"6.824/labrpc"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// TODO You will have to modify this struct.
	prevLeaderId int
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
	// TODO You'll have to add code here.
	ck.prevLeaderId = -1
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
	if ck.prevLeaderId == -1 {
		ck.prevLeaderId = int(nrand()) % len(ck.servers)
	}
	args := GetArgs{
		Key: key,
	}
	reply := GetReply{}

	ok := ck.servers[ck.prevLeaderId].Call("KVServer.Get", &args, &reply)
	// TODO You will have to modify this function.
	// TODO no consider idempotency
	if reply.Err == "" {
		ck.prevLeaderId = -1
		return ""
	}
	if !ok {
		return ""
	}
	if reply.Err == OK{

	}
	if reply.Err == ErrNoKey{

	}
	if reply.Err == ErrWrongLeader{

	}
	return reply.Value
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
	// TODO You will have to modify this function.
	if ck.prevLeaderId == -1 {
		ck.prevLeaderId = int(nrand()) % len(ck.servers)
	}
	args := PutAppendArgs{
		Key: key,
		Value: value,
		Op: op,
	}
	reply := PutAppendReply{}

	ok := ck.servers[ck.prevLeaderId].Call("KVServer.PutAppend", &args, &reply)

	// TODO no consider idempotency
	if !ok || reply.Err == "" {
		ck.prevLeaderId = -1
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
