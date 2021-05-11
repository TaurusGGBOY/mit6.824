package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"
	"sync"
	"sync/atomic"

	//	"6.824/labgob"
	"6.824/labrpc"
	"math/rand"
	"time"
)

var lock sync.Mutex

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type Entry struct {
	term    int
	Command interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent all
	// TODO need persistent
	// TODO how to define id
	isLeader    bool
	currentTerm int
	votedFor    int
	log         []Entry

	// Volatile all
	commitIndex int
	lastApplied int

	// Volatile leader
	nextIndex  []int
	matchIndex []int

	receiveHeartbeat bool
}

//
type AppendEntriesArgs struct {
	term         int
	leaderId     int
	prevLogIndex int
	prevLogTerm  int
	entries      []Entry
	leaderCommit int
}

type AppendEntriesReply struct {
	term    int
	success bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	// TODO what called 'believe it is the leader'?
	term = rf.currentTerm
	isleader = rf.isLeader
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.xxx)
	e.Encode(rf.yyy)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	term         int
	candidateId  int
	lastLogIndex int
	lastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	term        int
	voteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	lock.Lock()
	defer lock.Unlock()

	reply.term = rf.currentTerm

	if args.term < rf.currentTerm {
		reply.voteGranted = false
		return
	}
	// TODO how to define up-to-date?
	if len(rf.log)-1 <= args.lastLogIndex || rf.votedFor == args.candidateId {
		reply.voteGranted = true
		rf.votedFor = args.candidateId
	}
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.ReceiveAppendEntries", args, reply)
	return ok
}

// TODO
func (rf *Raft) ReceiveAppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	// TODO heart beat and log
	reply.term = rf.currentTerm
	rf.receiveHeartbeat = true
	// it's heartbeat
	if len(args.entries) == 0 {
		reply.success = true
		return true
	}
	// TODO need a timer to trace the situation of timeout or in ticker??
	d := time.Duration(time.Microsecond * 300)
	timer := time.NewTimer(d)
	go func() {
		<-timer.C
		if c.phase == MapPhase {
			value, ok := c.mapWaitingResponseQueue[t.TaskNumber]
			if ok {
				delete(c.mapWaitingResponseQueue, t.TaskNumber)
				c.mapTasks[t.TaskNumber] = value
			}
		} else if c.phase == ReducePhase {
			value, ok := c.reduceWaitingResponseQueue[t.TaskNumber]
			if ok {
				delete(c.reduceWaitingResponseQueue, t.TaskNumber)
				c.reduceTasks[t.TaskNumber] = value
			}
		}
	}()

	reply.success = false
	if len(rf.log) >= args.prevLogIndex && rf.log[args.prevLogIndex].term == args.prevLogTerm {
		reply.success = true
	} else {
		return false
	}

	if args.term < rf.currentTerm {
		return false
	}
	return true
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		time.Sleep(time.Millisecond*150 + rand.Intn(150))

		if rf.receiveHeartbeat {
			rf.receiveHeartbeat = false
			return
		}

		// TODO consider the situation that long time no leader
		// not receive heart beat and start a selection

		args := RequestVoteArgs{}
		args.candidateId = rf.me
		args.lastLogIndex = len(rf.log) - 1
		args.lastLogTerm = rf.log[len(rf.log)-1].term
		args.term = rf.currentTerm

		count := 0
		for i := range rf.peers {
			reply := RequestVoteReply{}
			// TODO can go routine
			rf.sendRequestVote(i, &args, &reply)
			if reply.voteGranted {
				count++
			}
		}
		if count > len(rf.peers)/2 {
			rf.isLeader = true
			rf.sendAllHeartbeat()
		}

	}
}

// send HeartBeat
func (rf *Raft) heartBeatTicker() {
	for rf.killed() == false {
		if !rf.isLeader {
			return
		}
		time.Sleep(time.Millisecond * 100)

		// heartbeat
		rf.sendAllHeartbeat()
	}
}

func (rf *Raft) sendAllHeartbeat() {
	args := AppendEntriesArgs{}
	args.term = rf.currentTerm
	args.entries = make([]Entry, 0)

	for i := range rf.peers {
		go func() {
			reply := AppendEntriesReply{}
			// TODO if heartbeat false
			rf.sendAppendEntries(i, &args, &reply)
		}()
	}
}
//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.commitIndex = 0
	rf.lastApplied = 0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// TODO if can't get from persist then give them value
	rf.currentTerm = 0
	rf.votedFor = me
	rf.log = make([]Entry, 0)
	rf.receiveHeartbeat = false

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
