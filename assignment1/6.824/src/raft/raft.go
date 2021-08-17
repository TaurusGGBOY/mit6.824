package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, Term, isleader)
//   start agreement on a new log entry
// rf.GetState() (Term, isLeader)
//   ask a Raft for its current Term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"6.824/labgob"
	"bytes"
	"errors"
	"fmt"
	"runtime"

	//	"bytes"
	"sync"
	"sync/atomic"

	//	"6.824/labgob"
	"6.824/labrpc"
	"math/rand"
	"time"
)

//
// as each Raft peer becomes aware that successive log Entries are
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
	Term    int
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
	isLeader    bool
	currentTerm int
	votedFor    int
	log         []Entry

	// Volatile all
	commitIndex int
	lastApplied int

	// Volatile leader
	nextIndex  []int // update once it applied
	matchIndex []int // update once reply to leader

	applyCh chan ApplyMsg
	// channel kill when killed
	closeCh chan struct{}
	// when receive request vote then reset selection time
	voteCh chan bool
	// when receive appendentry vote then reset selection time
	appendEntryCh chan bool
	// use channel to control heartbeat
	heartBeatCh chan bool
	// receive snapshot rpc
	snapshotCh chan bool
	// apply before snapshot
	applySnapshotCh chan bool

	// 2D
	snapshot      []byte
	snapshotTerm  int
	snapshotIndex int
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool

	ConflictTerm  int
	ConflictIndex int
}

type InstallSnapshotArgs struct {
	Term             int
	LeaderId         int
	LastIncludeIndex int
	LastIncludedTer  int
	data             []byte
	done             bool
}

type InstallSnapshotReply struct {
	Term int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	// what called 'believe it is the leader'?
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.isLeader
	rf.mu.Unlock()
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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.commitIndex)
	e.Encode(rf.snapshotTerm)
	e.Encode(rf.snapshotIndex)
	//fmt.Printf(time.Now().Format("2006-01-02 15:04:05")+" id:%d persist term:%d, voteFor:%d, commitIndex:%d\n", rf.me, rf.currentTerm, rf.votedFor, rf.commitIndex)
	data := w.Bytes()
	rf.persister.SaveStateAndSnapshot(data, rf.snapshot)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte, snapshot []byte) error {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		fmt.Printf(time.Now().Format("2006-01-02 15:04:05")+" id:%d recover and read persist nothing get1\n", rf.me)
		return errors.New("nothing get")
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []Entry
	var commitIndex int
	var snapshotTerm int
	var snapshotIndex int
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&log) != nil || d.Decode(&commitIndex) != nil || d.Decode(&snapshotTerm) != nil || d.Decode(&snapshotIndex) != nil {
		fmt.Printf(time.Now().Format("2006-01-02 15:04:05")+" id:%d recover and read persist nothing get2\n", rf.me)
		return errors.New("nothing get")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
		rf.commitIndex = commitIndex

		rf.matchIndex[rf.me] = len(rf.log) - 1
		rf.snapshotTerm = snapshotTerm
		rf.snapshotIndex = snapshotIndex
		rf.snapshot = snapshot
		fmt.Printf(time.Now().Format("2006-01-02 15:04:05")+" id:%d recover and read persist term:%d, voteFor:%d,commitIndex:%d\n", rf.me, rf.currentTerm, rf.votedFor, rf.commitIndex)
		return nil
	}
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).
	select {
	case <-rf.applySnapshotCh:
		return false
	default:
	}
	lastIndex := rf.getLastIndex()
	if lastIncludedIndex >= lastIndex {
		rf.log = make([]Entry, 1)
	} else {
		rf.log = append([]Entry{}, rf.log[rf.getIndex(rf.snapshotIndex)+1:]...)
	}
	rf.snapshot = snapshot
	rf.snapshotIndex = lastIncludedIndex
	rf.snapshotTerm = lastIncludedTerm
	runtime.GC()

	send(rf.snapshotCh)
	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	lastIndex := rf.getLastIndex()
	if index > lastIndex {
		return
	}
	rf.snapshot = snapshot
	rf.snapshotIndex = index
	rf.snapshotTerm = rf.log[rf.getIndex(index)].Term
	rf.log = rf.log[rf.getIndex(rf.snapshotIndex)+1:]

	runtime.GC()

	// 8. Reset state machine using snapshot contents (and load snapshot’s cluster configuration)
	send(rf.snapshotCh)
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	//lock.Lock()
	//defer lock.Unlock()
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm

	// election restriction 1 bigger term win
	// election restriction 2 same term longer index win
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		//fmt.Printf(time.Now().Format("2006-01-02 15:04:05")+" %d out of date vote from %d at term %d\n", rf.me, args.CandidateId, rf.currentTerm)
		return
	} else if args.Term > rf.currentTerm {
		fmt.Printf(time.Now().Format("2006-01-02 15:04:05")+" %d update requestvote in term %d from %d for term %d\n", rf.me, rf.currentTerm, args.CandidateId, args.Term)
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.isLeader = false
		reply.Term = rf.currentTerm
		rf.persist()
	}

	if args.Term == rf.currentTerm {
		fmt.Printf(time.Now().Format("2006-01-02 15:04:05")+" %d receive requestvote in term %d from %d for term %d\n", rf.me, rf.currentTerm, args.CandidateId, args.Term)
	}

	// 5.4.2 selection restriction
	if rf.votedFor == args.CandidateId || rf.votedFor == -1 {
		if rf.getLastIndex() > 0 && (args.LastLogTerm < rf.getByIndex(rf.getLastIndex()).Term || (rf.getByIndex(rf.getLastIndex()).Term == args.LastLogTerm && rf.getLastIndex() > args.LastLogIndex)) {
			//fmt.Printf(time.Now().Format("2006-01-02 15:04:05")+" me:%d candidate:%d, can lastterm:%d, can lastindex:%d, my term:%d, my index:%d\n", rf.me, args.CandidateId, args.LastLogTerm, args.LastLogIndex, rf.log[len(rf.log)-1].Term, len(rf.log)-1)
			reply.VoteGranted = false
			return
		}
		reply.VoteGranted = true
		send(rf.voteCh)
		rf.votedFor = args.CandidateId
		rf.persist()
		fmt.Printf(time.Now().Format("2006-01-02 15:04:05")+" %d vote to candidate:%d, can lastterm:%d, can lastindex:%d, my term:%d, my index:%d\n", rf.me, args.CandidateId, args.LastLogTerm, args.LastLogIndex, rf.getByIndex(rf.getLastIndex()).Term, rf.getLastIndex())
		return
	}
	reply.VoteGranted = false
	fmt.Printf(time.Now().Format("2006-01-02 15:04:05")+" %d not vote for you but vote for %d\n", rf.me, rf.votedFor)
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

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.ReceiveInstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) ReceiveInstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	reply.Term = rf.currentTerm

	// 1 Reply immediately if term < currentTerm
	if args.Term < rf.currentTerm {
		return
	}

	// if snapshot not the lasted
	if rf.snapshotIndex >= args.LastIncludeIndex {
		return
	}

	rf.applyCh <- ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.data,
		SnapshotIndex: args.LastIncludeIndex,
		SnapshotTerm:  args.LastIncludedTer,
	}

	// notify applyCh snapshot

	send(rf.snapshotCh)

	// 4. Reply and wait for more data chunks if done is false
}

func (rf *Raft) getLastIndex() int {
	return rf.snapshotIndex + len(rf.log) - 1
}

func (rf *Raft) getIndex(index int) int {
	return index - rf.snapshotIndex - 1
}

func (rf *Raft) getByIndex(index int) Entry {
	if index == rf.getLastIndex() {
		return Entry{Term: rf.snapshotTerm}
	} else if index > rf.getLastIndex() {
		return rf.log[index-rf.snapshotIndex-1]
	} else {
		panic("wrong index!")
	}
}

func (rf *Raft) ReceiveAppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// heart beat and log
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//fmt.Printf(time.Now().Format("2006-01-02 15:04:05")+" %d in term %d receive ReceiveAppendEntries from %d with term %d, isleader:%v\n", rf.me, rf.currentTerm, args.LeaderId, args.Term, rf.isLeader)
	reply.Term = rf.currentTerm
	reply.ConflictTerm = -1
	reply.ConflictIndex = -1

	// it's heartbeat
	// also use to reply
	// no use to distinguish heart beart
	// high term heart beat
	if rf.currentTerm > args.Term {
		fmt.Printf(time.Now().Format("2006-01-02 15:04:05")+" %d receive wrong term from %d, mine:%d, yours:%d\n", rf.me, args.LeaderId, rf.currentTerm, args.Term)
		reply.Success = false
		return
	}

	//if rf.commitIndex > args.LeaderCommit {
	//	fmt.Printf(time.Now().Format("2006-01-02 15:04:05")+" %d receive wrong leader commit from %d, mine:%d, yours:%d\n", rf.me, args.LeaderId, rf.commitIndex, args.LeaderCommit)
	//}

	send(rf.appendEntryCh)

	if rf.currentTerm < args.Term {
		rf.votedFor = -1
		rf.currentTerm = args.Term
		rf.persist()
	}
	// only term > me then give up leader
	if rf.isLeader {
		fmt.Printf(time.Now().Format("2006-01-02 15:04:05")+" %d quit leader and give leader to %d at term %d\n", rf.me, args.LeaderId, args.Term)
		rf.isLeader = false
		reply.Success = false
		return
	}

	//if len(args.Entries) > 0 {
	//	fmt.Printf(time.Now().Format("2006-01-02 15:04:05")+"ReceiveAppendEntries: EntryLen %d, leaderCommit %d, leaderId %d, pervLogIndex %d, prevLogTerm %d, term %d\n",
	//		len(args.Entries), args.LeaderCommit, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, args.Term)
	//	fmt.Printf(time.Now().Format("2006-01-02 15:04:05")+"ReceiveAppendEntries: me: %d, commitIndex: %d, lastApplied: %d, matchIndex: %d, nextIndex: %d, logLen: %d\n",
	//		rf.me, rf.commitIndex, rf.lastApplied, rf.matchIndex, rf.nextIndex, len(rf.log))
	//	//fmt.Printf(time.Now().Format("2006-01-02 15:04:05")+"ReceiveAppendEntries: log: %s\n", rf.log)
	//}

	// if log.len==1 continue
	// implementation 2
	if rf.getLastIndex() >= args.PrevLogIndex && rf.getByIndex(args.PrevLogIndex).Term != args.PrevLogTerm {
		reply.Success = false
		prevTerm := -1
		for i, log := range rf.log {
			if log.Term > args.PrevLogTerm {
				break
			}
			if prevTerm != log.Term {
				reply.ConflictIndex = i + rf.snapshotIndex + 1
				reply.ConflictTerm = log.Term
				prevTerm = log.Term
			}
		}
		if reply.ConflictIndex > args.PrevLogIndex-1 {
			reply.ConflictIndex = args.PrevLogIndex - 1
		}
		fmt.Printf(time.Now().Format("2006-01-02 15:04:05")+" %d receive wrong logindex and term:%d,%d,my my get logindex and term:%d,%d,commitIndex:%d\n", rf.me, args.PrevLogTerm, args.PrevLogTerm, reply.ConflictIndex, reply.Term, rf.commitIndex)
		return
	} else if rf.getLastIndex() < args.PrevLogIndex {
		reply.Success = false
		reply.ConflictIndex = rf.getLastIndex()
		reply.ConflictTerm = rf.getByIndex(reply.ConflictIndex).Term
		//fmt.Printf(time.Now().Format("2006-01-02 15:04:05")+" me:%d, prevLogIndex:%d, return false2, log:%v, my commitIndex:%d\n", rf.me, args.PrevLogIndex, rf.log, rf.commitIndex)
		return
	}

	if len(args.Entries) > 0 {
		removeIndex := args.PrevLogIndex + 1
		for ; removeIndex < len(rf.log) && removeIndex < args.PrevLogIndex+len(args.Entries); removeIndex++ {
			if rf.log[removeIndex].Term != args.Entries[removeIndex-args.PrevLogIndex-1].Term {
				break
			}
		}
		fmt.Printf(time.Now().Format("2006-01-02 15:04:05")+" removeIndex:%d, args.prevLogIndex:%d, len:%d\n", removeIndex, args.PrevLogIndex, len(args.Entries))
		rf.log = rf.log[:removeIndex]
		rf.log = append(rf.log, args.Entries[(removeIndex-args.PrevLogIndex-1):]...)
		rf.persist()
	}

	// update commitIndex
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = args.LeaderCommit
		rf.persist()
	}

	// update lastApplied
	// TODO before confirm apply, one should confirm if they are the same
	applyFlag := 0
	for rf.commitIndex > rf.lastApplied && rf.lastApplied+1 < len(rf.log) {
		applyFlag = 1
		rf.lastApplied++
		// what is apply log[lastApplied] to state machine
		// it is apply
		applyMsg := ApplyMsg{}
		applyMsg.CommandValid = true
		applyMsg.CommandIndex = rf.lastApplied
		applyMsg.Command = rf.log[applyMsg.CommandIndex].Command
		rf.applyCh <- applyMsg
		send(rf.applySnapshotCh)
	}
	if applyFlag == 1 {
		fmt.Printf(time.Now().Format("2006-01-02 15:04:05")+" follower:%d, last apply:%d, log[i]:%v, log:%v\n", rf.me, rf.lastApplied, rf.log[rf.lastApplied], rf.log)
	}

	reply.Success = true
	if len(args.Entries) > 0 {
		//fmt.Printf(time.Now().Format("2006-01-02 15:04:05") + "ReceiveAppendEntries: end\n")
		fmt.Printf(time.Now().Format("2006-01-02 15:04:05")+"ReceiveAppendEntries: EntryLen %d, leaderCommit %d, leaderId %d, pervLogIndex %d, prevLogTerm %d, term %d\n",
			len(args.Entries), args.LeaderCommit, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, args.Term)
		fmt.Printf(time.Now().Format("2006-01-02 15:04:05")+"ReceiveAppendEntries: me: %d, commitIndex: %d, lastApplied: %d, matchIndex: %d, nextIndex: %d, logLen: %d\n",
			rf.me, rf.commitIndex, rf.lastApplied, rf.matchIndex, rf.nextIndex, len(rf.log))
		//fmt.Printf(time.Now().Format("2006-01-02 15:04:05")+"ReceiveAppendEntries: log: %v\n", rf.log)
	}
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
// Term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	isLeader := rf.isLeader
	if !rf.isLeader {
		return index, term, isLeader
	}
	//if rf.log[len(rf.log)-1].Command != command {
	entry := Entry{rf.currentTerm, command}
	rf.log = append(rf.log, entry)
	rf.persist()
	//}else{
	//	rf.log[len(rf.log)-1].Term = rf.currentTerm
	//}

	rf.matchIndex[rf.me] = len(rf.log) - 1

	// if it is
	index = len(rf.log) - 1
	term = rf.currentTerm
	fmt.Printf(time.Now().Format("2006-01-02 15:04:05")+" leader:%d, in term:%d, start cmd %d\n", rf.me, rf.currentTerm, command)
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
	close(rf.closeCh)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	selectionTime := time.Duration(200+rand.Intn(300)) * time.Millisecond
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		//fmt.Printf(time.Now().Format("2006-01-02 15:04:05")+" %d ticker start with term %d\n", rf.me, rf.currentTerm)

		// if receive heartbeart in last sleep then no selection
		select {
		case <-rf.voteCh:
		case <-rf.appendEntryCh:
		case <-rf.snapshotCh:
		case <-time.After(selectionTime):
			{
				rf.mu.Lock()
				if !rf.isLeader {
					go rf.startSelection()
				}
				rf.mu.Unlock()
			}
		}
		//fmt.Printf(time.Now().Format("2006-01-02 15:04:05")+" %d start selection in term %d receiveheartbeat:%v\n", rf.me, rf.currentTerm, rf.receiveHeartbeat)
	}
	fmt.Printf(time.Now().Format("2006-01-02 15:04:05")+" %d quit select ticker\n", rf.me)
}

func (rf *Raft) startSelection() {
	// TODO consider the situation that long time no leader
	// not receive heart beat and start a se vglection
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.currentTerm++
	fmt.Printf(time.Now().Format("2006-01-02 15:04:05")+" %d start selection in term %d\n", rf.me, rf.currentTerm)
	rf.votedFor = rf.me
	rf.persist()

	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: len(rf.log) - 1,
		LastLogTerm:  rf.log[len(rf.log)-1].Term,
	}

	// vote for self
	var count int32 = 1
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(i int) {
			reply := RequestVoteReply{}
			//fmt.Printf(time.Now().Format("2006-01-02 15:04:05")+" %d send request vote to %d for term %d,args:%v\n", rf.me, i, args.Term, args)
			rf.sendRequestVoteToPeer(i, &args, &reply)
			fmt.Printf(time.Now().Format("2006-01-02 15:04:05")+" %d get reply waiting for lock args:%v, reply:%v\n", rf.me, args, reply)
			rf.mu.Lock()
			defer rf.mu.Unlock()
			fmt.Printf(time.Now().Format("2006-01-02 15:04:05")+" %d get lock args:%v, reply:%v\n", rf.me, args, reply)
			if args.Term == rf.currentTerm {
				fmt.Printf(time.Now().Format("2006-01-02 15:04:05")+" %d recieve vote reply to %d for term %d,args:%v,reply:%v\n", rf.me, i, args.Term, args, reply)
			}

			// appendix condition
			if args.Term != rf.currentTerm || rf.isLeader {
				return
			}

			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.persist()
				return
			}

			// prevent timeout vote
			if reply.VoteGranted {
				atomic.AddInt32(&count, 1)
			}

			if atomic.LoadInt32(&count) > (int32)(len(rf.peers)/2) {
				rf.becomeLeader()
			}
		}(i)
	}
}

func (rf *Raft) sendRequestVoteToPeer(i int, args *RequestVoteArgs, reply *RequestVoteReply) {
	for !rf.killed() {
		r := RequestVoteReply{}
		ch := make(chan bool, 1)
		go func() {
			ch <- rf.sendRequestVote(i, args, &r)
		}()

		select {
		case ok := <-ch:
			if ok {
				reply.Term = r.Term
				reply.VoteGranted = r.VoteGranted
				return
			} else {
				continue
			}
		case <-rf.closeCh:
			return
		}
	}
}

func (rf *Raft) becomeLeader() {
	fmt.Printf(time.Now().Format("2006-01-02 15:04:05")+" %d become leader with log:%v\n", rf.me, rf.log)
	rf.isLeader = true
	for i := range rf.nextIndex {
		rf.nextIndex[i] = len(rf.log)
	}
	for i := range rf.matchIndex {
		rf.matchIndex[i] = 0
	}
	rf.matchIndex[rf.me] = len(rf.log) - 1
	// periodly heartbeat
	go rf.syncLogTicker()
}

func (rf *Raft) syncLogTicker() {
	fmt.Printf(time.Now().Format("2006-01-02 15:04:05")+" %d start sync log ticker\n", rf.me)
	heartbeatDuring := time.Duration(100) * time.Millisecond
	for !rf.killed() {
		rf.mu.Lock()
		if !rf.isLeader {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()

		go rf.sendAllHeartbeat()

		select {
		case <-rf.heartBeatCh:
		case <-time.After(heartbeatDuring):
		}
	}
	fmt.Printf(time.Now().Format("2006-01-02 15:04:05")+" %d quit sync log ticker\n", rf.me)
}

func (rf *Raft) sendAllHeartbeat() {
	rf.mu.Lock()
	fmt.Printf(time.Now().Format("2006-01-02 15:04:05")+" %d send heartbeat to all with isleader:%v\n", rf.me, rf.isLeader)
	rf.mu.Unlock()

	for i, peer := range rf.peers {
		if i == rf.me {
			continue
		}
		go rf.sendHeartbeatToPeer(i, peer)
	}
}

func (rf *Raft) sendHeartbeatToPeer(i int, peer *labrpc.ClientEnd) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if !rf.isLeader {
		return
	}
	lastedEntryIndex := len(rf.log) - 1
	tempEntries := make([]Entry, len(rf.log)-rf.nextIndex[i])
	copy(tempEntries, rf.log[rf.nextIndex[i]:])
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		Entries:      tempEntries,
		LeaderCommit: rf.commitIndex,
		LeaderId:     rf.me,
		PrevLogIndex: rf.nextIndex[i] - 1,
		PrevLogTerm:  rf.log[rf.nextIndex[i]-1].Term,
	}
	//fmt.Printf(time.Now().Format("2006-01-02 15:04:05")+" %d send log to %d, start with %d, term:%d, isleader:%v, canIn:%v\n", rf.me, i, rf.nextIndex[i], rf.currentTerm, rf.isLeader, rf.canIn[i])
	reply := AppendEntriesReply{}

	r := AppendEntriesReply{}
	//fmt.Printf(time.Now().Format("2006-01-02 15:04:05")+" i:%d send heartbeat ok:%v, reply success:%v, with reply:%v\n", i, ok, reply.Success, reply)
	rf.mu.Unlock()
	ok := rf.sendAppendEntries(i, &args, &r)
	rf.mu.Lock()

	if !ok {
		return
	}

	reply.Term = r.Term
	reply.Success = r.Success
	reply.ConflictIndex = r.ConflictIndex
	reply.ConflictTerm = r.ConflictTerm
	//fmt.Printf(time.Now().Format("2006-01-02 15:04:05")+" i:%d reply ok:%v, reply success:%v, with reply:%v\n", i, ok, reply.Success, reply)

	// appendix condition
	if args.Term != rf.currentTerm {
		return
	}

	if reply.Term > rf.currentTerm {
		rf.isLeader = false
		return
	}

	if reply.Success == false {
		if reply.ConflictIndex != -1 {
			rf.nextIndex[i] = reply.ConflictIndex
			if rf.log[rf.nextIndex[i]].Term > reply.ConflictTerm {
				tempIndex := -1
				prevTerm := -1
				for j, log := range rf.log {
					if log.Term > reply.ConflictTerm {
						break
					}
					if prevTerm != log.Term {
						tempIndex = j
						prevTerm = log.Term
					}
				}
				if tempIndex < rf.nextIndex[i] {
					rf.nextIndex[i] = tempIndex
				}
			}
		} else {
			rf.nextIndex[i]--
		}
		if rf.nextIndex[i] <= 0 {
			rf.nextIndex[i] = 1
		}
		if len(rf.log) > 1 {
			fmt.Printf(time.Now().Format("2006-01-02 15:04:05")+" me:%d follower:%d fail and nextIndex-- to %d which term %d\n", rf.me, i, rf.nextIndex[i], rf.log[rf.nextIndex[i]].Term)
		}
		return
	}

	// update commitIndex
	rf.matchIndex[i] = lastedEntryIndex
	rf.nextIndex[i] = rf.matchIndex[i] + 1
	changeCommitFlag := 0
	if rf.log[lastedEntryIndex].Term == rf.currentTerm {
		for j := rf.commitIndex + 1; j <= lastedEntryIndex && j < len(rf.log); j++ {
			count := 0
			for k, _ := range rf.peers {
				if rf.matchIndex[k] >= j {
					count++
				}
			}
			if count > len(rf.peers)/2 {
				changeCommitFlag = 1
				rf.commitIndex++
				rf.persist()
				// apply is end state, commit is not
				for rf.commitIndex > rf.lastApplied {
					changeCommitFlag = 2
					rf.lastApplied++
					applyMsg := ApplyMsg{}
					applyMsg.CommandValid = true
					applyMsg.CommandIndex = rf.lastApplied
					applyMsg.Command = rf.log[applyMsg.CommandIndex].Command
					rf.applyCh <- applyMsg
					send(rf.applySnapshotCh)
					send(rf.heartBeatCh)
				}
			}
		}
		if changeCommitFlag == 1 {
			fmt.Printf(time.Now().Format("2006-01-02 15:04:05")+" leader:%d, commit to %d, log[i]:%v, log:%v\n", rf.me, rf.lastApplied, rf.log[rf.lastApplied], rf.log)
		} else if changeCommitFlag == 2 {
			fmt.Printf(time.Now().Format("2006-01-02 15:04:05")+" leader:%d, apply to %d, log[i]:%v, log:%v\n", rf.me, rf.lastApplied, rf.log[rf.lastApplied], rf.log)
		}
	}
}

func send(ch chan bool) {
	select {
	case <-ch: //if already set, consume it then resent to avoid block
	default:
	}
	ch <- true
}

func clear(ch chan bool) {
	select {
	case <-ch: //if already set, consume it then resent to avoid block
	default:
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
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.isLeader = false
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.matchIndex = make([]int, len(rf.peers))
	rf.nextIndex = make([]int, len(rf.peers))

	// initialize from state persisted before a crash
	err := rf.readPersist(persister.ReadRaftState(), persister.ReadSnapshot())

	// TODO if can't get from persist then give them value
	if err != nil {
		rf.currentTerm = 0
		rf.votedFor = -1
		// it's leading to the index start with 1 but there are 1 more log
		rf.log = make([]Entry, 0)
		rf.log = append(rf.log, Entry{})
	}

	rf.applyCh = applyCh
	rf.closeCh = make(chan struct{})
	rf.voteCh = make(chan bool, 1)
	rf.appendEntryCh = make(chan bool, 1)
	rf.heartBeatCh = make(chan bool, 1)
	rf.snapshotCh = make(chan bool, 1)
	rf.applySnapshotCh = make(chan bool, 1)

	// 2D
	rf.snapshot = make([]byte, 0)
	rf.snapshotIndex = 0
	rf.snapshotTerm = 0

	// start ticker goroutine to start elections
	fmt.Printf(time.Now().Format("2006-01-02 15:04:05")+" %d created\n", me)
	go rf.ticker()
	return rf
}
