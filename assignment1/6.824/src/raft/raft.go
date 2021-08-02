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
	// TODO need persistent
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

	receiveHeartbeat bool

	applyCh chan ApplyMsg
}

//
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
	fmt.Printf(time.Now().Format("2006-01-02 15:04:05")+" id:%d persist term:%d, voteFor:%d, log:%v\n", rf.me, rf.currentTerm, rf.votedFor, rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) error {
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
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&log) != nil || d.Decode(&commitIndex) != nil {
		fmt.Printf(time.Now().Format("2006-01-02 15:04:05")+" id:%d recover and read persist nothing get2\n", rf.me)
		return errors.New("nothing get")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
		rf.commitIndex = commitIndex

		rf.matchIndex[rf.me] = len(rf.log) - 1
		fmt.Printf(time.Now().Format("2006-01-02 15:04:05")+" id:%d recover and read persist term:%d, voteFor:%d, log:%v\n", rf.me, rf.currentTerm, rf.votedFor, rf.log)
		return nil
	}
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

	fmt.Printf(time.Now().Format("2006-01-02 15:04:05")+" %d receive requestvote in term %d from %d for term %d\n", rf.me, rf.currentTerm, args.CandidateId, args.Term)

	reply.Term = rf.currentTerm

	// election restriction 1 bigger term win
	// election restriction 2 same term longer index win
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		fmt.Printf(time.Now().Format("2006-01-02 15:04:05")+" %d out of date vote from %d at term %d\n", rf.me, args.CandidateId, rf.currentTerm)
		return
	} else if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		reply.Term = rf.currentTerm
		rf.votedFor = -1
		rf.isLeader = false
		rf.persist()
	}

	// 5.4.2 selection restriction
	if rf.votedFor == args.CandidateId || rf.votedFor == -1 {
		if len(rf.log) > 1 && (args.LastLogTerm < rf.log[len(rf.log)-1].Term || (rf.log[len(rf.log)-1].Term == args.LastLogTerm && len(rf.log)-1 > args.LastLogIndex)) {
			fmt.Printf(time.Now().Format("2006-01-02 15:04:05")+" me:%d candidate:%d, can term:%d, can index:%d, my term:%d, my index:%d\n", rf.me, args.CandidateId, args.LastLogTerm, args.LastLogIndex, rf.log[len(rf.log)-1].Term, len(rf.log)-1)
			reply.VoteGranted = false
			return
		}
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.persist()
		fmt.Printf(time.Now().Format("2006-01-02 15:04:05")+" %d vote to candidate:%d, can term:%d, can index:%d, my term:%d, my index:%d\n", rf.me, args.CandidateId, args.LastLogTerm, args.LastLogIndex, rf.log[len(rf.log)-1].Term, len(rf.log)-1)

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

// TODO
func (rf *Raft) ReceiveAppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// heart beat and log
	//fmt.Printf(time.Now().Format("2006-01-02 15:04:05")+" %d receive a appendEntries from %d with term %d\n", rf.me, args.LeaderId, args.Term)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//fmt.Printf(time.Now().Format("2006-01-02 15:04:05")+" %d receive ReceiveAppendEntries from %d with term %d\n", rf.me, args.LeaderId, args.Term)
	reply.Term = rf.currentTerm

	// it's heartbeat
	// also use to reply
	// no use to distinguish heart beart
	// high term heart beat
	if rf.currentTerm > args.Term {
		fmt.Printf(time.Now().Format("2006-01-02 15:04:05")+" %d receive wrong term from %d, mine:%d, yours:%d\n", rf.me, args.LeaderId, rf.currentTerm, args.Term)
		reply.Success = false
		return
	}

	if rf.commitIndex > args.LeaderCommit {
		fmt.Printf(time.Now().Format("2006-01-02 15:04:05")+" %d receive wrong leader commit from %d, mine:%d, yours:%d\n", rf.me, args.LeaderId, rf.commitIndex, args.LeaderCommit)
		reply.Success = false
		return
	}

	rf.receiveHeartbeat = true

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
	if len(rf.log) > 1 {
		if len(rf.log) > args.PrevLogIndex && rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
			reply.Success = false
			fmt.Printf(time.Now().Format("2006-01-02 15:04:05") + " return false1\n")
			return
		} else if len(rf.log) <= args.PrevLogIndex {
			reply.Success = false
			fmt.Printf(time.Now().Format("2006-01-02 15:04:05")+" me:%d, prevLogIndex:%d, return false2, log:%v\n", rf.me, args.PrevLogIndex, rf.log)
			return
		}
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
		if args.LeaderCommit > len(rf.log)-1 {
			rf.commitIndex = len(rf.log) - 1
		} else {
			rf.commitIndex = args.LeaderCommit
		}
	}

	// update lastApplied
	// TODO before confirm apply, one should confirm if they are the same
	for rf.commitIndex > rf.lastApplied && rf.lastApplied+1 < len(rf.log) {
		rf.lastApplied++
		fmt.Printf(time.Now().Format("2006-01-02 15:04:05")+" follower:%d, last apply:%d, log[i]:%v, log:%v\n", rf.me, rf.lastApplied, rf.log[rf.lastApplied], rf.log)
		// what is apply log[lastApplied] to state machine
		// it is apply
		applyMsg := ApplyMsg{}
		applyMsg.CommandValid = true
		applyMsg.CommandIndex = rf.lastApplied
		applyMsg.Command = rf.log[applyMsg.CommandIndex].Command
		rf.applyCh <- applyMsg
	}
	reply.Success = true
	if len(args.Entries) > 0 {
		fmt.Printf(time.Now().Format("2006-01-02 15:04:05") + "ReceiveAppendEntries: end\n")
		fmt.Printf(time.Now().Format("2006-01-02 15:04:05")+"ReceiveAppendEntries: EntryLen %d, leaderCommit %d, leaderId %d, pervLogIndex %d, prevLogTerm %d, term %d\n",
			len(args.Entries), args.LeaderCommit, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, args.Term)
		fmt.Printf(time.Now().Format("2006-01-02 15:04:05")+"ReceiveAppendEntries: me: %d, commitIndex: %d, lastApplied: %d, matchIndex: %d, nextIndex: %d, logLen: %d\n",
			rf.me, rf.commitIndex, rf.lastApplied, rf.matchIndex, rf.nextIndex, len(rf.log))
		fmt.Printf(time.Now().Format("2006-01-02 15:04:05")+"ReceiveAppendEntries: log: %v\n", rf.log)
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
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if !rf.isLeader {
		isLeader = false
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
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		time.Sleep(time.Millisecond * time.Duration(150+rand.Intn(150)))
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		rf.mu.Lock()
		//fmt.Printf(time.Now().Format("2006-01-02 15:04:05")+" %d ticker start with term %d\n", rf.me, rf.currentTerm)

		// if receive heartbeart in last sleep then no selection
		if rf.receiveHeartbeat || rf.isLeader {
			rf.receiveHeartbeat = false
			//fmt.Printf(time.Now().Format("2006-01-02 15:04:05")+" %d receive heartbeat %t or I'm leader%t in term %d\n", rf.me, rf.receiveHeartbeat, rf.isLeader, rf.currentTerm)
			rf.mu.Unlock()
			continue
		}
		fmt.Printf(time.Now().Format("2006-01-02 15:04:05")+" %d start selection in term %d\n", rf.me, rf.currentTerm)

		// TODO consider the situation that long time no leader
		// not receive heart beat and start a se vglection
		args := RequestVoteArgs{}
		args.CandidateId = rf.me
		args.LastLogIndex = len(rf.log) - 1
		args.LastLogTerm = rf.log[args.LastLogIndex].Term
		rf.currentTerm++
		rf.votedFor = rf.me
		args.Term = rf.currentTerm
		rf.persist()

		// vote for self
		count := 1
		wg := sync.WaitGroup{}
		wg.Add(len(rf.peers) / 2)
		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			if rf.receiveHeartbeat || rf.isLeader {
				return
			}
			go func(i int, currentTerm int) {
				reply := RequestVoteReply{}
				fmt.Printf(time.Now().Format("2006-01-02 15:04:05")+" %d send request vote to %d for term %d\n", rf.me, i, args.Term)
				//if !rf.sendRequestVote(i, &args, &reply) {
				//	fmt.Printf(time.Now().Format("2006-01-02 15:04:05")+" %d can't get vote reply from %d for term %d\n", rf.me, i, args.Term)
				//} else {
				//	fmt.Printf(time.Now().Format("2006-01-02 15:04:05")+" %d get vote reply from %d vote for me: %t for term %d\n", rf.me, i, reply.VoteGranted, args.Term)
				//}
				rf.sendRequestVote(i, &args, &reply)
				rf.mu.Lock()
				// prevent timeout vote
				if reply.VoteGranted {
					count++
					if count-1 <= len(rf.peers)/2 {
						wg.Done()
					}
				}
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.persist()
				}
				rf.mu.Unlock()
			}(i, rf.currentTerm)
		}
		// if timeout then stop
		go func(term int) {
			wg.Wait()
			rf.mu.Lock()
			defer rf.mu.Unlock()
			fmt.Printf(time.Now().Format("2006-01-02 15:04:05")+" %d win term %d, current term %d\n", rf.me, term, rf.currentTerm)
			// condition2 when receiveHeartBeat
			if rf.receiveHeartbeat || rf.isLeader || rf.currentTerm != term {
				return
			}

			// condition1 when win
			fmt.Printf(time.Now().Format("2006-01-02 15:04:05")+" %d become leader\n", rf.me)
			rf.becomeLeader()
		}(rf.currentTerm)
		rf.mu.Unlock()

		time.Sleep(time.Millisecond * time.Duration(401))

		rf.mu.Lock()
		go func(term int) {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			// condition2 when receiveHeartBeat
			if rf.receiveHeartbeat || rf.isLeader || term != rf.currentTerm {
				return
			}

			// condition1 when win
			if count > len(rf.peers)/2 {
				fmt.Printf(time.Now().Format("2006-01-02 15:04:05")+" %d become leader\n", rf.me)
				rf.becomeLeader()
			}
			// condition3 when lose
		}(rf.currentTerm)
		fmt.Printf(time.Now().Format("2006-01-02 15:04:05")+" %d ticker end\n", rf.me)
		rf.mu.Unlock()
	}
}

func (rf *Raft) becomeLeader() {
	rf.isLeader = true
	for i := range rf.nextIndex {
		rf.nextIndex[i] = len(rf.log)
	}
	for i := range rf.matchIndex {
		rf.matchIndex[i] = 0
	}
	rf.matchIndex[rf.me] = len(rf.log) - 1
	// periodly heartbeat
	go rf.sendAllHeartbeat()
	go rf.heartbeatTicker()
	go rf.syncLogTicker()
}

// send HeartBeat
func (rf *Raft) heartbeatTicker() {
	for rf.killed() == false {
		rf.mu.Lock()
		if !rf.isLeader {
			rf.mu.Unlock()
			return
		}
		// heartbeat
		//fmt.Printf(time.Now().Format("2006-01-02 15:04:05")+" %d send all heartbeat in term %d\n", rf.me, rf.currentTerm)
		rf.mu.Unlock()
		go rf.sendAllHeartbeat()
		time.Sleep(time.Millisecond * 100)
	}
}

// TODO sync log ticker
func (rf *Raft) syncLogTicker() {

	rf.mu.Lock()
	fmt.Printf(time.Now().Format("2006-01-02 15:04:05")+" %d start sync log ticker\n", rf.me)
	canIn := make([]bool, len(rf.peers))
	for i, _ := range canIn {
		canIn[i] = true
	}
	rf.mu.Unlock()

	for {
		rf.mu.Lock()
		if !rf.isLeader {
			rf.mu.Unlock()
			break
		}
		rf.mu.Unlock()
		//fmt.Printf(time.Now().Format("2006-01-02 15:04:05")+" %d sync log to all\n", rf.me)

		time.Sleep(time.Millisecond * 100)
		//fmt.Printf(time.Now().Format("2006-01-02 15:04:05")+" %d syncing log ticker\n", rf.me)
		rf.mu.Lock()
		me := rf.me

		for i, peer := range rf.peers {
			if !rf.isLeader {
				break
			}
			if i == me || rf.matchIndex[i] >= len(rf.log)-1 {
				continue
			}
			go func(i int, peer *labrpc.ClientEnd) {
				rf.mu.Lock()
				defer rf.mu.Unlock()

				args := AppendEntriesArgs{}
				lastedEntryIndex := len(rf.log) - 1
				tempEntries := make([]Entry, len(rf.log)-rf.nextIndex[i])
				copy(tempEntries, rf.log[rf.nextIndex[i]:])
				args.Entries = tempEntries
				args.LeaderCommit = rf.commitIndex
				args.LeaderId = rf.me
				args.PrevLogIndex = rf.nextIndex[i] - 1
				args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
				args.Term = rf.currentTerm
				reply := AppendEntriesReply{}
				fmt.Printf(time.Now().Format("2006-01-02 15:04:05")+" %d send log to %d, %v\n", rf.me, i, args.Entries)
				done := make(chan bool, 1)
				if canIn[i] == true {
					canIn[i] = false
					go func() {
						done <- peer.Call("Raft.ReceiveAppendEntries", &args, &reply)
					}()
				} else {
					return
				}

				var ok bool
				select {
				case ok = <-done:
					canIn[i] = true
				case <-time.After(time.Duration(1000 * time.Millisecond)):
					canIn[i] = true
					fmt.Println("timeout!!!")
					return
				}
				//ok := peer.Call("Raft.ReceiveAppendEntries", &args, &reply)

				if ok {
					fmt.Printf(time.Now().Format("2006-01-02 15:04:05")+" %v reply success\n", reply.Success)
					// if there is some one reconnect with high term then exit leader state
					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.isLeader = false
						fmt.Printf(time.Now().Format("2006-01-02 15:04:05")+" me:%d in ???? %d fail and nextIndex-- to %d\n", rf.me, i, rf.nextIndex[i])
						return
					}

					if reply.Success == false {
						if rf.nextIndex[i]-1 <= 0 {
							rf.nextIndex[i] = 1
						} else {
							rf.nextIndex[i]--
						}
						fmt.Printf(time.Now().Format("2006-01-02 15:04:05")+" me:%d follower:%d fail and nextIndex-- to %d\n", rf.me, i, rf.nextIndex[i])
						return
					}

					// update commitIndex
					// TODO monotonically?
					rf.matchIndex[i] = lastedEntryIndex
					// TODO update nextIndex it equals to lastedEntryIndex+1?
					rf.nextIndex[i] = rf.matchIndex[i] + 1
					for j := rf.commitIndex + 1; j <= lastedEntryIndex && j < len(rf.log); j++ {
						count := 0
						for k, _ := range rf.peers {
							if rf.matchIndex[k] >= j {
								count++
							}
						}
						fmt.Printf(time.Now().Format("2006-01-02 15:04:05")+" leader:%d, index:%d, count:%d, matchIndex:%v\n", rf.me, j, count, rf.matchIndex)
						if count > len(rf.peers)/2 && rf.log[j].Term == rf.currentTerm {
							rf.commitIndex++
							fmt.Printf(time.Now().Format("2006-01-02 15:04:05")+" leader:%d, commitIndex to %d\n", rf.me, rf.commitIndex)
							// apply is end state, commit is not
							for rf.commitIndex > rf.lastApplied {
								rf.lastApplied++
								fmt.Printf(time.Now().Format("2006-01-02 15:04:05")+" leader:%d, apply to %d, log[i]:%v, log:%v\n", rf.me, rf.lastApplied, rf.log[rf.lastApplied], rf.log)
								applyMsg := ApplyMsg{}
								applyMsg.CommandValid = true
								applyMsg.CommandIndex = rf.lastApplied
								applyMsg.Command = rf.log[applyMsg.CommandIndex].Command
								rf.applyCh <- applyMsg

								go rf.sendAllHeartbeat()
							}
							fmt.Printf(time.Now().Format("2006-01-02 15:04:05")+" leader %d get applyPeerNum %d increase to commitindex %d\n", rf.me, count, rf.commitIndex)
						}
					}
				}
			}(i, peer)
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) sendAllHeartbeat() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	fmt.Printf(time.Now().Format("2006-01-02 15:04:05")+" leader %d send all heartbeat\n", rf.me)

	for i := range rf.peers {
		if !rf.isLeader {
			break
		}
		if i == rf.me {
			continue
		}
		go func(i int) {
			rf.mu.Lock()
			args := AppendEntriesArgs{}
			args.Term = rf.currentTerm
			args.LeaderId = rf.me
			// TODO want if follower wrong log but with leaderCommit send to follower
			// TODO once confirm one
			args.Entries = make([]Entry, 0)
			args.PrevLogIndex = rf.nextIndex[i] - 1
			args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
			if rf.commitIndex > rf.matchIndex[i] {
				args.LeaderCommit = rf.matchIndex[i]
			} else {
				args.LeaderCommit = rf.commitIndex
			}

			reply := AppendEntriesReply{}
			rf.mu.Unlock()
			rf.sendAppendEntries(i, &args, &reply)

			rf.mu.Lock()
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.isLeader = false
			}
			rf.mu.Unlock()
		}(i)
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
	rf.matchIndex = make([]int, len(rf.peers))
	rf.nextIndex = make([]int, len(rf.peers))

	// initialize from state persisted before a crash
	err := rf.readPersist(persister.ReadRaftState())

	// TODO if can't get from persist then give them value
	if err != nil {
		rf.currentTerm = 0
		rf.votedFor = -1
		// it's leading to the index start with 1 but there are 1 more log
		rf.log = make([]Entry, 0)
		rf.log = append(rf.log, Entry{})
	}
	rf.receiveHeartbeat = false

	rf.applyCh = applyCh
	// start ticker goroutine to start elections
	fmt.Printf(time.Now().Format("2006-01-02 15:04:05")+" %d created\n", me)
	go rf.ticker()
	return rf
}
