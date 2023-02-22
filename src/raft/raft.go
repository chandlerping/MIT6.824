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
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
)

// import "bytes"
// import "../labgob"

const heartbeatInterval time.Duration = 120 * time.Millisecond

// ApplyMsg ...
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

// Raft ...
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

	currentTerm int
	votedFor    int
	logs        []LogEntry

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	idendity string // "Leader", "Follower", "Candidate"
	counter  int    // counter for election

	heartbeatCh   chan bool // receive heartbeat
	degradeCh     chan bool // transfer from "Leader" or "Candidate" to "Follower"
	voteGrantCh   chan bool // grant vote to another candidate
	winElectionCh chan bool // win an election
	clientCh      chan bool // client calls
	applyCh       chan ApplyMsg
}

// LogEntry struct
type LogEntry struct {
	Term    int
	Command interface{}
}

// GetState ...
// return CurrentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	idendity := rf.idendity

	if idendity == "Leader" {
		isleader = true
	} else {
		isleader = false
	}

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
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var logs []LogEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logs) != nil {
		// error
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.logs = logs
	}
}

//
// Idendity conversions
//

// from "Leader" or "Candidate" to "Follower"
func (rf *Raft) convertToFollower(newTerm int) {
	// Since this is done before the message is sent to
	// the channel of "Leader" or "Candidate", there is
	// no need to reset the channels otherwise there will
	// be a race. Also it is always called inside a lock.
	// oldIdendity := rf.idendity
	// This is an atomic operation.
	oldIdendity := rf.idendity
	rf.idendity = "Follower"
	rf.currentTerm = newTerm
	rf.votedFor = -1
	rf.persist()
	if oldIdendity != "Follower" {
		rf.sendCh(rf.degradeCh, true)
	}
}

// from "Candidate" to "Leader"
func (rf *Raft) convertToLeader() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.idendity != "Candidate" {
		return
	}

	// need to reset nextIndex and matchIndex
	for i := range rf.nextIndex {
		rf.nextIndex[i] = rf.getLastIndex() + 1
		rf.matchIndex[i] = 0
	}
	rf.matchIndex[rf.me] = rf.getLastIndex()

	rf.resetChannels()
	rf.idendity = "Leader"
	rf.startAppendEntries()
}

// from "Candidate" or "Follower" to "Candidate"
func (rf *Raft) convertToCandidate(oldIdendity string) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.idendity != oldIdendity {
		return
	}

	rf.resetChannels()
	rf.idendity = "Candidate"
	rf.startElection()
}

//
// Channel operations
//
func (rf *Raft) resetChannels() {
	rf.degradeCh = make(chan bool)
	rf.heartbeatCh = make(chan bool)
	rf.voteGrantCh = make(chan bool)
	rf.winElectionCh = make(chan bool)
	rf.clientCh = make(chan bool)
}

func (rf *Raft) sendCh(channel chan bool, value bool) {
	select {
	case channel <- value:
	default:
	}
}

//
// Election
//

// RequestVoteArgs ...
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // candidate's term
	CandidateID  int // candidate's id
	LastLogIndex int
	LastLogTerm  int
}

// RequestVoteReply ...
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term         int // CurrentTerm
	VotedGranted bool
}

// RequestVote ...
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	reply.Term = rf.currentTerm
	reply.VotedGranted = false

	// refuse the request
	if args.Term < rf.currentTerm {
		return
	}

	// A more up-to-date request, convert to an initial follower anyway,
	// maybe from a "Leader", "Follower" or "Candidate".
	if args.Term > rf.currentTerm {
		rf.convertToFollower(args.Term)
	}

	// refuse if rf is more up-to-date than candidate
	if rf.logs[rf.getLastIndex()].Term > args.LastLogTerm {
		return
	} else if rf.logs[rf.getLastIndex()].Term == args.LastLogTerm &&
		rf.getLastIndex() > args.LastLogIndex {
		return
	}

	// grant the vote, check if this follower has already voted
	if rf.votedFor == -1 || rf.votedFor == args.CandidateID {
		rf.votedFor = args.CandidateID
		rf.currentTerm = args.Term
		reply.VotedGranted = true
		rf.sendCh(rf.voteGrantCh, true)
	}

	// rf.persist()
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// get a vote successfully, or convert to follower if not
	if reply.VotedGranted == true {
		rf.counter++
		if rf.counter > len(rf.peers)/2 {
			rf.sendCh(rf.winElectionCh, true)
		}
	} else if reply.Term > rf.currentTerm {
		rf.convertToFollower(reply.Term)
	}
}

// Candidate starts the election
func (rf *Raft) startElection() {
	// locked in convertToCandidate
	if rf.idendity != "Candidate" {
		return
	}

	rf.currentTerm++
	rf.votedFor = rf.me
	rf.counter = 1

	rf.persist()

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		args := RequestVoteArgs{}
		args.Term = rf.currentTerm
		args.CandidateID = rf.me
		args.LastLogIndex = rf.getLastIndex()
		args.LastLogTerm = rf.logs[args.LastLogIndex].Term
		reply := RequestVoteReply{}
		go rf.sendRequestVote(i, &args, &reply)
	}
}

//
// Append entries
//

// AppendEntriesArgs struct
type AppendEntriesArgs struct {
	Term         int // leader's term
	LeaderID     int // leader's id
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

// AppendEntriesReply struct
type AppendEntriesReply struct {
	Term          int  // current term
	Success       bool // true if follower contained entry matching prevLogIndex and prevLogTerm
	ConflictTerm  int
	ConflictIndex int
}

// AppendEntries func
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	reply.Success = false
	reply.Term = rf.currentTerm

	// reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		return
	}

	// convert to an initial follower
	if args.Term > rf.currentTerm {
		rf.convertToFollower(args.Term)
	}

	rf.sendCh(rf.heartbeatCh, true)

	if rf.getLastIndex() < args.PrevLogIndex {
		reply.ConflictIndex = rf.getLastIndex() + 1
		return
	} else if rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.ConflictTerm = rf.logs[rf.getLastIndex()].Term
		reply.ConflictIndex = rf.findConflictIndex(reply.ConflictTerm)
		if args.PrevLogIndex < reply.ConflictIndex {
			reply.ConflictIndex = args.PrevLogIndex
		}
		rf.logs = rf.logs[:reply.ConflictIndex]
		return
	} else {
		// success
		rf.logs = rf.logs[:args.PrevLogIndex+1]
		rf.logs = append(rf.logs, args.Entries...)
	}
	// rf.persist()

	// update follower's commitIndex
	oldCommitIndex := rf.commitIndex
	if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit > rf.getLastIndex() {
			rf.commitIndex = rf.getLastIndex()
		} else {
			rf.commitIndex = args.LeaderCommit
		}
	}

	// applyCh
	go rf.applyEntry(oldCommitIndex)

	reply.Success = true
}

// leader sends AppendEntries RPCs
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	// race check
	if rf.idendity != "Leader" {
		return
	}

	if reply.Term > rf.currentTerm {
		rf.convertToFollower(reply.Term)
		return
	}

	// contain an entry
	if len(args.Entries) > 0 {
		if reply.Success {
			rf.nextIndex[server] = rf.getLastIndex() + 1
			rf.matchIndex[server] = rf.getLastIndex()
		} else if reply.Term <= rf.currentTerm {
			rf.nextIndex[server] = reply.ConflictIndex
		}
	}

	// update leader's commitIndex
	oldCommitIndex := rf.commitIndex
	for n := rf.getLastIndex(); n > oldCommitIndex; n-- {
		if rf.logs[n].Term < rf.currentTerm {
			break
		} else if rf.logs[n].Term > rf.currentTerm {
			continue
		}

		count := 0
		for i := 0; i < len(rf.peers); i++ {
			if rf.matchIndex[i] >= n {
				count++
			}
		}
		if count > len(rf.peers)/2 && rf.logs[n].Term == rf.currentTerm {
			rf.commitIndex = n
			go rf.applyEntry(oldCommitIndex)
			break
		}
	}
}

func (rf *Raft) startAppendEntries() {
	if rf.idendity != "Leader" {
		return
	}

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		args := AppendEntriesArgs{}
		args.Term = rf.currentTerm
		args.LeaderID = rf.me
		args.LeaderCommit = rf.commitIndex

		// need to replicate logs
		args.PrevLogIndex = rf.nextIndex[i] - 1
		args.PrevLogTerm = rf.logs[args.PrevLogIndex].Term
		if rf.getLastIndex() >= rf.nextIndex[i] {
			for j := rf.nextIndex[i]; j <= rf.getLastIndex(); j++ {
				entry := LogEntry{}
				entry.Term = rf.logs[j].Term
				entry.Command = rf.logs[j].Command
				args.Entries = append(args.Entries, entry)
			}
		}

		reply := AppendEntriesReply{}

		go rf.sendAppendEntries(i, &args, &reply)
	}
}

// Start ...
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
	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term := rf.currentTerm

	if rf.idendity != "Leader" {
		return -1, term, false
	}

	rf.logs = append(rf.logs, LogEntry{term, command})
	rf.persist()
	rf.matchIndex[rf.me] = rf.getLastIndex()
	return rf.getLastIndex(), term, true
}

// Kill ...
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

// Make ...
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
	rf.idendity = "Follower"
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.applyCh = applyCh
	rf.resetChannels()
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.logs = append(rf.logs, LogEntry{Term: 0})

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// 2A: Main background goroutine
	go rf.backgroundLoop()

	return rf
}

// backgroundLoop is the main loop of every server
func (rf *Raft) backgroundLoop() {
	for rf.killed() == false {
		rf.mu.Lock()
		idendity := rf.idendity
		rf.mu.Unlock()

		if idendity == "Leader" {
			select {
			case <-rf.degradeCh:
				// do nothing
			case <-time.After(heartbeatInterval):
				// send heartbeats
				rf.mu.Lock()
				rf.startAppendEntries()
				rf.mu.Unlock()
			}
		} else if idendity == "Candidate" {
			select {
			case <-rf.degradeCh:
				// do nothing
			case <-rf.winElectionCh:
				// convert to leader
				rf.convertToLeader()
			case <-time.After(randTimeout()):
				// restart election
				rf.convertToCandidate("Candidate")
			}
		} else if idendity == "Follower" {
			select {
			case <-rf.voteGrantCh:
				// do nothing
			case <-rf.heartbeatCh:
				// do nothing
			case <-time.After(randTimeout()):
				// convert to candidate
				rf.convertToCandidate("Follower")
			}
		}
	}
}

// get the last index in the logs
func (rf *Raft) getLastIndex() int {
	return len(rf.logs) - 1
}

// apply the log entries starting from oldIndex
func (rf *Raft) applyEntry(oldIndex int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for i := oldIndex + 1; i <= rf.commitIndex; i++ {
		applyMsg := ApplyMsg{}
		applyMsg.CommandValid = true
		applyMsg.Command = rf.logs[i].Command
		applyMsg.CommandIndex = i
		rf.applyCh <- applyMsg
	}
}

// find the first index with a term
func (rf *Raft) findConflictIndex(term int) int {
	for i := 0; i < len(rf.logs); i++ {
		if rf.logs[i].Term == term {
			return i
		}
	}
	return -1
}

// get a random timeout
func randTimeout() time.Duration {
	return time.Duration(rand.Intn(240)+360) * time.Millisecond
}
