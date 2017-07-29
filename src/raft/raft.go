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
	"labrpc"
	"math/rand"
	"sync"
	"time"
)

// import "bytes"
// import "encoding/gob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

//
// Log structure contains current term and the actual command.
//
type Log struct {
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

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	role        string
	currentTerm int
	votedFor    int
	log         []Log
	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int
	heartbeatCh chan bool
	electionCh  chan bool
	voteCount   int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = (rf.role == "leader")
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
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateID  int
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

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Default reply arguments
	reply.VoteGranted = false
	reply.Term = rf.currentTerm

	// Vote only when self's term <= candidate's term.
	if args.Term < rf.currentTerm {
		return
	}

	// Incoming candidate with higher term.
	// Vote for it and convert to follower.
	if args.Term > rf.currentTerm {
		rf.mu.Lock()
		rf.currentTerm = args.Term
		//rf.votedFor = args.CandidateID
		rf.role = "follower"
		rf.votedFor = -1
		rf.mu.Unlock()
		reply.Term = rf.currentTerm
		//reply.VoteGranted = true
	}

	var selfLogIndex = len(rf.log) - 1
	var selfLogTerm = rf.log[selfLogIndex].Term
	// Election restriction
	if args.LastLogTerm < selfLogTerm {
		return
	}
	if (args.LastLogTerm == selfLogTerm) && (args.LastLogIndex < selfLogIndex) {
		return
	}

	// Only one vote in one term
	if rf.votedFor != -1 {
		return
	}

	// Only renew the timeout when granting a vote
	if rf.role == "follower" {
		rf.heartbeatCh <- true
	}
	rf.mu.Lock()
	rf.votedFor = args.CandidateID
	rf.mu.Unlock()
	reply.VoteGranted = true
	return
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

//
// BroadcastRequestVote broadcast request votes to all other servers.
//
func (rf *Raft) BroadcastRequestVote() {
	args := &RequestVoteArgs{
		CandidateID:  rf.me,
		Term:         rf.currentTerm,
		LastLogIndex: len(rf.log) - 1,
		LastLogTerm:  0,
	}
	if args.LastLogIndex > -1 {
		args.LastLogTerm = rf.log[args.LastLogIndex].Term
	}

	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go func(server int) {
				reply := &RequestVoteReply{}
				ret := rf.sendRequestVote(server, args, reply)
				if ret == true {
					if reply.VoteGranted == true {
						rf.mu.Lock()
						rf.voteCount++
						if (rf.voteCount > (len(rf.peers) / 2)) && rf.role == "candidate" {
							rf.electionCh <- true
							rf.role = "leader"
							rf.matchIndex = []int{}
							rf.nextIndex = []int{}
							for i := 0; i < len(rf.peers); i++ {
								rf.nextIndex = append(rf.nextIndex, len(rf.log))
								rf.matchIndex = append(rf.matchIndex, 0)
							}
						}
						rf.mu.Unlock()
					} else {
						if reply.Term > rf.currentTerm {
							rf.mu.Lock()
							rf.role = "follower"
							rf.currentTerm = reply.Term
							rf.mu.Unlock()
						}
					}
				}
			}(i)
		}
	}
}

//
// AppendEntriesArgs is the append entries structure
//
type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Log
	LeaderCommit int
}

//
// AppendEntriesReply is the reply of the append entries structure
//
type AppendEntriesReply struct {
	Term      int
	Success   bool
	NextIndex int
}

//
// AppendEntries RPC handler.
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.heartbeatCh <- true

	reply.NextIndex = len(rf.log)
	// Receiver Step 1
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	// Receiver Step 2
	if len(rf.log)-1 < args.PrevLogIndex {
		reply.Term = rf.currentTerm
		reply.Success = false

		return
	}
	// Receiver Step 3
	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		for i := args.PrevLogIndex; i > 0; i-- {
			if rf.log[i].Term != rf.log[i-1].Term {
				reply.NextIndex = i
				break
			}
		}
		rf.log = rf.log[:reply.NextIndex]
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	// Receiver Step 4
	if len(args.Entries) != 0 {
		rf.log = rf.log[:args.PrevLogIndex+1]
		for i := 0; i < len(args.Entries); i++ {
			rf.log = append(rf.log, args.Entries[i])

		}
		reply.NextIndex = len(rf.log)
	}
	// Receiver Step 5
	if args.LeaderCommit > rf.commitIndex {

		if args.LeaderCommit < len(rf.log)-1 {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = len(rf.log) - 1
		}
	}
	reply.Success = true
	reply.Term = rf.currentTerm
	return
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// BroadcastHeartbeat broadcast heartbeat to all the follower servers.
//
func (rf *Raft) BroadcastHeartbeat() {

	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go func(server int) {
				args := &AppendEntriesArgs{}
				args.LeaderID = rf.me
				args.Term = rf.currentTerm
				args.LeaderCommit = rf.commitIndex
				args.PrevLogIndex = rf.nextIndex[server] - 1
				args.PrevLogTerm = rf.log[args.PrevLogIndex].Term

				if args.PrevLogIndex == len(rf.log)-1 {
					args.Entries = []Log{}
				} else {
					args.Entries = rf.log[args.PrevLogIndex+1:]
				}
				reply := &AppendEntriesReply{}
				ok := rf.sendAppendEntries(server, args, reply)
				if ok {
					if reply.Success == true {
						// Only when it's not a pure heartbeat
						if len(args.Entries) > 0 {
							rf.mu.Lock()
							rf.nextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
							rf.matchIndex[server] = rf.nextIndex[server] - 1
							rf.mu.Unlock()
						}
					} else {
						if reply.Term > rf.currentTerm {
							// Turn to follower
							rf.mu.Lock()
							rf.role = "follower"
							rf.currentTerm = reply.Term
							rf.mu.Unlock()
							return
						}

						// Only when it's not a pure heartbeat
						if args.Term == rf.currentTerm {
							rf.mu.Lock()
							rf.nextIndex[server] = reply.NextIndex
							rf.mu.Unlock()
						}
					}
				}
			}(i)
		}
	}
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
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
	rf.mu.Lock()
	index = len(rf.log)
	term = rf.currentTerm
	isLeader = false
	if rf.role == "leader" {
		isLeader = true
		rf.log = rf.log[:index]
		entry := new(Log)
		entry.Command = command
		entry.Term = rf.currentTerm
		rf.log = append(rf.log, *entry)
		rf.nextIndex[rf.me] = len(rf.log)
		rf.matchIndex[rf.me] = rf.nextIndex[rf.me] - 1
	}
	rf.mu.Unlock()
	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

//
// LeaderState serves the role of leader
//
func (rf *Raft) LeaderState() {
	// The time interval set below is set to meet the requirement of tester.
	time.Sleep(time.Duration(50) * time.Millisecond)
	if rf.role == "leader" {
		rf.BroadcastHeartbeat()
	}
}

//
// CandidateState serves the role of candidate
//
func (rf *Raft) CandidateState() {
	select {
	case <-rf.electionCh:
	default:
	}
	rf.mu.Lock()
	rf.voteCount = 1
	rf.votedFor = rf.me
	rf.currentTerm++
	rf.mu.Unlock()
	rf.BroadcastRequestVote()
	timeout := time.Duration(rand.Intn(100)+200) * time.Millisecond
	select {
	case <-rf.electionCh:
		return
	case <-time.After(timeout):
		return
	}

}

//
// FollowerState serves the role of follower
//
func (rf *Raft) FollowerState() {
	timeout := time.Duration(rand.Intn(300)+500) * time.Millisecond
	select {
	case <-rf.heartbeatCh:
	case <-time.After(timeout):
		rf.mu.Lock()
		rf.role = "candidate"
		rf.mu.Unlock()
	}
}

//
// StateManager take action accroding to the role of the server
//
func (rf *Raft) StateManager() {
	for {
		if rf.role == "follower" {
			rf.FollowerState()
		} else if rf.role == "candidate" {
			rf.CandidateState()
		} else {
			rf.LeaderState()
		}
	}
}

//
// LeaderCommit update the leadercommit in leader server
//
func (rf *Raft) LeaderCommit() {
	count := 0
	newCommit := rf.commitIndex + 1
	for i := rf.commitIndex + 1; i <= len(rf.log); i++ {
		if i == len(rf.log) {
			return
		}
		if rf.log[i].Term == rf.currentTerm {
			newCommit = i
			break
		}
	}

	for i := 0; i < len(rf.peers); i++ {
		// Not exist in this server
		if len(rf.log)-1 < newCommit {
			continue
		}
		if (rf.matchIndex[i] >= newCommit) && (rf.log[newCommit].Term == rf.currentTerm) {
			count++
		}
	}
	if count > (len(rf.peers) / 2) {
		rf.mu.Lock()
		rf.commitIndex = newCommit
		rf.mu.Unlock()
	}
}

//
// LogManager commit logs.
//
func (rf *Raft) LogManager(applyCh chan ApplyMsg) {
	for {
		if rf.role == "leader" {
			go rf.LeaderCommit()
		}
		time.Sleep(10 * time.Millisecond)
		if rf.commitIndex > rf.lastApplied {
			go func() {
				rf.mu.Lock()
				oldApplied := rf.lastApplied
				commitIndex := rf.commitIndex
				rf.mu.Unlock()
				for i := oldApplied + 1; i <= commitIndex; i++ {
					Msg := new(ApplyMsg)
					Msg.Index = i
					Msg.Command = rf.log[i].Command
					applyCh <- *Msg
					rf.mu.Lock()
					rf.lastApplied++
					rf.mu.Unlock()
				}
			}()
		}
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
	rf.role = "follower"
	rf.currentTerm = 1
	rf.votedFor = -1
	rf.electionCh = make(chan bool)
	rf.heartbeatCh = make(chan bool)
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.voteCount = 0
	rf.log = []Log{Log{Term: 0}}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.StateManager()
	go rf.LogManager(applyCh)

	return rf
}
