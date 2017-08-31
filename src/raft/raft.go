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
	"encoding/gob"
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
	Role        string
	CurrentTerm int
	VotedFor    int
	VoteCount   int
	Log         []Log
	CommitIndex int
	LastApplied int
	NextIndex   []int
	MatchIndex  []int

	// Channel for message communication
	heartbeatCh chan bool
	electionCh  chan bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.CurrentTerm
	isleader = (rf.Role == "leader")
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
	e := gob.NewEncoder(w)
	e.Encode(rf.Role)
	e.Encode(rf.VoteCount)
	e.Encode(rf.VotedFor)
	e.Encode(rf.CurrentTerm)
	//e.Encode(rf.CommitIndex)
	//e.Encode(rf.LastApplied)
	//e.Encode(rf.MatchIndex)
	//e.Encode(rf.NextIndex)
	e.Encode(rf.Log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.Role)
	d.Decode(&rf.VoteCount)
	d.Decode(&rf.VotedFor)
	d.Decode(&rf.CurrentTerm)
	//d.Decode(&rf.CommitIndex)
	//d.Decode(&rf.LastApplied)
	//d.Decode(&rf.MatchIndex)
	//d.Decode(&rf.NextIndex)
	d.Decode(&rf.Log)
	if len(rf.Log) == 0 {
		// We should be in initial state of a server
		rf.Log = append(rf.Log, Log{Term: 0})
	}
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
	reply.Term = rf.CurrentTerm
	var (
		selfLogIndex = len(rf.Log) - 1
		selfLogTerm  = rf.Log[selfLogIndex].Term
	)

	// Vote only when self's term <= candidate's term.
	if args.Term < rf.CurrentTerm {
		return
	}

	// Incoming candidate with higher term.
	// Vote for it and convert to follower.
	if args.Term > rf.CurrentTerm {
		rf.mu.Lock()
		rf.CurrentTerm = args.Term
		//rf.VotedFor = args.CandidateID
		rf.Role = "follower"
		rf.VotedFor = -1
		rf.mu.Unlock()
		rf.persist()
		reply.Term = rf.CurrentTerm
		//reply.VoteGranted = true
	}

	// Election restriction
	if args.LastLogTerm < selfLogTerm {
		return
	}
	if (args.LastLogTerm == selfLogTerm) && (args.LastLogIndex < selfLogIndex) {
		return
	}

	// Only one vote in one term
	if rf.VotedFor != -1 {
		return
	}

	DPrintf("%d(%d)[%d.%d] -> %d[%d.%d]\n",
		args.CandidateID, args.Term, args.LastLogIndex, args.LastLogTerm,
		rf.me, selfLogIndex, selfLogTerm)

	// Only renew the timeout when granting a vote
	if rf.Role == "follower" {
		rf.heartbeatCh <- true
	}
	rf.mu.Lock()
	rf.VotedFor = args.CandidateID
	rf.mu.Unlock()
	rf.persist()
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
		Term:         rf.CurrentTerm,
		LastLogIndex: len(rf.Log) - 1,
		LastLogTerm:  0,
	}
	if args.LastLogIndex > -1 {
		args.LastLogTerm = rf.Log[args.LastLogIndex].Term
	}

	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go func(server int) {
				reply := &RequestVoteReply{}
				ret := rf.sendRequestVote(server, args, reply)
				if ret == true {
					if reply.VoteGranted == true {
						if reply.Term == rf.CurrentTerm {
							rf.mu.Lock()
							rf.VoteCount++
							if (rf.VoteCount > (len(rf.peers) / 2)) && rf.Role == "candidate" {
								DPrintf("[election] %d(%d)\n", rf.me, rf.CurrentTerm)
								rf.electionCh <- true
								rf.Role = "leader"
								rf.MatchIndex = []int{}
								rf.NextIndex = []int{}
								for i := 0; i < len(rf.peers); i++ {
									rf.NextIndex = append(rf.NextIndex, len(rf.Log))
									rf.MatchIndex = append(rf.MatchIndex, 0)
								}
							}
							rf.mu.Unlock()
							rf.persist()
						}
					} else {
						if reply.Term > rf.CurrentTerm {
							rf.mu.Lock()
							rf.Role = "follower"
							rf.CurrentTerm = reply.Term
							rf.mu.Unlock()
							rf.persist()
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

	reply.NextIndex = len(rf.Log)
	// Receiver Step 1
	if args.Term < rf.CurrentTerm {
		reply.Term = rf.CurrentTerm
		reply.Success = false
		return
	}
	// Receiver Step 2
	if len(rf.Log)-1 < args.PrevLogIndex {
		reply.Term = rf.CurrentTerm
		reply.Success = false

		return
	}
	// Receiver Step 3
	if rf.Log[args.PrevLogIndex].Term != args.PrevLogTerm {
		for i := args.PrevLogIndex; i > 0; i-- {
			if rf.Log[i].Term != rf.Log[i-1].Term {
				reply.NextIndex = i
				break
			}
		}
		rf.mu.Lock()
		rf.Log = rf.Log[:reply.NextIndex]
		rf.mu.Unlock()
		reply.Term = rf.CurrentTerm
		reply.Success = false
		return
	}
	// Receiver Step 4
	if len(args.Entries) != 0 {
		rf.mu.Lock()
		rf.Log = rf.Log[:args.PrevLogIndex+1]
		for i := 0; i < len(args.Entries); i++ {
			rf.Log = append(rf.Log, args.Entries[i])
		}
		rf.mu.Unlock()
		rf.persist()
		DPrintf("[append] %d (%d-%d) %+v A%d From(%d,%d)\n", rf.me, len(rf.Log)-len(args.Entries), len(rf.Log)-1, args.Entries, rf.LastApplied, args.LeaderID, args.Term)

		reply.NextIndex = len(rf.Log)
	}
	// Receiver Step 5
	if args.LeaderCommit > rf.CommitIndex {
		var prevCommit = rf.CommitIndex
		rf.mu.Lock()
		if args.LeaderCommit < len(rf.Log)-1 {
			rf.CommitIndex = args.LeaderCommit
		} else {
			rf.CommitIndex = len(rf.Log) - 1
		}
		rf.mu.Unlock()
		DPrintf("[update commit] %d (%d,%d)\n", rf.me, prevCommit, rf.CommitIndex)
		rf.persist()
	}

	reply.Success = true
	reply.Term = rf.CurrentTerm
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
				args.Term = rf.CurrentTerm
				args.LeaderCommit = rf.CommitIndex
				args.PrevLogIndex = rf.NextIndex[server] - 1
				args.PrevLogTerm = rf.Log[args.PrevLogIndex].Term

				if args.PrevLogIndex == len(rf.Log)-1 {
					args.Entries = []Log{}
				} else {
					args.Entries = rf.Log[args.PrevLogIndex+1:]
				}
				reply := &AppendEntriesReply{}
				ok := rf.sendAppendEntries(server, args, reply)
				if ok {
					if reply.Success == true {
						// Only when it's not a pure heartbeat
						if len(args.Entries) > 0 {
							rf.mu.Lock()
							rf.NextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
							rf.MatchIndex[server] = rf.NextIndex[server] - 1
							rf.mu.Unlock()
							rf.persist()
						}
					} else {
						if reply.Term > rf.CurrentTerm {
							// Turn to follower
							rf.mu.Lock()
							rf.Role = "follower"
							rf.CurrentTerm = reply.Term
							rf.mu.Unlock()
							rf.persist()
							return
						}

						// Only when it's not a pure heartbeat
						if args.Term == rf.CurrentTerm {
							rf.mu.Lock()
							rf.NextIndex[server] = reply.NextIndex
							rf.mu.Unlock()
							rf.persist()
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
	index = len(rf.Log)
	term = rf.CurrentTerm
	isLeader = false
	if rf.Role == "leader" {
		isLeader = true
		rf.Log = rf.Log[:index]
		entry := new(Log)
		entry.Command = command
		entry.Term = rf.CurrentTerm
		rf.Log = append(rf.Log, *entry)
		rf.NextIndex[rf.me] = len(rf.Log)
		rf.MatchIndex[rf.me] = rf.NextIndex[rf.me] - 1
		DPrintf("[new] %d(%d) (%d,%d)\n", rf.me, rf.CurrentTerm, len(rf.Log)-1, command)
	}
	rf.mu.Unlock()
	rf.persist()
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
	if rf.Role == "leader" {
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
	rf.VoteCount = 1
	rf.VotedFor = rf.me
	rf.CurrentTerm++
	rf.mu.Unlock()
	rf.persist()
	DPrintf("[new candidate term] %d(%d)\n", rf.me, rf.CurrentTerm)
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
		rf.Role = "candidate"
		rf.mu.Unlock()
		rf.persist()
	}
}

//
// StateManager take action accroding to the role of the server
//
func (rf *Raft) StateManager() {
	for {
		if rf.Role == "follower" {
			rf.FollowerState()
		} else if rf.Role == "candidate" {
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
	newCommit := rf.CommitIndex + 1
	for i := rf.CommitIndex + 1; i <= len(rf.Log); i++ {
		if i == len(rf.Log) {
			return
		}
		if rf.Log[i].Term == rf.CurrentTerm {
			newCommit = i
			break
		}
	}

	for i := 0; i < len(rf.peers); i++ {
		// Not exist in this server
		if len(rf.Log)-1 < newCommit {
			continue
		}
		if (rf.MatchIndex[i] >= newCommit) && (rf.Log[newCommit].Term == rf.CurrentTerm) {
			count++
		}
	}
	if count > (len(rf.peers) / 2) {
		rf.mu.Lock()
		rf.CommitIndex = newCommit
		rf.mu.Unlock()
		rf.persist()
		DPrintf("[leader commit] %d(%d) %d\n", rf.me, rf.CurrentTerm, newCommit)
	}
}

//
// LogManager commit logs.
//
func (rf *Raft) LogManager(applyCh chan ApplyMsg) {
	for {
		if rf.Role == "leader" {
			go rf.LeaderCommit()
		}
		time.Sleep(10 * time.Millisecond)
		if rf.CommitIndex > rf.LastApplied {
			go func() {
				DPrintf("[apply] %d(%d) (%d, %d) A%d\n", rf.me, rf.CurrentTerm, rf.LastApplied, rf.CommitIndex, rf.LastApplied)
				rf.mu.Lock()
				oldApplied := rf.LastApplied
				commitIndex := rf.CommitIndex
				rf.mu.Unlock()
				for i := oldApplied + 1; i <= commitIndex; i++ {
					Msg := ApplyMsg{Index: i, Command: rf.Log[i].Command}
					applyCh <- Msg
					//DPrintf("[apply] %d (%d, %d)\n", rf.me, Msg.Index, Msg.Command)
				}
				rf.mu.Lock()
				rf.LastApplied = commitIndex
				rf.mu.Unlock()
				rf.persist()
				DPrintf("[apply index] %d (%d, %d)\n", rf.me, oldApplied, rf.LastApplied)
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
	rf.Role = "follower"
	rf.CurrentTerm = 1
	rf.VotedFor = -1
	rf.electionCh = make(chan bool)
	rf.heartbeatCh = make(chan bool)
	rf.CommitIndex = 0
	rf.LastApplied = 0
	rf.VoteCount = 0
	rf.Log = []Log{Log{Term: 0}}
	rf.MatchIndex = []int{}
	rf.NextIndex = []int{}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	if rf.Role == "leader" {
		for i := 0; i < len(rf.peers); i++ {
			rf.NextIndex = append(rf.NextIndex, len(rf.Log))
			rf.MatchIndex = append(rf.MatchIndex, 0)
		}
	}
	//DPrintf("[start] %d(%d)\n", rf.me, len(rf.Log))
	go rf.StateManager()
	go rf.LogManager(applyCh)

	return rf
}
