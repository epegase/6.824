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
	"sync"
	"sync/atomic"
	"time"

	"6.824/labrpc"
)

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

//
// Raft log entry
//
type LogEntry struct {
	Index int // log index
	Term  int // term when entry was received by leader
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
	applyCh   chan ApplyMsg       // channel to apply committed message to upper service

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// persistent state on all servers, updated on stable storage before responding to RPCs
	currentTerm int        // latest term server has seen
	votedFor    int        // candidateId that received vote in current term (or null if none)
	log         []LogEntry // log entries

	// volatile state on all servers
	commitIndex   int         // index of highest log entry known to be committed
	lastApplied   int         // index of highest log entry applied to state machine
	state         serverState // current server state (each time, server start as a Follower)
	electionAlarm time.Time   // election timeout alarm, if time passed by alarm, server start new election

	// volatile state on leaders, reinitialized after election
	nextIndex  []int // for each server, index of the next log entry to send to that server
	matchIndex []int // for each server, index of hightest log entry known to be replicated on server

	// TODO: misc
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
func Make(
	peers []*labrpc.ClientEnd,
	me int,
	persister *Persister,
	applyCh chan ApplyMsg,
) *Raft {
	// Your initialization code here (2A, 2B, 2C).
	rf := &Raft{
		peers:     peers,
		persister: persister,
		me:        me,
		dead:      0,
		applyCh:   applyCh,

		currentTerm: 0,           // initialized to 0 on first boot, increases monotonically
		votedFor:    voteForNull, // null on startup
		log:         make([]LogEntry, 0),

		commitIndex:   0, // initialized to 0, increases monotonically
		lastApplied:   0, // initialized to 0, increases monotonically
		state:         Follower,
		electionAlarm: nextElectionAlarm(),

		nextIndex:  make([]int, 0),
		matchIndex: make([]int, 0),
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	lastLogIndex := 0
	if l := len(rf.log); l > 0 {
		lastLogIndex = rf.log[l-1].Index
	}
	Debug(rf, dClient, "Started at T:%d LLI:%d", rf.currentTerm, lastLogIndex)

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

// return currentTerm and whether this server believes it is the leader.
func (rf *Raft) GetState() (term int, isLeader bool) {
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == Leader
}

/************************* Helper *********************************************/

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

// step down as a follower,
// with mutex held
func (rf *Raft) stepDown(term int) {
	rf.state = Follower
	rf.currentTerm = term
	rf.votedFor = voteForNull
	rf.electionAlarm = nextElectionAlarm()
}

/************************* Persist *******************************************/

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

/************************* RequestVote ****************************************/

type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // candidate's term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidate's last log entry
}

type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

// make RequestVote RPC args, with mutex held
func (rf *Raft) constructRequestVoteArgs() *RequestVoteArgs {
	lastLogIndex := 0
	lastLogTerm := 0
	if l := len(rf.log); l > 0 {
		lastLogIndex = rf.log[l-1].Index
		lastLogTerm = rf.log[l-1].Term
	}
	return &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}
}

// RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if args.Term < rf.currentTerm {
		// Reply false if term < currentTerm
		return
	}

	if args.Term > rf.currentTerm {
		// If RPC request contains term T > currentTerm: set currentTerm = T, convert to follower
		Debug(rf, dTerm, "RV request term is higher(%d > %d), following", args.Term, rf.currentTerm)
		rf.stepDown(args.Term)
	}

	Debug(rf, dVote, "C%d asking for Vote, T%d", args.CandidateId, args.Term)
	if rf.votedFor == voteForNull || rf.votedFor == args.CandidateId {
		// If votedFor is nul or candidateId,
		// and candidate's log is at least as up-to-date as receiver's log,
		// grant vote
		Debug(rf, dVote, "Granting Vote to S%d at T%d", args.CandidateId, args.Term)
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
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
	Debug(rf, dLog, "T%d -> S%d Sending RV, LLI:%d LLT:%d", args.Term, server, args.LastLogIndex, args.LastLogTerm)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// The requestVote go routine sends RequestVote RPC to one peer and deal with reply
func (rf *Raft) requestVote(server int, args *RequestVoteArgs, grant chan<- bool) {
	reply := &RequestVoteReply{}
	r := rf.sendRequestVote(server, args, reply)

	granted := false
	if r {
		rf.mu.Lock()
		// re-check all relevant assumptions
		// - still a candidate
		// - rf.currentTerm hasn't changed since the decision to become a candidate
		if rf.state == Candidate && args.Term == rf.currentTerm {
			if reply.Term > rf.currentTerm {
				Debug(rf, dTerm, "RV <- S%d Term is higher(%d > %d), following", server, reply.Term, rf.currentTerm)
				rf.stepDown(reply.Term)
			} else {
				granted = reply.VoteGranted
			}
		}
		rf.mu.Unlock()
	}

	Debug(rf, dVote, "<- S%d Got Vote: %t, at T%d", server, granted, args.Term)
	grant <- granted
}

// The collectVote go routine collects votes from peers
func (rf *Raft) collectVote(term int, grant <-chan bool) {
	cnt := 1
	done := false
	for i := 0; i < len(rf.peers)-1; i++ {
		if <-grant {
			cnt++
		}
		// Votes received from majority of servers: become leader
		if !done && cnt >= len(rf.peers)/2+1 {
			done = true
			rf.mu.Lock()
			// re-check all relevant assumptions
			// - still a candidate
			// - rf.currentTerm hasn't changed since the decision to become a candidate
			if rf.state != Candidate || rf.currentTerm != term {
				rf.mu.Unlock()
			} else {
				Debug(rf, dLeader, "Achieved Majority for T%d (%d), converting to Leader", rf.currentTerm, cnt)
				rf.state = Leader
				term := rf.currentTerm
				rf.mu.Unlock()
				go rf.pacemaker(term)
			}
		}
	}
}

// The ticker go routine starts a new election if this peer hasn't received heartbeats recently.
func (rf *Raft) ticker() {
	rf.mu.Lock()
	if rf.state == Leader {
		s := "A Leader, but start ticker"
		Debug(rf, dError, s)
		panic(s)
	}
	rf.mu.Unlock()

	for !rf.killed() {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		rf.mu.Lock()
		if rf.state != Leader {
			Debug(rf, dTimer, "Not Leader, checking election timeout")
			if rf.electionAlarm.After(time.Now()) {
				rf.mu.Unlock()
			} else {
				// On conversion to candidate, start election:
				// - Increment currentTerm
				rf.currentTerm++
				Debug(rf, dTerm, "Converting to Candidate, calling election T:%d", rf.currentTerm)
				// - Change to Candidate
				rf.state = Candidate
				// - Vote for self
				rf.votedFor = rf.me

				// - Reset election timer
				Debug(rf, dTimer, "Resetting ELT because election")
				rf.electionAlarm = nextElectionAlarm()

				args := rf.constructRequestVoteArgs()
				rf.mu.Unlock()

				grant := make(chan bool)
				// - Send RequestVote RPCs to all other servers
				for i := range rf.peers {
					if i != rf.me {
						go rf.requestVote(i, args, grant)
					}
				}

				go rf.collectVote(args.Term, grant)
			}
		} else {
			rf.mu.Unlock()
		}

		rf.mu.Lock()
		if rf.state == Leader {
			// Leader reset its own election timeout alarm each time
			rf.electionAlarm = nextElectionAlarm()
		}
		sleepDuration := time.Until(rf.electionAlarm)
		rf.mu.Unlock()

		Debug(rf, dTimer, "Ticker going to sleep for %d ms", sleepDuration.Milliseconds())
		time.Sleep(sleepDuration)
	}
}

/************************* Log ************************************************/

type AppendEntriesArgs struct {
	Term         int        // leader's term
	LeaderId     int        // so follower can redirect clients
	PrevLogIndex int        // index of log entry immediately preceding new ones
	PrevLogTerm  int        // term of prevLogIndex entry
	Entries      []LogEntry // log entries to store (empty for heartbeat)
	LeaderCommit int        // leader's commitIndex
}

type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
}

// make AppendEntries RPC args, with mutex held
func (rf *Raft) constructAppendEntriesArgs() *AppendEntriesArgs {
	return &AppendEntriesArgs{
		Term:     rf.currentTerm,
		LeaderId: rf.me,
	}
}

// AppendEntries RPC handler.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.Success = false

	if args.Term < rf.currentTerm {
		// Reply false if term < currentTerm
		return
	}

	if args.Term > rf.currentTerm {
		// If RPC request contains term T > currentTerm: set currentTerm = T, convert to follower
		Debug(rf, dTerm, "%s request term is higher(%d > %d), following", intentOfAppendEntriesRPC(args), args.Term, rf.currentTerm)
		rf.stepDown(args.Term)
	}

	Debug(rf, dTimer, "Resetting ELT, received %s T%d", intentOfAppendEntriesRPC(args), args.Term)
	rf.electionAlarm = nextElectionAlarm()
}

// send a AppendEntries RPC to a server
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	Debug(rf, dLog, "T%d -> S%d Sending %s, PLI:%d PLT:%d LC:%d", args.Term, server, intentOfAppendEntriesRPC(args), args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

/************************* Heartbeat ******************************************/

// The heartBeat go routine sends AppendEntries RPC (act as HeartBeat RPC) to one peer and deal with reply
func (rf *Raft) heartBeat(server int, args *AppendEntriesArgs) {
	reply := &AppendEntriesReply{}
	r := rf.sendAppendEntries(server, args, reply)

	if r {
		rf.mu.Lock()
		if reply.Term > rf.currentTerm {
			Debug(rf, dTerm, "HB <- S%d Term is higher(%d > %d), following", server, reply.Term, rf.currentTerm)
			rf.stepDown(reply.Term)
		}
		rf.mu.Unlock()
	}
}

// The pacemaker go routine broadcasts periodic heartbeat to all followers
func (rf *Raft) pacemaker(term int) {
	rf.mu.Lock()
	if rf.state != Leader {
		s := "Not a Leader, but start heartbeat"
		Debug(rf, dError, s)
		panic(s)
	}
	rf.mu.Unlock()

	for !rf.killed() {
		rf.mu.Lock()
		// re-check state and term each time, before broadcast
		if rf.state != Leader || rf.currentTerm != term {
			rf.mu.Unlock()
			// end of broadcast for this term
			return
		}

		args := rf.constructAppendEntriesArgs()
		rf.mu.Unlock()

		Debug(rf, dTimer, "Leader at T%d, broadcasting heartbeats", rf.currentTerm)
		for i := range rf.peers {
			if i != rf.me {
				go rf.heartBeat(i, args)
			}
		}

		time.Sleep(heartbeatInterval * time.Millisecond)
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
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (index int, term int, isLeader bool) {
	// Your code here (2B).

	return
}
