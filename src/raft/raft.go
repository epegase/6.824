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
	"fmt"
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

// Raft log entry
type LogEntry struct {
	Index   int         // log index
	Term    int         // term when entry was received by leader
	Command interface{} // client command carried by this log entry
}

// Stringer
func (e LogEntry) String() string {
	commandStr := fmt.Sprintf("%v", e.Command)
	if len(commandStr) > 10 {
		commandStr = commandStr[:10]
	}
	return fmt.Sprintf("{I:%d T:%d C:%s}", e.Index, e.Term, commandStr)
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	applyCh   chan<- ApplyMsg     // channel to apply committed message to upper service

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// persistent state on all servers, updated on stable storage before responding to RPCs

	currentTerm int        // latest term server has seen
	votedFor    int        // candidateId that received vote in current term (or null if none)
	log         []LogEntry // log entries, has at least ONE log

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
	applyCh chan<- ApplyMsg,
) *Raft {
	// Your initialization code here (2A, 2B, 2C).
	rf := &Raft{
		peers:     peers,
		persister: persister,
		me:        me,
		dead:      0,
		applyCh:   applyCh,

		currentTerm: 0,                               // initialized to 0 on first boot, increases monotonically
		votedFor:    voteForNull,                     // null on startup
		log:         []LogEntry{{Index: 0, Term: 0}}, // a placeholder log to make life easier

		commitIndex:   0, // initialized to 0, increases monotonically
		lastApplied:   0, // initialized to 0, increases monotonically
		state:         Follower,
		electionAlarm: initElectionAlarm(),

		nextIndex:  nil, // nil for follower
		matchIndex: nil, // nil for follower
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

// step down as a follower, with mutex held
func (rf *Raft) stepDown(term int) {
	rf.state = Follower
	rf.currentTerm = term
	rf.votedFor = voteForNull
	rf.electionAlarm = nextElectionAlarm()
	rf.nextIndex = nil
	rf.matchIndex = nil
}

// candidate wins election, comes to power, with mutex held
func (rf *Raft) winElection() {
	rf.state = Leader

	// Reinitialized after election

	// initialized to leader last log index + 1
	lastLogIndex := rf.log[len(rf.log)-1].Index
	rf.nextIndex = make([]int, len(rf.peers))
	for i := range rf.nextIndex {
		rf.nextIndex[i] = lastLogIndex + 1
	}

	// initialized to 0
	rf.matchIndex = make([]int, len(rf.peers))
	for i := range rf.matchIndex {
		rf.matchIndex[i] = 0
	}
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

// restore previously persisted state.
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

// A service wants to switch to snapshot. Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
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

/********************** RequestVote RPC handler *******************************/
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if args.Term < rf.currentTerm || args.CandidateId == rf.me {
		// Respond to RPCs from candidates and leaders
		// Reply false if term < currentTerm
		// Followers only respond to requests from other servers
		return
	}

	if args.Term > rf.currentTerm {
		// If RPC request contains term T > currentTerm: set currentTerm = T, convert to follower
		Debug(rf, dTerm, "C%d RV request term is higher(%d > %d), following", args.CandidateId, args.Term, rf.currentTerm)
		rf.stepDown(args.Term)
	}

	Debug(rf, dVote, "C%d asking for Vote, T%d", args.CandidateId, args.Term)
	if rf.votedFor == voteForNull || rf.votedFor == args.CandidateId {
		// If votedFor is nul or candidateId,
		// and candidate's log is at least as up-to-date as receiver's log,
		// grant vote
		Debug(rf, dVote, "Granting Vote to C%d at T%d", args.CandidateId, args.Term)
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
	}
}

/********************** RequestVote RPC caller ********************************/

// make RequestVote RPC args, with mutex held
func (rf *Raft) constructRequestVoteArgs() *RequestVoteArgs {
	return &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.log[len(rf.log)-1].Index,
		LastLogTerm:  rf.log[len(rf.log)-1].Term,
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
	Debug(rf, dVote, "T%d -> S%d Sending RV, LLI:%d LLT:%d", args.Term, server, args.LastLogIndex, args.LastLogTerm)
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
				// If RPC response contains term T > currentTerm: set currentTerm = T, convert to follower
				Debug(rf, dTerm, "RV <- S%d Term is higher(%d > %d), following", server, reply.Term, rf.currentTerm)
				rf.stepDown(reply.Term)
			} else {
				granted = reply.VoteGranted
			}
			Debug(rf, dVote, "<- S%d Got Vote: %t, at T%d", server, granted, args.Term)
		}
		rf.mu.Unlock()
	}

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
				rf.winElection()
				rf.mu.Unlock()
				go rf.pacemaker(term)
			}
		}
	}
}

// The ticker go routine starts a new election if this peer hasn't received heartbeats recently.
func (rf *Raft) ticker() {
	var sleepDuration time.Duration
	for !rf.killed() {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		rf.mu.Lock()
		if rf.state == Leader {
			// Leader resets its own election timeout alarm each time
			rf.electionAlarm = nextElectionAlarm()
			sleepDuration = time.Until(rf.electionAlarm)
			rf.mu.Unlock()
		} else {
			Debug(rf, dTimer, "Not Leader, checking election timeout")
			if rf.electionAlarm.After(time.Now()) {
				// Not reach election timeout, going to sleep
				sleepDuration = time.Until(rf.electionAlarm)
				rf.mu.Unlock()
			} else {
				// If election timeout elapses without
				//  receiving AppendEntries RPC from current leader
				//  or granting vote to candidate:
				// convert to candidate, start new election

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
				sleepDuration = time.Until(rf.electionAlarm)

				args := rf.constructRequestVoteArgs()
				rf.mu.Unlock()

				grant := make(chan bool)
				// - Send RequestVote RPCs to all other servers
				for i := range rf.peers {
					if i != rf.me {
						go rf.requestVote(i, args, grant)
					}
				}

				// Candidate collects votes for current term
				go rf.collectVote(args.Term, grant)
			}
		}

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

/********************** AppendEntries RPC handler *****************************/

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.Success = false

	if args.Term < rf.currentTerm || args.LeaderId == rf.me {
		// Respond to RPCs from candidates and leaders
		// Followers only respond to requests from other servers
		// Reply false if term < currentTerm
		return
	}

	if len(rf.log) <= args.PrevLogIndex || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		// Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
		return
	}

	// true if follower contained entry matching prevLogIndex and prevLogTerm
	reply.Success = true

	if args.Term > rf.currentTerm {
		// If RPC request contains term T > currentTerm: set currentTerm = T, convert to follower
		Debug(rf, dTerm, "S%d %s request term is higher(%d > %d), following", args.LeaderId, intentOfAppendEntriesRPC(args), args.Term, rf.currentTerm)
		rf.stepDown(args.Term)
	}

	if rf.state == Candidate && args.Term >= rf.currentTerm {
		// While waiting for votes, a candidate may receive an AppendEntries RPC from another server claiming to be leader.
		// If the leader's term is at least as large as the candidate's current term,
		// then the candidate recognizes the leader as legitimate and returns to follower state
		Debug(rf, dTerm, "I'm Candidate, S%d %s request term %d >= %d, following", args.LeaderId, intentOfAppendEntriesRPC(args), args.Term, rf.currentTerm)
		rf.stepDown(args.Term)
	}

	Debug(rf, dTimer, "Resetting ELT, received %s T%d", intentOfAppendEntriesRPC(args), args.Term)
	rf.electionAlarm = nextElectionAlarm()

	if len(args.Entries) > 0 {
		// If an existing entry conflicts with a new one (same index but different terms),
		// delete the existing entry and all that follow it
		existingEntries := rf.log[args.PrevLogIndex+1:]
		var i int
		for i = 0; i < min(len(existingEntries), len(args.Entries)); i++ {
			if existingEntries[i].Term != args.Entries[i].Term {
				break
			}
		}
		// Append any new entries not already in the log
		rf.log = append(rf.log[:args.PrevLogIndex+1+i], args.Entries[i:]...)
	}

	if args.LeaderCommit > rf.commitIndex {
		// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
		rf.commitIndex = min(args.LeaderCommit, rf.log[len(rf.log)-1].Index)
		// going to commit
		go rf.commit()
	}
}

/********************** AppendEntries RPC caller ******************************/

// make AppendEntries RPC args, with mutex held
func (rf *Raft) constructAppendEntriesArgs(server int) *AppendEntriesArgs {
	prevLogIndex := rf.nextIndex[server] - 1
	prevLogTerm := rf.log[prevLogIndex].Term

	var entries []LogEntry
	// If last log index ≥ nextIndex for a follower:
	// send AppendEntries RPC with log entries starting at nextIndex
	if rf.log[len(rf.log)-1].Index >= rf.nextIndex[server] {
		entries = rf.log[rf.nextIndex[server]:]
	}

	return &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}
}

// send a AppendEntries RPC to a server
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	Debug(rf, dLog, "T%d -> S%d Sending %s, PLI:%d PLT:%d LC:%d - %v", args.Term, server, intentOfAppendEntriesRPC(args), args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit, args.Entries)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// The appendEntries go routine sends AppendEntries RPC to one peer and deal with reply
func (rf *Raft) appendEntries(server int, term int) {
	rf.mu.Lock()
	if rf.state != Leader || rf.currentTerm != term {
		rf.mu.Unlock()
		return
	}
	args := rf.constructAppendEntriesArgs(server)
	rf.mu.Unlock()

	reply := &AppendEntriesReply{}
	r := rf.sendAppendEntries(server, args, reply)

	if r {
		Debug(rf, dLog, "%s <- S%d Reply: %+v", intentOfAppendEntriesRPC(args), server, *reply)
		rf.mu.Lock()
		if reply.Term > rf.currentTerm {
			// If RPC response contains term T > currentTerm: set currentTerm = T, convert to follower
			Debug(rf, dTerm, "%s <- S%d Term is higher(%d > %d), following", intentOfAppendEntriesRPC(args), server, reply.Term, rf.currentTerm)
			rf.stepDown(reply.Term)
		}

		if l := len(args.Entries); l > 0 {
			if reply.Success {
				// If successful: update nextIndex and matchIndex for follower
				rf.nextIndex[server] = args.Entries[l-1].Index + 1
				rf.matchIndex[server] = args.Entries[l-1].Index
				go rf.checkCommit(term)
			} else {
				// If AppendEntries fails because of log inconsistency:
				// decrement nextIndex and retry
				rf.nextIndex[server]--
				go rf.appendEntries(server, term)
			}
		}
		rf.mu.Unlock()
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
	rf.mu.Lock()
	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}

	index = rf.log[len(rf.log)-1].Index + 1
	term = rf.currentTerm
	isLeader = true

	// If command received from client: append entry to local log
	rf.log = append(rf.log, LogEntry{
		Index:   index,
		Term:    term,
		Command: command,
	})
	Debug(rf, dLog2, "Received log: %v, with NI:%v, MI:%v", rf.log[len(rf.log)-1], rf.nextIndex, rf.matchIndex)

	rf.mu.Unlock()

	// Send AppendEntries RPCs to all followers, ask for agreement
	for i := range rf.peers {
		if i != rf.me {
			go rf.appendEntries(i, term)
		}
	}

	return
}

// The pacemaker go routine broadcasts periodic heartbeat to all followers
// Upon election: send initial empty AppendEntries RPCs (heartbeat) to each server
// repeat during idle periods to prevent election timeouts
func (rf *Raft) pacemaker(term int) {
	// only Leader can start pacemaker
	// Leader's pacemaker only last for its current term
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
			// end of pacemaker for this term
			return
		}
		rf.mu.Unlock()

		// send heartbeat to each server
		Debug(rf, dTimer, "Leader at T%d, broadcasting heartbeats", term)
		for i := range rf.peers {
			if i != rf.me {
				go rf.appendEntries(i, term)
			}
		}

		// repeat during idle periods to prevent election timeout
		time.Sleep(heartbeatInterval * time.Millisecond)
	}
}

// The checkCommit go routine check if any log entry commit-able,
// and if so, going to commit (in a separate goroutine)
func (rf *Raft) checkCommit(term int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// re-check state and term
	if rf.state != Leader || rf.currentTerm != term {
		return
	}

	Debug(rf, dCommit, "L%d at T%d try to commit, with CI:%d, LL:%d", rf.me, term, rf.commitIndex, len(rf.log))
	// If there exists an N such that
	// - N > commitIndex
	// - a majority of matchIndex[i] ≥ N
	// - log[N].term == currentTerm:
	// set commitIndex = N
	for n := rf.commitIndex + 1; n < len(rf.log); n++ {
		if rf.log[n].Term == term {
			cnt := 1
			for i := range rf.peers {
				if i != rf.me && rf.matchIndex[i] >= n {
					cnt++
				}
			}
			if cnt >= len(rf.peers)/2+1 {
				Debug(rf, dCommit, "Commit achieved majority, set CI from %d to %d", rf.commitIndex, n)
				rf.commitIndex = n
			}
		}
	}

	// going to commit
	go rf.commit()
}

// The commit go routine compare commitIndex with lastApplied,
// increment lastApplied if commit-safe
// and if any log applied, send to client through applyCh
func (rf *Raft) commit() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.commitIndex > rf.lastApplied {
		// If commitIndex > lastApplied:
		// increment lastApplied,
		// apply log[lastApplied] to state machine
		rf.lastApplied++
		logEntry := rf.log[rf.lastApplied]
		Debug(rf, dCommit, "CI:%d > LA:%d, apply log: %s", rf.commitIndex, rf.lastApplied-1, logEntry)
		rf.applyCh <- ApplyMsg{
			CommandValid: true,
			Command:      logEntry.Command,
			CommandIndex: logEntry.Index,
		}
	}
}
