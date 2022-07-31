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
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/lablog"
	"6.824/labrpc"
	"6.824/labutil"
)

const (
	// special NULL value for voteFor
	voteForNull = -1
	// election timeout range, in millisecond
	electionTimeoutMax = 1200
	electionTimeoutMin = 800
	// heartbeat interval, in millisecond (10 heartbeat RPCs per second)
	// heartbeat interval should be one order less than election timeout
	heartbeatInterval = 100
	// leader will keep a small amount of trailing logs not compacted when snapshot
	// to deal with slightly lagging follower
	leaderKeepLogAmount = 20
)

type serverState string

const (
	Leader    serverState = "L"
	Candidate serverState = "C"
	Follower  serverState = "F"
)

// set normal election timeout, with randomness
func nextElectionAlarm() time.Time {
	return time.Now().Add(time.Duration(labutil.RandRange(electionTimeoutMin, electionTimeoutMax)) * time.Millisecond)
}

// set fast election timeout on startup, with randomness
func initElectionAlarm() time.Time {
	return time.Now().Add(time.Duration(labutil.RandRange(0, electionTimeoutMax-electionTimeoutMin)) * time.Millisecond)
}

// actual intention name of AppendEntries RPC call
func intentOfAppendEntriesRPC(args *AppendEntriesArgs) string {
	if len(args.Entries) == 0 {
		return "HB"
	}
	return "AE"
}

func dTopicOfAppendEntriesRPC(args *AppendEntriesArgs, defaultTopic lablog.LogTopic) lablog.LogTopic {
	if len(args.Entries) == 0 {
		return lablog.Heart
	}
	return defaultTopic
}

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
	if len(commandStr) > 15 {
		commandStr = commandStr[:15]
	}
	return fmt.Sprintf("{I:%d T:%d C:%s}", e.Index, e.Term, commandStr)
}

// snapshot command
// received by Snapshot func, send through channel to snapshoter
type snapshotCmd struct {
	index    int
	snapshot []byte
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// if send true, tell committer to advance commit and apply msg to client
	// if send false, tell committer to send received snapshot back to service
	commitTrigger   chan bool
	snapshotCh      chan snapshotCmd // tell snapshoter to take a snapshot
	snapshotTrigger chan bool        // tell snapshoter to save delayed snapshot

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// persistent state on all servers, updated on stable storage before responding to RPCs

	CurrentTerm       int        // latest term server has seen, increases monotonically
	VotedFor          int        // candidateId that received vote in current term (or null if none)
	Log               []LogEntry // log entries
	LastIncludedIndex int        // index of the log entry that before the start of log, increases monotonically
	LastIncludedTerm  int        // term of the log entry that before the start of log

	// volatile state on all servers

	commitIndex   int         // index of highest log entry known to be committed, increases monotonically
	lastApplied   int         // index of highest log entry applied to state machine, increases monotonically
	state         serverState // current server state (each time, server start as a Follower)
	electionAlarm time.Time   // election timeout alarm, if time passed by alarm, server start new election

	// volatile state on leaders, reinitialized after election

	nextIndex         []int      // for each server, index of the next log entry to send to that server
	matchIndex        []int      // for each server, index of hightest log entry known to be replicated on server
	appendEntriesCh   []chan int // for each follower, channel to trigger AppendEntries RPC call (send serialNo if retry, otherwise 0)
	installSnapshotCh []chan int // for each follower, channel to trigger InstallSnapshot RPC call (send serialNo if retry, otherwise 0)
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

		// IMPORTANT: buffered channel
		// (unbuffered channel may cause trigger message lost when under high concurrency pressure)
		commitTrigger: make(chan bool, 1),

		// IMPORTANT: buffered channel
		// (unbuffered channel may cause trigger message lost when under high concurrency pressure)
		snapshotTrigger: make(chan bool, 1),
		snapshotCh:      make(chan snapshotCmd),

		CurrentTerm:       0,           // initialized to 0 on first boot
		VotedFor:          voteForNull, // null on startup
		Log:               []LogEntry{},
		LastIncludedIndex: 0, // initialized to 0
		LastIncludedTerm:  0, // initialized to 0

		commitIndex:   0, // initialized to 0
		lastApplied:   0, // initialized to 0
		state:         Follower,
		electionAlarm: initElectionAlarm(),

		nextIndex:         nil, // nil for follower
		matchIndex:        nil, // nil for follower
		appendEntriesCh:   nil, // nil for follower
		installSnapshotCh: nil, // nil for follower
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// set commitIndex and lastApplied to snapshot's LastIncludedIndex
	rf.commitIndex = rf.LastIncludedIndex
	rf.lastApplied = rf.LastIncludedIndex

	lastLogIndex, lastLogTerm := rf.lastLogIndexAndTerm()
	lablog.Debug(rf.me, lablog.Client, "Started at T:%d with (LII:%d LIT:%d), (LLI:%d LLT:%d)", rf.CurrentTerm, rf.LastIncludedIndex, rf.LastIncludedTerm, lastLogIndex, lastLogTerm)

	// start ticker goroutine to start elections
	go rf.ticker()

	// start committer goroutine to wait for commit trigger signal, advance lastApplied and apply msg to client
	go rf.committer(applyCh, rf.commitTrigger)

	// start snapshoter goroutine to wait for snapshot command that will sent through snapshotCh
	// if is leader, maybe keep a small amount of trailing log entries for lagging followers to catch up
	// so leader's snapshot will be delayed and be triggered by snapshotTrigger each time leader's nextIndex updated
	go rf.snapshoter(rf.snapshotTrigger)

	return rf
}

// return CurrentTerm and whether this server believes it is the leader.
func (rf *Raft) GetState() (term int, isLeader bool) {
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.CurrentTerm, rf.state == Leader
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// to terminate all long-run goroutines
	// quit entriesAppender
	for _, c := range rf.appendEntriesCh {
		if c != nil {
			close(c)
		}
	}
	// IMPORTANT: not just close channels, but also need to reset appendEntriesCh to avoid send to closed channel
	rf.appendEntriesCh = nil

	// quit snapshotInstaller
	for _, c := range rf.installSnapshotCh {
		if c != nil {
			close(c)
		}
	}
	// IMPORTANT: not just close channels, but also need to reset installSnapshotCh to avoid send to closed channel
	rf.installSnapshotCh = nil

	// quit committer
	if rf.commitTrigger != nil {
		close(rf.commitTrigger)
	}
	// IMPORTANT: not just close channels, but also need to reset commitTrigger to avoid send to closed channel
	rf.commitTrigger = nil

	// quit snapshoter
	close(rf.snapshotTrigger)
	// IMPORTANT: not just close channels, but also need to reset snapshotTrigger to avoid send to closed channel
	rf.snapshotTrigger = nil
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// step down as a follower, with mutex held
func (rf *Raft) stepDown(term int) {
	rf.state = Follower
	rf.CurrentTerm = term
	rf.VotedFor = voteForNull
	rf.nextIndex = nil
	rf.matchIndex = nil
	// close all channels and reset
	for _, c := range rf.appendEntriesCh {
		if c != nil {
			close(c)
		}
	}
	rf.appendEntriesCh = nil
	for _, c := range rf.installSnapshotCh {
		if c != nil {
			close(c)
		}
	}
	rf.installSnapshotCh = nil

	rf.persist()

	// once step down, trigger any delayed snapshot
	select {
	case rf.snapshotTrigger <- true:
	default:
	}
}

// index and term of last log entry, with mutex held
func (rf *Raft) lastLogIndexAndTerm() (index, term int) {
	index, term = rf.LastIncludedIndex, rf.LastIncludedTerm
	if l := len(rf.Log); l > 0 {
		index, term = rf.Log[l-1].Index, rf.Log[l-1].Term
	}
	return
}

/************************* Election (2A) **************************************/

// is my log more up-to-date compared to other's, with mutex held
// Raft determines which of two logs is more up-to-date
// by comparing the index and term of the last entries in the logs
func (rf *Raft) isMyLogMoreUpToDate(index int, term int) bool {
	// If the logs have last entries with different terms,
	// then the log with the later term is more up-to-date
	myLastLogIndex, myLastLogTerm := rf.lastLogIndexAndTerm()
	if myLastLogTerm != term {
		return myLastLogTerm > term
	}

	// If the logs end with the same term,
	// then whichever log is longer is more up-to-date
	return myLastLogIndex > index
}

// candidate wins election, comes to power, with mutex held
func (rf *Raft) winElection() {
	rf.state = Leader

	// Reinitialized after election

	// initialized to leader last log index + 1
	lastLogIndex, _ := rf.lastLogIndexAndTerm()
	rf.nextIndex = make([]int, len(rf.peers))
	for i := range rf.nextIndex {
		rf.nextIndex[i] = lastLogIndex + 1
	}

	// initialized to 0
	rf.matchIndex = make([]int, len(rf.peers))
	for i := range rf.matchIndex {
		rf.matchIndex[i] = 0
	}
	// but i know my own highest log entry
	rf.matchIndex[rf.me] = lastLogIndex

	// start entriesAppender goroutine for each follower, in this term
	rf.appendEntriesCh = make([]chan int, len(rf.peers))
	for i := range rf.appendEntriesCh {
		if i != rf.me {
			rf.appendEntriesCh[i] = make(chan int, 1)
			go rf.entriesAppender(i, rf.appendEntriesCh[i], rf.CurrentTerm)
		}
	}

	// start snapshotInstaller goroutine for each follower, in this term
	rf.installSnapshotCh = make([]chan int, len(rf.peers))
	for i := range rf.installSnapshotCh {
		if i != rf.me {
			rf.installSnapshotCh[i] = make(chan int, 1)
			go rf.snapshotInstaller(i, rf.installSnapshotCh[i], rf.CurrentTerm)
		}
	}

	// start pacemaker to send heartbeat to each follower periodically, in this term
	go rf.pacemaker(rf.CurrentTerm)
}

type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // candidate's term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidate's last log entry
}

type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // CurrentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

/********************** RequestVote RPC handler *******************************/

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.CurrentTerm
	reply.VoteGranted = false

	if args.Term < rf.CurrentTerm || args.CandidateId == rf.me || rf.killed() {
		// Respond to RPCs from candidates and leaders
		// Reply false if term < CurrentTerm
		// Followers only respond to requests from other servers
		return
	}

	if args.Term > rf.CurrentTerm {
		// If RPC request contains term T > CurrentTerm: set CurrentTerm = T, convert to follower
		lablog.Debug(rf.me, lablog.Term, "C%d RV request term is higher(%d > %d), following", args.CandidateId, args.Term, rf.CurrentTerm)
		rf.stepDown(args.Term)
	}

	lablog.Debug(rf.me, lablog.Vote, "C%d asking for Vote, T%d", args.CandidateId, args.Term)
	if (rf.VotedFor == voteForNull || rf.VotedFor == args.CandidateId) &&
		!rf.isMyLogMoreUpToDate(args.LastLogIndex, args.LastLogTerm) {
		// If VotedFor is nul or candidateId,
		// and candidate's log is at least as up-to-date as receiver's log,
		// grant vote
		lablog.Debug(rf.me, lablog.Vote, "Granting Vote to C%d at T%d", args.CandidateId, args.Term)
		reply.VoteGranted = true
		rf.VotedFor = args.CandidateId

		// IMPORTANT:
		// Adopt advice from #Livelocks part of [students-guide-to-raft](https://thesquareplanet.com/blog/students-guide-to-raft/#livelocks)
		// Quote:
		//   restart your election timer if a)...; b)...; c) you grant a vote to another peer
		rf.electionAlarm = nextElectionAlarm()

		rf.persist()
	}
}

/********************** RequestVote RPC caller ********************************/

// make RequestVote RPC args, with mutex held
func (rf *Raft) constructRequestVoteArgs() *RequestVoteArgs {
	lastLogIndex, lastLogTerm := rf.lastLogIndexAndTerm()
	return &RequestVoteArgs{
		Term:         rf.CurrentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
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
	lablog.Debug(rf.me, lablog.Vote, "T%d -> S%d Sending RV, LLI:%d LLT:%d", args.Term, server, args.LastLogIndex, args.LastLogTerm)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// The requestVote go routine sends RequestVote RPC to one peer and deal with reply
func (rf *Raft) requestVote(server int, term int, args *RequestVoteArgs, grant chan<- bool) {
	granted := false
	defer func() { grant <- granted }()

	rf.mu.Lock()
	if rf.state != Candidate || rf.CurrentTerm != term || rf.killed() {
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()

	reply := &RequestVoteReply{}
	r := rf.sendRequestVote(server, args, reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// re-check all relevant assumptions
	// - still a candidate
	// - instance not killed
	if rf.state != Candidate || rf.killed() {
		return
	}

	if !r {
		lablog.Debug(rf.me, lablog.Drop, "-> S%d RV been dropped: {T:%d LLI:%d LLT:%d}", server, args.Term, args.LastLogIndex, args.LastLogTerm)
		return
	}

	if reply.Term > rf.CurrentTerm {
		// If RPC response contains term T > CurrentTerm: set CurrentTerm = T, convert to follower
		lablog.Debug(rf.me, lablog.Term, "RV <- S%d Term is higher(%d > %d), following", server, reply.Term, rf.CurrentTerm)
		rf.stepDown(reply.Term)
		return
	}

	// IMPORTANT:
	// Adopt advice from #Term-confusion part of [students-guide-to-raft](https://thesquareplanet.com/blog/students-guide-to-raft/#term-confusion)
	// Quote:
	//  From experience, we have found that by far the simplest thing to do is to
	//  first record the term in the reply (it may be higher than your current term),
	//  and then to compare the current term with the term you sent in your original RPC.
	//  If the two are different, drop the reply and return.
	//  Only if the two terms are the same should you continue processing the reply.
	if rf.CurrentTerm != term {
		return
	}

	granted = reply.VoteGranted
	lablog.Debug(rf.me, lablog.Vote, "<- S%d Got Vote: %t, at T%d", server, granted, term)
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
			// - rf.CurrentTerm hasn't changed since the decision to become a candidate
			// - install not killed
			if rf.state != Candidate || rf.CurrentTerm != term || rf.killed() {
				rf.mu.Unlock()
			} else {
				rf.winElection()
				lablog.Debug(rf.me, lablog.Leader, "Achieved Majority for T%d, converting to Leader, NI:%v, MI:%v", rf.CurrentTerm, rf.nextIndex, rf.matchIndex)
				rf.mu.Unlock()
			}
		}
	}
}

// The ticker go routine starts a new election if this peer hasn't received heartbeats recently.
func (rf *Raft) ticker() {
	var sleepDuration time.Duration
	// long-run singleton go routine, should check if killed
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
			lablog.Debug(rf.me, lablog.Timer, "Not Leader, checking election timeout")
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
				// - Increment CurrentTerm
				rf.CurrentTerm++
				term := rf.CurrentTerm
				lablog.Debug(rf.me, lablog.Term, "Converting to Candidate, calling election T:%d", term)
				// - Change to Candidate
				rf.state = Candidate
				// - Vote for self
				rf.VotedFor = rf.me

				rf.persist()

				// - Reset election timer
				lablog.Debug(rf.me, lablog.Timer, "Resetting ELT because election")
				rf.electionAlarm = nextElectionAlarm()
				sleepDuration = time.Until(rf.electionAlarm)

				args := rf.constructRequestVoteArgs()
				rf.mu.Unlock()

				grant := make(chan bool)
				// - Send RequestVote RPCs to all other servers
				for i := range rf.peers {
					if i != rf.me {
						go rf.requestVote(i, term, args, grant)
					}
				}

				// Candidate collects votes for current term
				go rf.collectVote(args.Term, grant)
			}
		}

		lablog.Debug(rf.me, lablog.Timer, "Ticker going to sleep for %d ms", sleepDuration.Milliseconds())
		time.Sleep(sleepDuration)
	}
}

/************************* Log (2B) *******************************************/

type AppendEntriesArgs struct {
	Term         int        // leader's term
	LeaderId     int        // so follower can redirect clients
	PrevLogIndex int        // index of log entry immediately preceding new ones
	PrevLogTerm  int        // term of prevLogIndex entry
	Entries      []LogEntry // log entries to store (empty for heartbeat)
	LeaderCommit int        // leader's commitIndex
}

type AppendEntriesReply struct {
	Term    int  // CurrentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm

	// OPTIMIZATION

	XTerm  int // term in the conflicting entry (if any)
	XIndex int // index of first entry with that XTerm term (if any)
	XLen   int // log length
}

/********************** AppendEntries RPC handler *****************************/

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.CurrentTerm
	reply.Success = false

	if args.Term < rf.CurrentTerm || args.LeaderId == rf.me || rf.killed() {
		// Respond to RPCs from candidates and leaders
		// Followers only respond to requests from other servers
		// Reply false if term < CurrentTerm
		return
	}

	if args.Term > rf.CurrentTerm {
		// If RPC request contains term T > CurrentTerm: set CurrentTerm = T, convert to follower
		lablog.Debug(rf.me, lablog.Term, "S%d %s request term is higher(%d > %d), following", args.LeaderId, intentOfAppendEntriesRPC(args), args.Term, rf.CurrentTerm)
		rf.stepDown(args.Term)
	}

	if rf.state == Candidate && args.Term >= rf.CurrentTerm {
		// While waiting for votes, a candidate may receive an AppendEntries RPC from another server claiming to be leader.
		// If the leader's term is at least as large as the candidate's current term,
		// then the candidate recognizes the leader as legitimate and returns to follower state
		lablog.Debug(rf.me, lablog.Term, "I'm Candidate, S%d %s request term %d >= %d, following", args.LeaderId, intentOfAppendEntriesRPC(args), args.Term, rf.CurrentTerm)
		rf.stepDown(args.Term)
	}

	lablog.Debug(rf.me, lablog.Timer, "Resetting ELT, received %s from L%d at T%d", intentOfAppendEntriesRPC(args), args.LeaderId, args.Term)
	rf.electionAlarm = nextElectionAlarm()

	// Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
	lastLogIndex, _ := rf.lastLogIndexAndTerm()
	if lastLogIndex < args.PrevLogIndex {
		// OPTIMIZATION: fast log roll back, follower's log is too short
		reply.XLen = lastLogIndex + 1
		return
	}

	// lastLogIndex >= args.PrevLogIndex, follower's log is long enough to cover args.PrevLogIndex
	var prevLogTerm int
	switch {
	case args.PrevLogIndex == rf.LastIncludedIndex:
		prevLogTerm = rf.LastIncludedTerm
	case args.PrevLogIndex < rf.LastIncludedIndex:
		// follower has committed PrevLogIndex log =>
		// PrevLogIndex consistency check is already done =>
		// it's OK to start log consistency check at rf.LastIncludedIndex
		args.PrevLogIndex = rf.LastIncludedIndex
		prevLogTerm = rf.LastIncludedTerm
		// trim args.Entries to start after LastIncludedIndex
		sameEntryInArgsEntries := false
		for i := range args.Entries {
			if args.Entries[i].Index == rf.LastIncludedIndex && args.Entries[i].Term == rf.LastIncludedTerm {
				sameEntryInArgsEntries = true
				args.Entries = args.Entries[i+1:]
				break
			}
		}
		if !sameEntryInArgsEntries {
			// not found LastIncludedIndex log entry in args.Entries =>
			// args.Entries are all covered by LastIncludedIndex =>
			// args.Entries are all committed =>
			// safe to trim args.Entries to empty slice
			args.Entries = make([]LogEntry, 0)
		}
	default:
		// args.PrevLogIndex > rf.LastIncludedIndex
		prevLogTerm = rf.Log[args.PrevLogIndex-rf.LastIncludedIndex-1].Term
	}

	if prevLogTerm != args.PrevLogTerm {
		// OPTIMIZATION: fast log roll back: set term of conflicting entry and index of first entry with that term
		reply.XTerm = prevLogTerm
		for i := args.PrevLogIndex - rf.LastIncludedIndex - 1; i > -1; i-- {
			reply.XIndex = rf.Log[i].Index
			if rf.Log[i].Term != prevLogTerm {
				break
			}
		}
		return
	}

	// true if follower contained entry matching prevLogIndex and prevLogTerm
	reply.Success = true

	if len(args.Entries) > 0 {
		// If an existing entry conflicts with a new one (same index but different terms),
		// delete the existing entry and all that follow it
		lablog.Debug(rf.me, lablog.Info, "Received: %v from S%d at T%d", args.Entries, args.LeaderId, args.Term)
		existingEntries := rf.Log[args.PrevLogIndex-rf.LastIncludedIndex:]
		var i int
		needPersist := false
		for i = 0; i < labutil.Min(len(existingEntries), len(args.Entries)); i++ {
			if existingEntries[i].Term != args.Entries[i].Term {
				lablog.Debug(rf.me, lablog.Info, "Discard conflicts: %v", rf.Log[args.PrevLogIndex-rf.LastIncludedIndex+i:])
				rf.Log = rf.Log[:args.PrevLogIndex-rf.LastIncludedIndex+i]
				needPersist = true
				break
			}
		}
		if i < len(args.Entries) {
			// Append any new entries not already in the log
			lablog.Debug(rf.me, lablog.Info, "Append new: %v from i: %d", args.Entries[i:], i)
			rf.Log = append(rf.Log, args.Entries[i:]...)
			needPersist = true
		}

		if needPersist {
			rf.persist()
		}
	}

	if args.LeaderCommit > rf.commitIndex {
		// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
		lastLogIndex, _ := rf.lastLogIndexAndTerm()
		rf.commitIndex = labutil.Min(args.LeaderCommit, lastLogIndex)
		// going to commit
		select {
		case rf.commitTrigger <- true:
		default:
		}
	}
}

/********************** AppendEntries RPC caller ******************************/

// make AppendEntries RPC args, with mutex held
// IMPORTANT: return nil to indicate installSnapshot
func (rf *Raft) constructAppendEntriesArgs(server int) *AppendEntriesArgs {
	prevLogIndex := rf.nextIndex[server] - 1
	prevLogTerm := rf.LastIncludedTerm
	if i := prevLogIndex - rf.LastIncludedIndex - 1; i > -1 {
		prevLogTerm = rf.Log[i].Term
	}

	var entries []LogEntry
	if lastLogIndex, _ := rf.lastLogIndexAndTerm(); lastLogIndex <= prevLogIndex {
		// this follower has all leader's log, no need to send log entries
		entries = nil
	} else if prevLogIndex >= rf.LastIncludedIndex {
		// send AppendEntries RPC with log entries starting at nextIndex
		newEntries := rf.Log[prevLogIndex-rf.LastIncludedIndex:]
		entries = make([]LogEntry, len(newEntries))
		// avoid data-race
		copy(entries, newEntries)
	} else {
		// IMPORTANT: leader's log too short, cannot appendEntries, need to installSnapshot
		return nil
	}

	return &AppendEntriesArgs{
		Term:         rf.CurrentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}
}

// send a AppendEntries RPC to a server
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	lablog.Debug(rf.me, dTopicOfAppendEntriesRPC(args, lablog.Info), "T%d -> S%d Sending %s, PLI:%d PLT:%d LC:%d - %v", args.Term, server, intentOfAppendEntriesRPC(args), args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit, args.Entries)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// The appendEntries go routine sends AppendEntries RPC to one peer and deal with reply
func (rf *Raft) appendEntries(server int, term int, args *AppendEntriesArgs, serialNo int) {
	reply := &AppendEntriesReply{}
	r := rf.sendAppendEntries(server, args, reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// re-check all relevant assumptions
	// - still a leader
	// - instance not killed
	if rf.state != Leader || rf.killed() {
		return
	}

	rpcIntent := intentOfAppendEntriesRPC(args)

	if !r {
		if rpcIntent == "HB" {
			// OPTIMIZATION: don't retry heartbeat rpc
			return
		}
		if args.PrevLogIndex < rf.nextIndex[server]-1 {
			// OPTIMIZATION: this AppendEntries RPC is out-of-data, don't retry
			return
		}
		// retry when no reply from the server
		select {
		case rf.appendEntriesCh[server] <- serialNo: // retry with serialNo
			lablog.Debug(rf.me, lablog.Drop, "-> S%d %s been dropped: {T:%d PLI:%d PLT:%d LC:%d log length:%d}, retry", server, rpcIntent, args.Term, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit, len(args.Entries))
		default:
		}
		return
	}

	lablog.Debug(rf.me, dTopicOfAppendEntriesRPC(args, lablog.Log), "%s <- S%d Reply: %+v", rpcIntent, server, *reply)

	if reply.Term > rf.CurrentTerm {
		// If RPC response contains term T > CurrentTerm: set CurrentTerm = T, convert to follower
		lablog.Debug(rf.me, lablog.Term, "%s <- S%d Term is higher(%d > %d), following", rpcIntent, server, reply.Term, rf.CurrentTerm)
		rf.stepDown(reply.Term)
		rf.electionAlarm = nextElectionAlarm()
		return
	}

	// IMPORTANT:
	// Adopt advice from #Term-confusion part of [students-guide-to-raft](https://thesquareplanet.com/blog/students-guide-to-raft/#term-confusion)
	// Quote:
	//  From experience, we have found that by far the simplest thing to do is to
	//  first record the term in the reply (it may be higher than your current term),
	//  and then to compare the current term with the term you sent in your original RPC.
	//  If the two are different, drop the reply and return.
	//  Only if the two terms are the same should you continue processing the reply.
	if rf.CurrentTerm != term {
		return
	}

	if reply.Success {
		// If successful: update nextIndex and matchIndex for follower
		// if success, nextIndex should ONLY increase
		oldNextIndex := rf.nextIndex[server]
		// if network reorders RPC replies, and to-update nextIndex < current nextIndex for a peer,
		// DON'T update nextIndex to smaller value (to avoid re-send log entries that follower already has)
		// So is matchIndex
		rf.nextIndex[server] = labutil.Max(args.PrevLogIndex+len(args.Entries)+1, rf.nextIndex[server])
		rf.matchIndex[server] = labutil.Max(args.PrevLogIndex+len(args.Entries), rf.matchIndex[server])
		lablog.Debug(rf.me, dTopicOfAppendEntriesRPC(args, lablog.Log), "%s RPC -> S%d success, updated NI:%v, MI:%v", rpcIntent, server, rf.nextIndex, rf.matchIndex)

		// matchIndex updated, maybe some log entries commit-able, going to check
		go rf.checkCommit(term)

		if rf.nextIndex[server] > oldNextIndex {
			// nextIndex updated, tell snapshoter to check if any delayed snapshot command can be processed
			select {
			case rf.snapshotTrigger <- true:
			default:
			}
		}

		return
	}

	// If AppendEntries fails because of log inconsistency:
	// decrement nextIndex and retry
	needToInstallSnapshot := false
	// OPTIMIZATION: fast log roll back
	if reply.XLen != 0 && reply.XTerm == 0 {
		// follower's log is too short
		rf.nextIndex[server] = reply.XLen
	} else {
		var entryIndex, entryTerm int
		for i := len(rf.Log) - 1; i >= -1; i-- {
			if i < 0 {
				entryIndex, entryTerm = rf.lastLogIndexAndTerm()
			} else {
				entryIndex, entryTerm = rf.Log[i].Index, rf.Log[i].Term
			}

			if entryTerm == reply.XTerm {
				// leader's log has XTerm
				rf.nextIndex[server] = entryIndex + 1
				break
			}
			if entryTerm < reply.XTerm {
				// leader's log doesn't have XTerm
				rf.nextIndex[server] = reply.XIndex
				break
			}

			if i < 0 {
				// leader's log too short, need to install snapshot to follower
				needToInstallSnapshot = true
				rf.nextIndex[server] = rf.LastIncludedIndex + 1
			}
		}
	}

	if needToInstallSnapshot || rf.nextIndex[server] <= rf.LastIncludedIndex {
		// leader's log too short, need to install snapshot to follower
		select {
		case rf.installSnapshotCh[server] <- 0:
		default:
		}
	} else {
		// try to append MORE previous log entries
		select {
		case rf.appendEntriesCh[server] <- 0:
		default:
		}
	}
}

// The entriesAppender go routine act as single point to call AppendEntries RPC to a server,
// Upon winning election, leader start this goroutine for each follower in its term,
// once step down or killed, this goroutine will terminate
func (rf *Raft) entriesAppender(server int, ch <-chan int, term int) {
	i := 1 // serialNo
	for !rf.killed() {
		serialNo, ok := <-ch
		if !ok {
			return // channel closed, should terminate this goroutine
		}
		rf.mu.Lock()
		// re-check
		if rf.state != Leader || rf.CurrentTerm != term || rf.killed() {
			rf.mu.Unlock()
			return
		}

		args := rf.constructAppendEntriesArgs(server)
		if args == nil {
			// cannot append entries, turning to install snapshot
			select {
			case rf.installSnapshotCh[server] <- 0:
				lablog.Debug(rf.me, lablog.Snap, "-> S%d cannot AE, going to IS", server)
			default:
			}
			rf.mu.Unlock()
			continue
		}
		rf.mu.Unlock()

		if serialNo == 0 || // new AppendEntries RPC call
			serialNo >= i { // is retry, need to check serialNo to ignore out-dated call
			go rf.appendEntries(server, term, args, i) // provide i as serialNo, to pass back if retry
			// increment this entriesAppender's serialNo
			i++
		}
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
	defer rf.mu.Unlock()
	term = rf.CurrentTerm
	if rf.state != Leader || rf.killed() {
		return
	}

	index = rf.nextIndex[rf.me]
	isLeader = true

	// If command received from client: append entry to local log
	rf.Log = append(rf.Log, LogEntry{
		Index:   index,
		Term:    term,
		Command: command,
	})
	rf.nextIndex[rf.me]++
	rf.matchIndex[rf.me] = index
	lablog.Debug(rf.me, lablog.Log2, "Received log: %v, with NI:%v, MI:%v", rf.Log[len(rf.Log)-1], rf.nextIndex, rf.matchIndex)

	rf.persist()

	// trigger entriesAppender for all followers, ask for agreement
	for i, c := range rf.appendEntriesCh {
		if i != rf.me {
			select {
			case c <- 0:
			default:
			}
		}
	}

	return
}

// The pacemaker go routine broadcasts periodic heartbeat to all followers
// Upon election: send initial empty AppendEntries RPCs (heartbeat) to each server
// repeat during idle periods to prevent election timeouts
func (rf *Raft) pacemaker(term int) {
	// Leader's pacemaker only last for its current term
	for !rf.killed() {
		rf.mu.Lock()
		// re-check state and term each time, before broadcast
		if rf.state != Leader || rf.CurrentTerm != term {
			rf.mu.Unlock()
			// end of pacemaker for this term
			return
		}
		// send heartbeat to each server
		lablog.Debug(rf.me, lablog.Timer, "Leader at T%d, broadcasting heartbeats", term)
		for i, c := range rf.appendEntriesCh {
			if i != rf.me {
				select {
				case c <- 0:
				default:
				}
			}
		}
		rf.mu.Unlock()

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
	if rf.state != Leader || rf.CurrentTerm != term || rf.killed() {
		return
	}

	// Leader's commitIndex is already last one, nothing to commit
	lastLogIndex, _ := rf.lastLogIndexAndTerm()
	if rf.commitIndex >= lastLogIndex {
		return
	}

	// If there exists an N such that
	// - N > commitIndex
	// - a majority of matchIndex[i] ≥ N
	// - log[N].term == CurrentTerm:
	// set commitIndex = N
	oldCommitIndex := rf.commitIndex
	for n := rf.commitIndex + 1; n < len(rf.Log)+rf.LastIncludedIndex+1; n++ {
		if rf.Log[n-rf.LastIncludedIndex-1].Term == term {
			cnt := 1
			for i := range rf.peers {
				if i != rf.me && rf.matchIndex[i] >= n {
					cnt++
				}
			}
			if cnt >= len(rf.peers)/2+1 {
				lablog.Debug(rf.me, lablog.Commit, "Commit achieved majority, set CI from %d to %d", rf.commitIndex, n)
				rf.commitIndex = n
			}
		}
	}

	if oldCommitIndex != rf.commitIndex {
		// going to commit
		select {
		case rf.commitTrigger <- true:
		default:
		}
	}
}

// The committer go routine run as background-goroutine,
// compare commitIndex with lastApplied,
// increment lastApplied if commit-safe
// and if any log applied, send to client through applyCh
func (rf *Raft) committer(applyCh chan<- ApplyMsg, triggerCh chan bool) {
	defer func() {
		// IMPORTANT: close channel to avoid resource leak
		close(applyCh)
		// IMPORTANT: drain commitTrigger to avoid goroutine resource leak
		for i := 0; i < len(triggerCh); i++ {
			<-triggerCh
		}
	}()

	// long-run singleton go routine, should check if killed
	for !rf.killed() {
		isCommit, ok := <-triggerCh // wait for signal

		if !ok {
			return
		}

		rf.mu.Lock()

		if !isCommit {
			// re-enable commitTrigger to be ready to accept commit signal
			rf.commitTrigger = triggerCh
			// is received snapshot from leader
			data := rf.persister.ReadSnapshot()
			if rf.LastIncludedIndex == 0 || len(data) == 0 {
				// snapshot data invalid
				rf.mu.Unlock()
				continue
			}

			// send snapshot back to upper-level service
			applyMsg := ApplyMsg{
				CommandValid:  false,
				SnapshotValid: true,
				Snapshot:      data,
				SnapshotIndex: rf.LastIncludedIndex,
				SnapshotTerm:  rf.LastIncludedTerm,
			}
			rf.mu.Unlock()

			applyCh <- applyMsg
			continue
		}

		// IMPORTANT: if there is a snapshot waiting to send back to upper-level service,
		// the snapshot should apply ASAP, and avoid ANY MORE log entries being applied.
		// Otherwise, try to apply as many log entries as possible
		for rf.commitTrigger != nil && rf.commitIndex > rf.lastApplied {
			// If commitIndex > lastApplied:
			// increment lastApplied,
			// apply log[lastApplied] to state machine
			rf.lastApplied++
			logEntry := rf.Log[rf.lastApplied-rf.LastIncludedIndex-1]
			lablog.Debug(rf.me, lablog.Client, "CI:%d > LA:%d, apply log: %s", rf.commitIndex, rf.lastApplied-1, logEntry)

			// release mutex before send ApplyMsg to applyCh
			rf.mu.Unlock()

			applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      logEntry.Command,
				CommandIndex: logEntry.Index,
			}

			// re-lock
			rf.mu.Lock()
		}

		rf.mu.Unlock()
	}
}

/************************* Persist (2C) ***************************************/

// get raft instance state as bytes, with mutex held
func (rf *Raft) raftState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(rf.CurrentTerm) != nil ||
		e.Encode(rf.VotedFor) != nil ||
		e.Encode(rf.Log) != nil ||
		e.Encode(rf.LastIncludedIndex) != nil ||
		e.Encode(rf.LastIncludedTerm) != nil {
		return nil
	}
	return w.Bytes()
}

//
// save Raft's persistent state to stable storage, with mutex held
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	if data := rf.raftState(); data == nil {
		lablog.Debug(rf.me, lablog.Error, "Write persistence failed")
	} else {
		lastLogIndex, lastLogTerm := rf.lastLogIndexAndTerm()
		lablog.Debug(rf.me, lablog.Persist, "Saved state T:%d VF:%d, (LII:%d LIT:%d), (LLI:%d LLT:%d)", rf.CurrentTerm, rf.VotedFor, rf.LastIncludedIndex, rf.LastIncludedTerm, lastLogIndex, lastLogTerm)
		rf.persister.SaveRaftState(data)
	}
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if len(data) == 0 { // bootstrap without any state
		return
	}
	// Your code here (2C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm, votedFor, lastIncludedIndex, lastIncludedTerm int
	var logs []LogEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logs) != nil ||
		d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&lastIncludedTerm) != nil {
		lablog.Debug(rf.me, lablog.Error, "Read broken persistence")
		return
	}
	rf.CurrentTerm = currentTerm
	rf.VotedFor = votedFor
	rf.Log = logs
	rf.LastIncludedIndex = lastIncludedIndex
	rf.LastIncludedTerm = lastIncludedTerm
}

/************************* Log Compaction (2D) ********************************/

// A service wants to switch to snapshot. Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	// Your code here (2D).
	// Previously, this lab recommended that you implement a function called CondInstallSnapshot
	// to avoid the requirement that snapshots and log entries sent on applyCh are coordinated.
	// This vestigial API interface remains, but you are discouraged from implementing it:
	// instead, we suggest that you simply have it return true.
	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	// no need to process immediately
	// send to queue for background-goroutine snapshoter to process
	select {
	case rf.snapshotCh <- snapshotCmd{index, snapshot}:
	default:
	}
}

// check if should suspend snapshot taken at index, with mutex held
// leader: suspend if any follower maybe lag (in a small amount, but not too far away, bound by leaderKeepLogAmount)
// otherwise: no suspend
func (rf *Raft) shouldSuspendSnapshot(index int) (r bool) {
	r = false
	if rf.state != Leader {
		return
	}

	for i := range rf.peers {
		if distance := index - rf.nextIndex[i]; distance >= 0 && distance <= leaderKeepLogAmount {
			// if any follower lagging, leader keeps a small amount of trailing log for a while
			r = true
			break
		}
	}
	return
}

// save snapshot and raft state, with mutex held
func (rf *Raft) saveStateAndSnapshot(snapshot []byte) {
	if data := rf.raftState(); data == nil {
		lablog.Debug(rf.me, lablog.Error, "Write snapshot failed")
	} else {
		lastLogIndex, lastLogTerm := rf.lastLogIndexAndTerm()
		lablog.Debug(rf.me, lablog.Snap, "Saved state: T:%d VF:%d, (LII:%d LIT:%d), (LLI:%d LLT:%d) and snapshot", rf.CurrentTerm, rf.VotedFor, rf.LastIncludedIndex, rf.LastIncludedTerm, lastLogIndex, lastLogTerm)
		rf.persister.SaveStateAndSnapshot(data, snapshot)
	}
}

// The snapshoter go routine run as background-goroutine to process snapshot command
func (rf *Raft) snapshoter(triggerCh <-chan bool) {
	// keep JUST one snapshot command
	var index int
	var snapshot []byte
	// may set to nil to block receiving from snapshotCh, to prevent current snapshot from being overwritten
	cmdCh := rf.snapshotCh

	// long-run singleton go routine, should check if killed
	for !rf.killed() {
		select {
		case cmd := <-cmdCh:
			index, snapshot = cmd.index, cmd.snapshot
		case _, ok := <-triggerCh:
			if !ok {
				return
			}
		}

		rf.mu.Lock()
		shouldSuspend := rf.shouldSuspendSnapshot(index)

		if cmdCh == nil {
			// receiving from snapshotCh is blocked =>
			// there is a delayed snapshot waiting to be processed and persisted,
			// re-check if can process this delayed snapshot
			if shouldSuspend {
				// no => wait for new trigger signal
				rf.mu.Unlock()
				continue
			}
			// yes => restore snapshotCh, ready to receive new snapshot command
			cmdCh = rf.snapshotCh
		}

		switch {
		case index <= rf.LastIncludedIndex:
			// this snapshot has been covered by LastIncludedIndex =>
			// this snapshot is out-of-date =>
			// ignore
		case shouldSuspend:
			// snapshot is up-to-date, but should suspend,
			// so block receiving from snapshotCh, and wait for trigger signal
			cmdCh = nil
		default:
			// snapshot is up-to-date, and can process to save

			// record term at index as LastIncludedTerm to persist
			rf.LastIncludedTerm = rf.Log[index-rf.LastIncludedIndex-1].Term
			// discard old log entries up through to index
			rf.Log = rf.Log[index-rf.LastIncludedIndex:]
			// record index as LastIncludedIndex to persist
			rf.LastIncludedIndex = index

			rf.saveStateAndSnapshot(snapshot)
		}

		rf.mu.Unlock()
	}
}

type InstallSnapshotArgs struct {
	Term              int    // leader's term
	LeaderId          int    // so follower can redirect clients
	LastIncludedIndex int    // the snapshot replaces all entries up through and including this index
	LastIncludedTerm  int    // term of LastIncludedIndex
	Offset            int    // byte offset where chunk is positioned in the snapshot file (not used)
	Data              []byte // raw bytes of the snapshot chunk, starting at Offset
	Done              bool   // true if this is the last chunk
}

type InstallSnapshotReply struct {
	Term int // CurrentTerm, for leader to update itself
}

/********************** InstallSnapshot RPC handler ***************************/

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.CurrentTerm

	if args.Term < rf.CurrentTerm || args.LeaderId == rf.me || rf.killed() {
		// Respond to RPCs from candidates and leaders
		// Followers only respond to requests from other servers
		// Reply immediately if term < CurrentTerm
		return
	}

	if args.Term > rf.CurrentTerm {
		// If RPC request contains term T > CurrentTerm: set CurrentTerm = T, convert to follower
		lablog.Debug(rf.me, lablog.Term, "S%d IS request term is higher(%d > %d), following", args.LeaderId, args.Term, rf.CurrentTerm)
		rf.stepDown(args.Term)
	}

	lablog.Debug(rf.me, lablog.Timer, "Resetting ELT, received IS from L%d at T%d", args.LeaderId, args.Term)
	rf.electionAlarm = nextElectionAlarm()

	if args.LastIncludedIndex <= rf.LastIncludedIndex || // out-of-date snapshot
		args.LastIncludedIndex <= rf.lastApplied { // IMPORTANT: already applied more log entries than snapshot, NO need to install this snapshot, otherwise will miss some log entries in between
		return
	}

	// assume args.Offset == 0 and args.Done == true
	lablog.Debug(rf.me, lablog.Snap, "Received snapshot from S%d at T%d, with (LII:%d LIT:%d)", args.LeaderId, args.Term, args.LastIncludedIndex, args.LastIncludedTerm)

	// snapshot accepted, start to install

	rf.LastIncludedIndex = args.LastIncludedIndex
	rf.LastIncludedTerm = args.LastIncludedTerm
	// update commitIndex and lastApplied
	rf.commitIndex = labutil.Max(args.LastIncludedIndex, rf.commitIndex)
	rf.lastApplied = args.LastIncludedIndex

	defer func() {
		// save snapshot file
		// discard any existing or partial snapshot with a smaller index (done by persister)
		rf.saveStateAndSnapshot(args.Data)

		if rf.commitTrigger != nil {
			// going to send snapshot to service
			// CANNOT lose this trigger signal, MUST wait channel sending done,
			// so cannot use select-default scheme
			go func(ch chan<- bool) { ch <- false }(rf.commitTrigger)
			// upon received a snapshot, must notify upper-level service ASAP,
			// before ANY new commit signal,
			// so set commitTrigger to nil to block other goroutines from sending to this channel.
			// committer will re-enable this channel once it start to process snapshot and send back to upper-level service
			rf.commitTrigger = nil
		}
	}()

	for i := range rf.Log {
		if rf.Log[i].Index == args.LastIncludedIndex && rf.Log[i].Term == args.LastIncludedTerm {
			// if existing log entry has same index and term as snapshot's last included entry,
			// retain log entries following it and reply
			rf.Log = rf.Log[i+1:]
			lablog.Debug(rf.me, lablog.Snap, "Retain log after index: %d term: %d, remain %d logs", args.LastIncludedIndex, args.LastIncludedTerm, len(rf.Log))
			return
		}
	}

	// discard the entire log
	rf.Log = make([]LogEntry, 0)
}

/********************** InstallSnapshot RPC caller ****************************/

// make InstallSnapshot RPC args, with mutex held
func (rf *Raft) constructInstallSnapshotArgs() *InstallSnapshotArgs {
	return &InstallSnapshotArgs{
		Term:              rf.CurrentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.LastIncludedIndex,
		LastIncludedTerm:  rf.LastIncludedTerm,
		Offset:            0,
		Data:              rf.persister.ReadSnapshot(),
		Done:              true,
	}
}

// send a InstallSnapshot RPC to a server
func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	lablog.Debug(rf.me, lablog.Snap, "T%d -> S%d Sending IS, (LII:%d LIT:%d)", args.Term, server, args.LastIncludedIndex, args.LastIncludedTerm)
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

// The installSnapshot go routine send InstallSnapshot RPC to one peer and deal with reply
func (rf *Raft) installSnapshot(server int, term int, args *InstallSnapshotArgs, serialNo int) {
	reply := &InstallSnapshotReply{}
	r := rf.sendInstallSnapshot(server, args, reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// re-check all relevant assumptions
	// - still a leader
	// - instance not killed
	if rf.state != Leader || rf.killed() {
		return
	}

	if !r {
		// retry when no reply from the server
		select {
		case rf.installSnapshotCh[server] <- serialNo: // retry with serialNo
			lablog.Debug(rf.me, lablog.Drop, "-> S%d IS been dropped: {T:%d LII:%d LIT:%d}, retry", server, args.Term, args.LastIncludedIndex, args.LastIncludedTerm)
		default:
		}
		return
	}

	if reply.Term > rf.CurrentTerm {
		// If RPC response contains term T > CurrentTerm: set CurrentTerm = T, convert to follower
		lablog.Debug(rf.me, lablog.Term, "IS <- S%d Term is higher(%d > %d), following", server, reply.Term, rf.CurrentTerm)
		rf.stepDown(reply.Term)
		rf.electionAlarm = nextElectionAlarm()
		return
	}

	// IMPORTANT:
	// Adopt advice from #Term-confusion part of [students-guide-to-raft](https://thesquareplanet.com/blog/students-guide-to-raft/#term-confusion)
	// Quote:
	//  From experience, we have found that by far the simplest thing to do is to
	//  first record the term in the reply (it may be higher than your current term),
	//  and then to compare the current term with the term you sent in your original RPC.
	//  If the two are different, drop the reply and return.
	//  Only if the two terms are the same should you continue processing the reply.
	if rf.CurrentTerm != term {
		return
	}

	// if success, nextIndex should ONLY increase
	oldNextIndex := rf.nextIndex[server]
	// if network reorders RPC replies, and to-update nextIndex < current nextIndex for a peer,
	// DON'T update nextIndex to smaller value (to avoid re-send log entries that follower already has)
	// So is matchIndex
	rf.nextIndex[server] = labutil.Max(args.LastIncludedIndex+1, rf.nextIndex[server])
	rf.matchIndex[server] = labutil.Max(args.LastIncludedIndex, rf.matchIndex[server])
	lablog.Debug(rf.me, lablog.Snap, "IS RPC -> S%d success, updated NI:%v, MI:%v", server, rf.nextIndex, rf.matchIndex)

	if rf.nextIndex[server] > oldNextIndex {
		// nextIndex updated, tell snapshoter to check if any delayed snapshot command can be processed
		select {
		case rf.snapshotTrigger <- true:
		default:
		}
	}
}

// The snapshotInstaller go routine act as single point to call InstallSnapshot RPC to a server,
// Upon winning election, leader start this goroutine for each follower in its term,
// once step down or killed, this goroutine will terminate
func (rf *Raft) snapshotInstaller(server int, ch <-chan int, term int) {
	var lastArgs *InstallSnapshotArgs // record last RPC's args
	i := 1                            // serialNo
	currentSnapshotSendCnt := 0       // count of outstanding InstallSnapshot RPC calls
	for !rf.killed() {
		serialNo, ok := <-ch
		if !ok {
			return // channel closed, should terminate this goroutine
		}
		rf.mu.Lock()

		switch {
		case rf.state != Leader || rf.CurrentTerm != term || rf.killed():
			// re-check
			rf.mu.Unlock()
			return
		case lastArgs == nil || rf.LastIncludedIndex > lastArgs.LastIncludedIndex:
			lastArgs = rf.constructInstallSnapshotArgs()
			// reset count of outstanding InstallSnapshot RPC calls
			currentSnapshotSendCnt = 1
			// increment this snapshotInstaller's serialNo
			i++
		case serialNo >= i:
			// is retry, need to check serialNo to ignore out-dated call
			// increment this snapshotInstaller's serialNo
			i++
		case rf.LastIncludedIndex == lastArgs.LastIncludedIndex && currentSnapshotSendCnt < 3:
			// send more to speed up, in case of network delay or drop
			// increment count of outstanding InstallSnapshot RPC calls
			currentSnapshotSendCnt++
		default:
			rf.mu.Unlock()
			continue
		}

		go rf.installSnapshot(server, term, lastArgs, i) // provide i as serialNo, to pass back if retry

		rf.mu.Unlock()
	}
}
