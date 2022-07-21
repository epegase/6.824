package shardctrler

import (
	"bytes"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/lablog"
	"6.824/labrpc"
	"6.824/raft"
)

const (
	// in each RPC handler, in case of leader lose its term while waiting for applyMsg from applyCh,
	// handler will periodically check leader's currentTerm, to see if term changed
	rpcHandlerCheckRaftTermInterval = 100
	// after how many ApplyMsgs KVServer received and applied, should KVServer trigger to take a snapshot
	snapshoterAppliedMsgInterval = 50
)

type Op struct {
	// Your data here.
	Args interface{}

	// for duplicated op detection
	ClientId int64
	OpId     int
}

type applyResult struct {
	Err    Err
	Result Config
	OpId   int
}

type commandEntry struct {
	op      Op
	replyCh chan applyResult
}

type ShardCtrler struct {
	mu   sync.Mutex
	me   int
	rf   *raft.Raft
	dead int32 // set by Kill()

	commandTbl          map[int]commandEntry // map from commandIndex to commandEntry, maintained by leader, initialized to empty when restart
	appliedCommandIndex int                  // last applied commandIndex from applyCh

	// need to persist between restart
	Configs   []Config              // config history, indexed by config num
	ClientTbl map[int64]applyResult // map from clientId to last RPC operation result (for duplicated operation detection)
}

// common logic for Query/Join/Leave/Move RPC handler
// very same logic as kvraft server RPC handler (see ../kvraft/server.go)
func (sc *ShardCtrler) commonHandler(op Op) (e Err, r Config) {
	if sc.killed() {
		e = ErrShutdown
		return
	}

	sc.mu.Lock()
	index, term, isLeader := sc.rf.Start(op)
	if term == 0 {
		sc.mu.Unlock()
		e = ErrInitElection
		return
	}
	if !isLeader {
		sc.mu.Unlock()
		e = ErrWrongLeader
		return
	}
	c := make(chan applyResult)
	sc.commandTbl[index] = commandEntry{op: op, replyCh: c}
	sc.mu.Unlock()

CheckTermAndWaitReply:
	for !sc.killed() {
		select {
		case result, ok := <-c:
			if !ok {
				e = ErrShutdown
				return
			}
			e = result.Err
			r = result.Result
			return
		case <-time.After(rpcHandlerCheckRaftTermInterval * time.Millisecond):
			t, _ := sc.rf.GetState()
			if term != t {
				e = ErrWrongLeader
				break CheckTermAndWaitReply
			}
		}
	}

	go func() { <-c }()
	if sc.killed() {
		e = ErrShutdown
	}
	return
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	reply.Err, _ = sc.commonHandler(Op{Args: *args, ClientId: args.ClientId, OpId: args.OpId})
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	reply.Err, _ = sc.commonHandler(Op{Args: *args, ClientId: args.ClientId, OpId: args.OpId})
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	reply.Err, _ = sc.commonHandler(Op{Args: *args, ClientId: args.ClientId, OpId: args.OpId})
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	reply.Err, reply.Config = sc.commonHandler(Op{Args: *args, ClientId: args.ClientId, OpId: args.OpId})
}

//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	atomic.StoreInt32(&sc.dead, 1)
	sc.rf.Kill()
	// Your code here, if desired.
}

func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	labgob.Register(Op{})
	labgob.Register(QueryArgs{})
	labgob.Register(JoinArgs{})
	labgob.Register(LeaveArgs{})
	labgob.Register(MoveArgs{})

	sc := new(ShardCtrler)
	sc.me = me
	applyCh := make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, applyCh)
	sc.appliedCommandIndex = sc.rf.LastIncludedIndex
	sc.commandTbl = make(map[int]commandEntry)
	initConfig := Config{Num: 0}
	for i := range initConfig.Shards {
		initConfig.Shards[i] = 0
	}
	sc.Configs = []Config{initConfig}
	sc.ClientTbl = make(map[int64]applyResult)

	// initialize from snapshot persisted before a crash
	sc.readSnapshot(persister.ReadSnapshot())

	// communication between applier and snapshoter,
	// let applier trigger snapshoter to take a snapshot when certain amount of msgs have been applied
	snapshotTrigger := make(chan bool, 1)

	go sc.applier(applyCh, snapshotTrigger, sc.appliedCommandIndex)

	go sc.snapshoter(persister, snapshotTrigger)

	return sc
}

type gidShardsPair struct {
	gid    int
	shards []int
}
type gidShardsPairList []gidShardsPair

// sort group by shard load (measured by number of shards this group holds)
func (p gidShardsPairList) Len() int { return len(p) }
func (p gidShardsPairList) Less(i, j int) bool {
	li, lj := len(p[i].shards), len(p[j].shards)
	return li < lj || (li == lj && p[i].gid < p[j].gid)
}
func (p gidShardsPairList) Swap(i, j int) { p[i], p[j] = p[j], p[i] }

// get group shard load of a config
// `sortedGroupLoad` sorted ascend by shard load
func (cfg *Config) getGroupLoad() (groupLoad map[int][]int, sortedGroupLoad gidShardsPairList) {
	groupLoad = make(map[int][]int)
	// group that holds shard(s)
	for shard, gid := range cfg.Shards {
		groupLoad[gid] = append(groupLoad[gid], shard)
	}
	// group that holds no shard
	for gid := range cfg.Groups {
		if _, ok := groupLoad[gid]; !ok {
			groupLoad[gid] = []int{}
		}
	}

	// sort groups by shard load
	sortedGroupLoad = make(gidShardsPairList, len(groupLoad))
	i := 0
	for k, v := range groupLoad {
		sortedGroupLoad[i] = gidShardsPair{k, v}
		i++
	}
	sort.Sort(sortedGroupLoad)
	return
}

// rebalance shards across all groups
// with two goals:
// - divide the shards as evenly as possible among the full set of groups
// - move as few shards as possible
func (cfg *Config) rebalance(oldConfig *Config, joinGids []int, leaveGids []int) {
	groupLoad, sortedGroupLoad := oldConfig.getGroupLoad()
	nOldGroups := len(sortedGroupLoad)

	// copy from previous config
	copy(cfg.Shards[:], oldConfig.Shards[:])

	switch {
	case len(cfg.Groups) == 0:
		// no groups, all shards be assigned to GID zero
		for i := range cfg.Shards {
			cfg.Shards[i] = 0
		}
	case len(leaveGids) > 0:
		// group leaves, evenly distribute shards on these groups to other remaining groups
		// distribute assignments start from low-load group
		leaveGidSet := map[int]bool{}
		for _, gid := range leaveGids {
			leaveGidSet[gid] = true
		}
		i := 0
		for _, gid := range leaveGids {
			for _, shard := range groupLoad[gid] { // distribute shards of this group
				for leaveGidSet[sortedGroupLoad[i%nOldGroups].gid] { // skip leaving group
					i++
				}
				cfg.Shards[shard] = sortedGroupLoad[i%nOldGroups].gid // assign to a available group
				i++
			}
		}
	case len(joinGids) > 0:
		// new group joins, offload some shards from existing groups to these new groups
		// offload start from high-load group
		nNewGroups := len(cfg.Groups)
		minLoad := NShards / nNewGroups // minimum balanced load
		nHigherLoadGroups := NShards - minLoad*nNewGroups
		joinGroupLoad := map[int]int{}
		// calculate load (# of shards) for each newly-joined group
		for i, gid := range joinGids {
			if i >= nNewGroups-nHigherLoadGroups {
				// some groups should carry one more shard to make WHOLE cluster load-balanced
				joinGroupLoad[gid] = minLoad + 1
			} else {
				joinGroupLoad[gid] = minLoad
			}
		}

		i := 0
		for _, gid := range joinGids {
			for j := 0; j < joinGroupLoad[gid]; j, i = j+1, i+1 {
				// take shards in a round-robin manner, start from high-load group
				idx := (nOldGroups - 1 - i) % nOldGroups
				if idx < 0 {
					idx = nOldGroups + idx
				}
				sourceGroup := &sortedGroupLoad[idx]
				// assign the first shard of this high-load group to a newly joined group
				cfg.Shards[sourceGroup.shards[0]] = gid
				// and remove this first shard from original group
				sourceGroup.shards = sourceGroup.shards[1:]
			}
		}
	}
}

// The applier go routine accept applyMsg from applyCh (from underlying raft),
// modify shard configs accordingly,
// reply config changes back to Shard Controller's RPC handler, if any, through channel identified by commandIndex
// after every snapshoterAppliedMsgInterval msgs, trigger a snapshot
func (sc *ShardCtrler) applier(applyCh <-chan raft.ApplyMsg, snapshotTrigger chan<- bool, lastSnapshoterTriggeredCommandIndex int) {
	var r Config

	for m := range applyCh {
		if m.SnapshotValid {
			// is snapshot, reset shard controller state according to this snapshot
			sc.mu.Lock()
			sc.appliedCommandIndex = m.SnapshotIndex
			sc.readSnapshot(m.Snapshot)
			// clear all pending reply channel, to avoid goroutine resource leak
			for _, ce := range sc.commandTbl {
				ce.replyCh <- applyResult{Err: ErrWrongLeader}
			}
			sc.commandTbl = make(map[int]commandEntry)
			sc.mu.Unlock()
			continue
		}

		if !m.CommandValid {
			continue
		}

		if m.CommandIndex-lastSnapshoterTriggeredCommandIndex > snapshoterAppliedMsgInterval {
			// certain amount of msgs have been applied, going to tell snapshoter to take a snapshot
			select {
			case snapshotTrigger <- true:
				lastSnapshoterTriggeredCommandIndex = m.CommandIndex // record as last time triggered commandIndex
			default:
			}
		}

		op := m.Command.(Op)
		sc.mu.Lock()

		sc.appliedCommandIndex = m.CommandIndex

		lastOpResult := sc.ClientTbl[op.ClientId]
		if lastOpResult.OpId >= op.OpId {
			// detect duplicated operation
			// reply with cached result, don't update shard configs
			r = lastOpResult.Result
		} else {
			l := len(sc.Configs)
			switch args := op.Args.(type) {
			case QueryArgs:
				if args.Num == -1 || args.Num >= l {
					// query for latest config
					args.Num = l - 1
				}
				r = sc.Configs[args.Num]
			case JoinArgs:
				lablog.ShardDebug(0, sc.me, lablog.Ctrler, "Apply Join: %v", args.Servers)
				lastConfig := sc.Configs[l-1]
				// copy from previous config
				newConfig := Config{Num: lastConfig.Num + 1, Groups: make(map[int][]string)}
				for k, v := range lastConfig.Groups {
					newConfig.Groups[k] = v
				}

				joinGids := []int{}
				for k, v := range args.Servers {
					// add new group
					newConfig.Groups[k] = v
					joinGids = append(joinGids, k)
				}
				// IMPORTANT: sort gids to make DETERMINISTIC shard rebalancing
				sort.Ints(joinGids)
				// going to rebalance shards across groups
				newConfig.rebalance(&lastConfig, joinGids, nil)
				// record new config
				sc.Configs = append(sc.Configs, newConfig)
				r = Config{Num: -1}
			case LeaveArgs:
				lablog.ShardDebug(0, sc.me, lablog.Ctrler, "Apply Leave: %v", args.GIDs)
				lastConfig := sc.Configs[l-1]
				// copy from previous config
				newConfig := Config{Num: lastConfig.Num + 1, Groups: make(map[int][]string)}
				for k, v := range lastConfig.Groups {
					newConfig.Groups[k] = v
				}

				for _, gid := range args.GIDs {
					// delete leaving group
					delete(newConfig.Groups, gid)
				}
				// going to rebalance shards across groups
				newConfig.rebalance(&lastConfig, nil, args.GIDs)
				// record new config
				sc.Configs = append(sc.Configs, newConfig)
				r = Config{Num: -1}
			case MoveArgs:
				lablog.ShardDebug(0, sc.me, lablog.Ctrler, "Apply Move: shard %v -> group %v", args.Shard, args.GID)
				lastConfig := sc.Configs[l-1]
				// copy from previous config
				newConfig := Config{Num: lastConfig.Num + 1, Groups: make(map[int][]string)}
				for k, v := range lastConfig.Groups {
					newConfig.Groups[k] = v
				}
				copy(newConfig.Shards[:], lastConfig.Shards[:])
				// move shard to gid
				newConfig.Shards[args.Shard] = args.GID
				// record new config
				sc.Configs = append(sc.Configs, newConfig)
				r = Config{Num: -1}
			default:
				panic(args)
			}

			if r.Num < 0 {
				// log write operation(Join/Leave/Move) result
				latestConfig := sc.Configs[len(sc.Configs)-1]
				_, sortedGroupLoad := latestConfig.getGroupLoad()
				output := fmt.Sprintf("%d %v / ", latestConfig.Num, latestConfig.Groups)
				for _, p := range sortedGroupLoad {
					output += fmt.Sprintf("G%d:%v ", p.gid, p.shards)
				}
				lablog.ShardDebug(0, sc.me, lablog.Info, output)
			}

			// cache operation result
			sc.ClientTbl[op.ClientId] = applyResult{OpId: op.OpId, Result: r}
		}

		ce, ok := sc.commandTbl[m.CommandIndex]
		if ok {
			delete(sc.commandTbl, m.CommandIndex) // delete won't-use reply channel
		}
		sc.mu.Unlock()

		// only leader server maintains shard configs, followers just apply config modification
		if ok {
			if ce.op.ClientId != op.ClientId || ce.op.OpId != op.OpId {
				// leader lost its leadership before the request is committed to the log
				ce.replyCh <- applyResult{Err: ErrWrongLeader}
			} else {
				ce.replyCh <- applyResult{Err: OK, Result: r}
			}
		}
	}

	close(snapshotTrigger)
	// clean all pending RPC handler reply channel, avoid goroutine resource leak
	sc.mu.Lock()
	defer sc.mu.Unlock()
	for _, ce := range sc.commandTbl {
		close(ce.replyCh)
	}
}

// The snapshoter go routine take a snapshot when receive a trigger event from applier
func (sc *ShardCtrler) snapshoter(persister *raft.Persister, snapshotTrigger <-chan bool) {
	for !sc.killed() {
		// wait for trigger
		_, ok := <-snapshotTrigger

		if ok {
			sc.mu.Lock()
			if data := sc.shardCtrlerSnapshot(); data == nil {
				lablog.ShardDebug(0, sc.me, lablog.Error, "Write snapshot failed")
			} else {
				sc.rf.Snapshot(sc.appliedCommandIndex, data)
			}
			sc.mu.Unlock()
		}
	}
}

// get shard controller instance state to be snapshotted, with mutex held
func (sc *ShardCtrler) shardCtrlerSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(sc.Configs) != nil ||
		e.Encode(sc.ClientTbl) != nil {
		return nil
	}
	return w.Bytes()
}

// restore previously persisted snapshot, with mutex held
func (sc *ShardCtrler) readSnapshot(data []byte) {
	if len(data) == 0 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var configs []Config
	var clientTbl map[int64]applyResult
	if d.Decode(&configs) != nil ||
		d.Decode(&clientTbl) != nil {
		lablog.ShardDebug(0, sc.me, lablog.Error, "Read broken snapshot")
		return
	}
	sc.Configs = configs
	sc.ClientTbl = clientTbl
}
