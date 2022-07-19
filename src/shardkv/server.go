package shardkv

import (
	"bytes"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/lablog"
	"6.824/labrpc"
	"6.824/raft"
	"6.824/shardctrler"
)

const (
	// in each RPC handler, in case of leader lose its term while waiting for applyMsg from applyCh,
	// handler will periodically check leader's currentTerm, to see if term changed
	rpcHandlerCheckRaftTermInterval = 100
	// max interval between snapshoter's checking of RaftStateSize,
	// snapshoter will sleep according to RaftStateSize,
	// if size grows big, sleep less time, and check more quickly,
	// otherwise, sleep more time
	snapshoterCheckInterval = 100
	// in which ratio of RaftStateSize/maxraftstate should KVServer going to take a snapshot
	snapshotThresholdRatio = 0.9
	// after how many ApplyMsgs KVServer received and applied, should KVServer trigger to take a snapshot
	snapshoterAppliedMsgInterval = 50
	serverRefreshConfigInterval  = 100
)

type Op struct {
	Type  opType
	Key   string
	Value string

	// for duplicated op detection
	ClientId int64
	OpId     int
}
type applyResult struct {
	Err   Err
	Value string
	OpId  int
}
type commandEntry struct {
	op      Op
	replyCh chan applyResult
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	sm           *shardctrler.Clerk // shard manager
	dead         int32              // set by Kill()
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	maxraftstate float64 // snapshot if log grows this big

	// Your definitions here.
	commandTbl          map[int]commandEntry // map from commandIndex to commandEntry, maintained by leader, initialized to empty when restart
	appliedCommandIndex int                  // last applied commandIndex from applyCh
	config              shardctrler.Config   // latest known shard config

	// need to persist between restart
	Tbl       map[string]string     // key-value table
	ClientTbl map[int64]applyResult // map from clientId to last RPC operation result (for duplicated operation detection)
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	if kv.killed() {
		reply.Err = ErrShutdown
		return
	}

	op := Op{Type: opGet, Key: args.Key, ClientId: args.ClientId, OpId: args.OpId}
	// IMPORTANT: lock before rf.Start,
	// to avoid raft finish too quick before kv.commandTbl has set replyCh for this commandIndex
	kv.mu.Lock()

	if kv.config.Num == 0 || kv.config.Shards[key2shard(args.Key)] != kv.gid {
		kv.mu.Unlock()
		reply.Err = ErrWrongGroup
		return
	}

	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		kv.mu.Unlock()
		reply.Err = ErrWrongLeader
		return
	}

	lablog.Debug(kv.me, lablog.Server, "Start op %v at idx: %d, from %d of client %d", op, index, op.OpId, op.ClientId)
	c := make(chan applyResult) // reply channel for applier goroutine
	kv.commandTbl[index] = commandEntry{op: op, replyCh: c}
	kv.mu.Unlock()

CheckTermAndWaitReply:
	for !kv.killed() {
		select {
		case result, ok := <-c:
			if !ok {
				reply.Err = ErrShutdown
				return
			}
			// get reply from applier goroutine
			lablog.Debug(kv.me, lablog.Server, "Op %v at idx: %d get %v", op, index, result)
			*reply = GetReply{Err: result.Err, Value: result.Value}
			return
		case <-time.After(rpcHandlerCheckRaftTermInterval * time.Millisecond):
			t, _ := kv.rf.GetState()
			if term != t {
				reply.Err = ErrWrongLeader
				break CheckTermAndWaitReply
			}
		}
	}

	go func() { <-c }() // avoid applier from blocking, and avoid resource leak
	if kv.killed() {
		reply.Err = ErrShutdown
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if kv.killed() {
		reply.Err = ErrShutdown
		return
	}

	op := Op{Type: args.Op, Key: args.Key, Value: args.Value, ClientId: args.ClientId, OpId: args.OpId}
	kv.mu.Lock()

	if kv.config.Num == 0 || kv.config.Shards[key2shard(args.Key)] != kv.gid {
		kv.mu.Unlock()
		reply.Err = ErrWrongGroup
		return
	}

	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		kv.mu.Unlock()
		reply.Err = ErrWrongLeader
		return
	}

	lablog.Debug(kv.me, lablog.Server, "Start op %v at idx: %d, from %d of client %d", op, index, op.OpId, op.ClientId)
	c := make(chan applyResult) // reply channel for applier goroutine
	kv.commandTbl[index] = commandEntry{op: op, replyCh: c}
	kv.mu.Unlock()

CheckTermAndWaitReply:
	for !kv.killed() {
		select {
		case result, ok := <-c:
			if !ok {
				reply.Err = ErrShutdown
				return
			}
			// get reply from applier goroutine
			lablog.Debug(kv.me, lablog.Server, "Op %v at idx: %d completed", op, index)
			reply.Err = result.Err
			return
		case <-time.After(rpcHandlerCheckRaftTermInterval * time.Millisecond):
			t, _ := kv.rf.GetState()
			if term != t {
				reply.Err = ErrWrongLeader
				break CheckTermAndWaitReply
			}
		}
	}

	go func() { <-c }() // avoid applier from blocking, and avoid resource leak
	if kv.killed() {
		reply.Err = ErrShutdown
	}
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(
	servers []*labrpc.ClientEnd,
	me int,
	persister *raft.Persister,
	maxraftstate int,
	gid int,
	ctrlers []*labrpc.ClientEnd,
	make_end func(string) *labrpc.ClientEnd,
) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.make_end = make_end
	kv.gid = gid
	kv.maxraftstate = float64(maxraftstate)
	applyCh := make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, applyCh)
	kv.sm = shardctrler.MakeClerk(ctrlers)

	kv.appliedCommandIndex = kv.rf.LastIncludedIndex
	kv.commandTbl = make(map[int]commandEntry)
	kv.Tbl = make(map[string]string)
	kv.ClientTbl = make(map[int64]applyResult)

	// initialize from snapshot persisted before a crash
	kv.readSnapshot(persister.ReadSnapshot())

	// communication between applier and snapshoter,
	// let applier trigger snapshoter to take a snapshot when certain amount of msgs have been applied
	snapshotTrigger := make(chan bool, 1)

	go kv.applier(applyCh, snapshotTrigger, kv.appliedCommandIndex)

	go kv.snapshoter(persister, snapshotTrigger)

	go kv.configFetcher()

	return kv
}

// The applier go routine accept applyMsg from applyCh (from underlying raft),
// modify key-value table accordingly,
// reply modified result back to KVServer's RPC handler, if any, through channel identified by commandIndex
// after every snapshoterAppliedMsgInterval msgs, trigger a snapshot
func (kv *ShardKV) applier(applyCh <-chan raft.ApplyMsg, snapshotTrigger chan<- bool, lastSnapshoterTriggeredCommandIndex int) {
	var r string

	for m := range applyCh {
		if m.SnapshotValid {
			// is snapshot, reset kv server state according to this snapshot
			kv.mu.Lock()
			kv.appliedCommandIndex = m.SnapshotIndex
			kv.readSnapshot(m.Snapshot)
			// clear all pending reply channel, to avoid goroutine resource leak
			for _, ce := range kv.commandTbl {
				ce.replyCh <- applyResult{Err: ErrWrongLeader}
			}
			kv.commandTbl = make(map[int]commandEntry)
			kv.mu.Unlock()
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
		kv.mu.Lock()

		kv.appliedCommandIndex = m.CommandIndex

		lastOpResult := kv.ClientTbl[op.ClientId]

		if lastOpResult.OpId >= op.OpId {
			// detect duplicated operation
			// reply with cached result, don't update kv table
			r = lastOpResult.Value
		} else {
			switch op.Type {
			case opGet:
				r = kv.Tbl[op.Key]
			case opPut:
				kv.Tbl[op.Key] = op.Value
				r = ""
			case opAppend:
				kv.Tbl[op.Key] = kv.Tbl[op.Key] + op.Value
				r = ""
			}

			// cache operation result
			kv.ClientTbl[op.ClientId] = applyResult{Value: r, OpId: op.OpId}
		}

		ce, ok := kv.commandTbl[m.CommandIndex]
		if ok {
			delete(kv.commandTbl, m.CommandIndex) // delete won't-use reply channel
		}
		kv.mu.Unlock()

		// only leader server maintains commandTbl, followers just apply kv modification
		if ok {
			if ce.op.ClientId != op.ClientId || ce.op.OpId != op.OpId {
				// Your solution needs to handle a leader that has called Start() for a Clerk's RPC,
				// but loses its leadership before the request is committed to the log.
				// In this case you should arrange for the Clerk to re-send the request to other servers
				// until it finds the new leader.
				//
				// One way to do this is for the server to detect that it has lost leadership,
				// by noticing that a different request has appeared at the index returned by Start()
				ce.replyCh <- applyResult{Err: ErrWrongLeader}
			} else {
				ce.replyCh <- applyResult{Err: OK, Value: r}
			}
		}
	}

	// clean all pending RPC handler reply channel, avoid goroutine resource leak
	kv.mu.Lock()
	defer kv.mu.Unlock()
	for _, ce := range kv.commandTbl {
		close(ce.replyCh)
	}
}

// The snapshoter go routine periodically check if raft state size is approaching maxraftstate threshold,
// if so, save a snapshot,
// or, if receive a trigger event from applier, also take a snapshot
func (kv *ShardKV) snapshoter(persister *raft.Persister, snapshotTrigger <-chan bool) {
	if kv.maxraftstate < 0 {
		// no need to take snapshot
		return
	}

	for !kv.killed() {
		ratio := float64(persister.RaftStateSize()) / kv.maxraftstate
		if ratio > snapshotThresholdRatio {
			// is approaching threshold
			kv.mu.Lock()
			if data := kv.kvServerSnapshot(); data == nil {
				lablog.Debug(kv.me, lablog.Error, "Write snapshot failed")
			} else {
				// take a snapshot
				kv.rf.Snapshot(kv.appliedCommandIndex, data)
			}
			kv.mu.Unlock()

			ratio = 0.0
		}

		select {
		// sleep according to current RaftStateSize/maxraftstate ratio
		case <-time.After(time.Duration((1-ratio)*snapshoterCheckInterval) * time.Millisecond):
		// waiting for trigger
		case <-snapshotTrigger:
		}
	}
}

// The configFetcher go routine periodically poll the shardctrler,
// to learn about new configurations
func (kv *ShardKV) configFetcher() {
	for !kv.killed() {
		config := kv.sm.Query(-1)

		kv.mu.Lock()
		if config.Num > kv.config.Num {
			kv.config = config
		}
		kv.mu.Unlock()

		time.Sleep(serverRefreshConfigInterval * time.Millisecond)
	}
}

// get KVServer instance state to be snapshotted, with mutex held
func (kv *ShardKV) kvServerSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(kv.Tbl) != nil ||
		e.Encode(kv.ClientTbl) != nil {
		return nil
	}
	return w.Bytes()
}

// restore previously persisted snapshot, with mutex held
func (kv *ShardKV) readSnapshot(data []byte) {
	if len(data) == 0 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var tbl map[string]string
	var clientTbl map[int64]applyResult
	if d.Decode(&tbl) != nil ||
		d.Decode(&clientTbl) != nil {
		lablog.Debug(kv.me, lablog.Error, "Read broken snapshot")
		return
	}
	kv.Tbl = tbl
	kv.ClientTbl = clientTbl
}
