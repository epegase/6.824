package kvraft

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/lablog"
	"6.824/labrpc"
	"6.824/raft"
)

const (
	rpcHandlerCheckRaftTermInterval = 100
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type  opType
	Key   string
	Value string

	// for duplicated op detection
	ClientId int64
	OpId     int
}

func (op Op) String() string {
	switch op.Type {
	case opGet:
		return fmt.Sprintf("{G %s}", op.Key)
	case opPut:
		return fmt.Sprintf("{P %s:%s}", op.Key, op.Value)
	case opAppend:
		return fmt.Sprintf("{A %s:+%s}", op.Key, op.Value)
	default:
		return ""
	}
}

type applyResult struct {
	err   Err
	value string
	opId  int
}

func (r applyResult) String() string {
	switch r.err {
	case OK:
		if l := len(r.value); l < 10 {
			return r.value
		} else {
			return fmt.Sprintf("...%s", r.value[l-10:])
		}
	default:
		return string(r.err)
	}
}

type commandEntry struct {
	op      Op
	replyCh chan applyResult
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.

	tbl        map[string]string     // key-value table
	commandTbl map[int]commandEntry  // map from commandIndex to commandEntry, maintained by leader
	clientTbl  map[int64]applyResult // map from clientId to last RPC operation result (for duplicated operation detection)
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	if kv.killed() {
		reply.Err = ErrShutdown
		return
	}

	op := Op{Type: opGet, Key: args.Key, ClientId: args.ClientId, OpId: args.OpId}
	kv.mu.Lock()
	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		kv.mu.Unlock()
		reply.Err = ErrWrongLeader
		return
	}

	lablog.Debug(kv.me, lablog.Server, "Start op %v at idx: %d", op, index)
	c := make(chan applyResult) // reply channel for applier goroutine
	kv.commandTbl[index] = commandEntry{op: op, replyCh: c}
	kv.mu.Unlock()

CheckTermAndWaitReply:
	for !kv.killed() {
		select {
		case result := <-c:
			// get reply from applier goroutine
			lablog.Debug(kv.me, lablog.Server, "Op %v at idx: %d get %v", op, index, result)
			*reply = GetReply{Err: result.err, Value: result.value}
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

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if kv.killed() {
		reply.Err = ErrShutdown
		return
	}

	op := Op{Type: args.Op, Key: args.Key, Value: args.Value, ClientId: args.ClientId, OpId: args.OpId}
	kv.mu.Lock()
	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		kv.mu.Unlock()
		reply.Err = ErrWrongLeader
		return
	}

	lablog.Debug(kv.me, lablog.Server, "Start op %v at idx: %d", op, index)
	c := make(chan applyResult) // reply channel for applier goroutine
	kv.commandTbl[index] = commandEntry{op: op, replyCh: c}
	kv.mu.Unlock()

CheckTermAndWaitReply:
	for !kv.killed() {
		select {
		case result := <-c:
			// get reply from applier goroutine
			lablog.Debug(kv.me, lablog.Server, "Op %v at idx: %d completed", op, index)
			*reply = PutAppendReply{Err: result.err}
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
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test kv.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.tbl = make(map[string]string)
	kv.commandTbl = make(map[int]commandEntry)
	kv.clientTbl = make(map[int64]applyResult)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	go kv.applier()

	return kv
}

// The applier go routine accept applyMsg from applyCh (from underlying raft),
// modify key-value table accordingly,
// reply modified result back to KVServer's RPC handler, if any, through channel identified by commandIndex
func (kv *KVServer) applier() {
	var r string

	for m := range kv.applyCh {
		if !m.CommandValid {
			continue
		}

		op := m.Command.(Op)
		kv.mu.Lock()

		lastOpResult, ok := kv.clientTbl[op.ClientId]
		if ok {
			lablog.Debug(kv.me, lablog.Server, "Get op %#v at idx: %d, lastOpId: %d", op, m.CommandIndex, lastOpResult.opId)
		} else {
			lablog.Debug(kv.me, lablog.Server, "Get op %#v at idx: %d", op, m.CommandIndex)
		}

		if lastOpResult.opId >= op.OpId {
			// detect duplicated operation
			// reply with cached result, don't update kv table
			r = lastOpResult.value
		} else {
			switch op.Type {
			case opGet:
				r = kv.tbl[op.Key]
			case opPut:
				kv.tbl[op.Key] = op.Value
				r = ""
			case opAppend:
				kv.tbl[op.Key] = kv.tbl[op.Key] + op.Value
				r = ""
			}

			// cache operation result
			kv.clientTbl[op.ClientId] = applyResult{value: r, opId: op.OpId}
		}

		ce, ok := kv.commandTbl[m.CommandIndex]
		if ok {
			delete(kv.commandTbl, m.CommandIndex) // delete won't-use reply channel
		}
		kv.mu.Unlock()

		// only leader server maintains commandTbl, followers just apply kv modification
		if ok {
			lablog.Debug(kv.me, lablog.Server, "Command tbl found for cidx: %d, %v", m.CommandIndex, ce)
			if ce.op != op {
				// Your solution needs to handle a leader that has called Start() for a Clerk's RPC,
				// but loses its leadership before the request is committed to the log.
				// In this case you should arrange for the Clerk to re-send the request to other servers
				// until it finds the new leader.
				//
				// One way to do this is for the server to detect that it has lost leadership,
				// by noticing that a different request has appeared at the index returned by Start()
				ce.replyCh <- applyResult{err: ErrWrongLeader}
			} else {
				ce.replyCh <- applyResult{err: OK, value: r}
			}
		}
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()
	for _, ce := range kv.commandTbl {
		ce.replyCh <- applyResult{err: ErrShutdown}
	}
}
