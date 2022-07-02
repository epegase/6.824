package kvraft

import (
	"fmt"
	"sync"
	"sync/atomic"

	"6.824/labgob"
	"6.824/lablog"
	"6.824/labrpc"
	"6.824/labutil"
	"6.824/raft"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type  opType
	Key   string
	Value string
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

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.

	tbl  map[string]string   // key-value table
	ctbl map[int]chan string // map from commandIndex to reply channel
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{Type: opGet, Key: args.Key}
	kv.mu.Lock()
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		kv.mu.Unlock()
		reply.Err = ErrWrongLeader
		return
	}

	lablog.Debug(kv.me, lablog.Server, "Start op %v at idx: %d", op, index)
	c := make(chan string) // reply channel for applier goroutine
	kv.ctbl[index] = c
	kv.mu.Unlock()

	// get reply from applier goroutine
	if s := <-c; s != "" {
		lablog.Debug(kv.me, lablog.Server, "Op %v at idx: %d get ...%s", op, index, s[labutil.Max(len(s)-10, 0):])
		reply.Err = OK
		reply.Value = s
	} else {
		reply.Err = ErrNoKey
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{Type: args.Op, Key: args.Key, Value: args.Value}
	kv.mu.Lock()
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		kv.mu.Unlock()
		reply.Err = ErrWrongLeader
		return
	}

	lablog.Debug(kv.me, lablog.Server, "Start op %v at idx: %d", op, index)
	c := make(chan string) // reply channel for applier goroutine
	kv.ctbl[index] = c
	kv.mu.Unlock()

	<-c // get reply from applier goroutine
	reply.Err = OK
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
	kv.ctbl = make(map[int]chan string)

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

		c, ok := kv.ctbl[m.CommandIndex]
		kv.mu.Unlock()

		if ok {
			// only leader server maintains ctbl, followers just apply kv modification
			c <- r

			kv.mu.Lock()
			delete(kv.ctbl, m.CommandIndex) // delete won't-use reply channel
			kv.mu.Unlock()
		}
	}
}
