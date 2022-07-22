package shardkv

import (
	"bytes"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/lablog"
	"6.824/labrpc"
	"6.824/labutil"
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
	// Your server will need to periodically poll the shardctrler to learn about new configurations.
	// The tests expect that your code polls roughly every 100 milliseconds;
	// more often is OK, but much less often may cause problems.
	serverRefreshConfigInterval = 100
	// client be told to wait a while and retry
	serverWaitAndRetryInterval = 50
)

type Op struct {
	Payload interface{}
}

func (op *Op) String() string {
	switch payload := op.Payload.(type) {
	case GetArgs:
		return fmt.Sprintf("{G%s}", payload.Key)
	case PutAppendArgs:
		if payload.Op == opPut {
			return fmt.Sprintf("{P%s}", payload.Key)
		}
		return fmt.Sprintf("{A%s}", payload.Key)
	case MigrateShardsArgs:
		return fmt.Sprintf("{M%d}", payload.ConfigNum)
	case RequestShardsArgs:
		return fmt.Sprintf("{R%d}", payload.ConfigNum)
	default:
		return ""
	}
}

func (op *Op) getClientIdAndOpId() (clientId int64, opId int) {
	switch payload := op.Payload.(type) {
	case GetArgs:
		clientId, opId = payload.ClientId, payload.OpId
	case PutAppendArgs:
		clientId, opId = payload.ClientId, payload.OpId
	case MigrateShardsArgs:
		clientId, opId = payload.ClientId, payload.OpId
	case RequestShardsArgs:
		clientId, opId = payload.ClientId, payload.OpId
	}
	return
}

// channel message from applier to RPC handler,
// and as cached operation result, to respond to duplicated operation
type applyResult struct {
	Err   Err
	Value interface{}
	OpId  int
}

// reply channel from applier to RPC handler, of a commandIndex
type commandEntry struct {
	op      Op
	replyCh chan applyResult
}

type kvTable map[string]string

type ShardKV struct {
	mu                   sync.Mutex
	me                   int
	rf                   *raft.Raft
	sm                   *shardctrler.Clerk // shard manager
	dead                 int32              // set by Kill()
	make_end             func(string) *labrpc.ClientEnd
	gid                  int       // my group id
	maxraftstate         float64   // snapshot if log grows this big
	configFetcherTrigger chan bool // trigger configFetcher to update shard config

	commandTbl          map[int]commandEntry // map from commandIndex to commandEntry, maintained by leader, initialized to empty when restart
	appliedCommandIndex int                  // last applied commandIndex from applyCh
	config              shardctrler.Config   // latest known shard config

	// need to persist between restart
	Tbl        kvTable               // key-value table
	ClientTbl  map[int64]applyResult // map from clientId to last RPC operation result (for duplicated operation detection)
	ClientId   int64                 // when migrate shards data to other group, act as a ShardKV client, so need a ClientId
	OpId       int                   // also for duplicated migration detection, need an OpId
	MigrateOut map[int]*migrateOut   // for each other group, map of gid -> migrate-out data
	WaitIn     map[int]*waitIn       // for each other group, map of gid -> wait-in data
}

// common logic for RPC handler, with mutex held
// and is RESPONSIBLE for mutex release
//
// 1. check is leader
// 2. check request's configNum
// 3. start Raft consensus
// 4. wait for agreement result from applier goroutine
// 5. return with RPC reply result
func (kv *ShardKV) commonHandler(requestConfigNum int, op Op) (e Err, r interface{}) {
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		// only leader should take responsibility to check request's configNum, then start Raft consensus
		kv.mu.Unlock()
		e = ErrWrongLeader
		return
	}
	if kv.config.Num < requestConfigNum {
		// my shard config seems outdated, tell configFetcher to update
		kv.mu.Unlock()
		select {
		case kv.configFetcherTrigger <- true:
		default:
		}
		// cannot accept request because of unknown config from future, tell client to try later
		e = ErrUnknownConfig
		return
	}

	if kv.config.Num > requestConfigNum {
		// request's config is outdated, abort request, and tell client to update its shard config
		kv.mu.Unlock()
		e = ErrOutdatedConfig
		return
	}

	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		kv.mu.Unlock()
		e = ErrWrongLeader
		return
	}

	clientId, opId := op.getClientIdAndOpId()
	lablog.ShardDebug(kv.gid, kv.me, lablog.Server, "Start %v@%d, %d/%d", op, index, opId, clientId%100)
	c := make(chan applyResult) // reply channel for applier goroutine
	kv.commandTbl[index] = commandEntry{op: op, replyCh: c}
	kv.mu.Unlock()

CheckTermAndWaitReply:
	for !kv.killed() {
		select {
		case result, ok := <-c:
			if !ok {
				e = ErrShutdown
				return
			}
			// get reply from applier goroutine
			switch v := result.Value.(type) {
			case string:
				lablog.ShardDebug(kv.gid, kv.me, lablog.Info, "Done %v@%d->%v", op, index, v[:labutil.Min(4, len(v))])
			default:
				lablog.ShardDebug(kv.gid, kv.me, lablog.Info, "Done %v@%d", op, index)
			}
			e = result.Err
			r = result.Value
			return
		case <-time.After(rpcHandlerCheckRaftTermInterval * time.Millisecond):
			t, _ := kv.rf.GetState()
			if term != t {
				e = ErrWrongLeader
				break CheckTermAndWaitReply
			}
		}
	}

	go func() { <-c }() // avoid applier from blocking, and avoid resource leak
	if kv.killed() {
		e = ErrShutdown
	}
	return
}

// Get RPC handler
func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	if kv.killed() {
		reply.Err = ErrShutdown
		return
	}
	shard := key2shard(args.Key)
	// IMPORTANT: lock before rf.Start,
	// to avoid raft finish too quick before kv.commandTbl has set replyCh for this commandIndex
	kv.mu.Lock()
	if kv.config.Num == 0 || kv.config.Shards[shard] != kv.gid {
		// no config fetched, or is not responsible for key's shard
		kv.mu.Unlock()
		reply.Err = ErrWrongGroup
		return
	}
	e, r := kv.commonHandler(args.ConfigNum, Op{Payload: *args})
	reply.Err = e
	if e == OK {
		reply.Value = r.(string)
	}
}

// PutAppend RPC handler
func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	if kv.killed() {
		reply.Err = ErrShutdown
		return
	}
	shard := key2shard(args.Key)
	kv.mu.Lock()
	if kv.config.Num == 0 || kv.config.Shards[shard] != kv.gid {
		// no config fetched, or is not responsible for key's shard
		kv.mu.Unlock()
		reply.Err = ErrWrongGroup
		return
	}
	reply.Err, _ = kv.commonHandler(args.ConfigNum, Op{Payload: *args})
}

/********************************* Migrate Out ********************************/

// every time shard config changed, create a migrateTodo of a group,
// and append this todo to group's migrateOut
type migrateTodo struct {
	ConfigNum int             // configNum when this migrateTodo is created
	Shards    map[int]kvTable // map of shard->shard's kv table
}

type migrateTodoQueue []migrateTodo

func (todos migrateTodoQueue) String() string {
	r := ""
	for _, t := range todos {
		r += fmt.Sprintf(" C%d:[", t.ConfigNum)
		shardIds := []int{}
		for s := range t.Shards {
			shardIds = append(shardIds, s)
		}
		sort.Ints(shardIds)
		for s := range shardIds {
			r += strconv.Itoa(s)
		}
	}
	return r
}

// after shard config updated, migrate-out data from my group to other group
type migrateOut struct {
	Leader  int              // this group's known leader
	Trigger chan bool        // trigger signal to call MigrateShards RPC to this group
	Todos   migrateTodoQueue // FIFO queue of all pending migration
}

// create a migrateOut for this group,
// also start shardsMigrator goroutine for this group
func (kv *ShardKV) createMigrateOut(gid int) (out *migrateOut) {
	out = &migrateOut{Trigger: make(chan bool, 1), Todos: migrateTodoQueue{}}
	kv.MigrateOut[gid] = out
	// kick off this target group's shardsMigrator
	go kv.shardsMigrator(gid, out.Trigger)
	return
}

// after shard config updated, compare oldConfig with newly updated config,
// add a new migrateTodo of shards data of each group that needs to migrate out from my group,
// with mutex held
func (kv *ShardKV) updateMigrateOut(oldConfig shardctrler.Config) {
	if oldConfig.Num <= 0 {
		return
	}

	// map of gid->migrateTodo of new shard config
	byGroup := make(map[int]*migrateTodo)
	for shard, oldGid := range oldConfig.Shards {
		if newGid := kv.config.Shards[shard]; oldGid == kv.gid && newGid != kv.gid {
			// shard in my group of oldConfig, but not in my group of current config, so need to migrate out
			todo, ok := byGroup[newGid]
			if !ok {
				// create migrateTodo for this target group
				todo = &migrateTodo{ConfigNum: kv.config.Num, Shards: make(map[int]kvTable)}
				byGroup[newGid] = todo
			}

			if _, ok := todo.Shards[shard]; !ok {
				// initial kvTable for this shard of this target group
				todo.Shards[shard] = make(kvTable)
			}

			for k, v := range kv.Tbl {
				if key2shard(k) == shard {
					// move into kvTable of this shard of this target group,
					// from my group's whole kvTable
					todo.Shards[shard][k] = v
					// TODO: remove key from kv.Tbl
				}
			}
		}
	}

	// append todo to each group
	for gid, todo := range byGroup {
		out, ok := kv.MigrateOut[gid]
		if !ok {
			out = kv.createMigrateOut(gid)
		}
		out.Todos = append(out.Todos, *todo)
	}
}

// The shardsMigrator go routine act as a long-run goroutine to migrate shards for ONE group,
// it is created when my group try to migrate shards to this group at first time,
// and upon receiving trigger signal, call MigrateShards RPC to this group
func (kv *ShardKV) shardsMigrator(gid int, ch <-chan bool) {
	for !kv.killed() {
		_, ok := <-ch
		if !ok {
			return
		}
		kv.migrateShards(gid)
	}
}

// MigrateShards RPC caller, migrate shards to a group
func (kv *ShardKV) migrateShards(gid int) {
	kv.mu.Lock()
	out, ok := kv.MigrateOut[gid]
	if !ok {
		// migration data not available any more
		kv.mu.Unlock()
		return
	}

	todos := make(migrateTodoQueue, len(out.Todos))
	copy(todos, out.Todos)

	lablog.ShardDebug(kv.gid, kv.me, lablog.Migrate, "Migrate->G%d@%d,%v", gid, kv.config.Num, todos)
	args := &MigrateShardsArgs{
		FromGid:   kv.gid,
		ConfigNum: kv.config.Num,
		Todos:     todos,
		ClientId:  kv.ClientId,
		OpId:      kv.OpId, // opId is fixed for this migration
	}
	kv.OpId++
	kv.mu.Unlock()

	for !kv.killed() {
		_, isLeader := kv.rf.GetState()
		if !isLeader {
			// not leader any more, abort
			return
		}

		kv.mu.Lock()
		servers := kv.config.Groups[gid]
		serverId := kv.MigrateOut[gid].Leader // start from last known leader of target group
		kv.mu.Unlock()

		for i, nServer := 0, len(servers); i < nServer && !kv.killed(); {
			srv := kv.make_end(servers[serverId])
			reply := &MigrateShardsReply{}
			ok := srv.Call("ShardKV.MigrateShards", args, reply)
			if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrShutdown {
				serverId = (serverId + 1) % nServer
				i++
				continue
			}

			kv.mu.Lock()
			kv.MigrateOut[gid].Leader = serverId // remember this leader of target group
			kv.mu.Unlock()
			if reply.Err == ErrUnknownConfig {
				// target server is trying to update shard config, so wait a while and retry this server
				time.Sleep(serverWaitAndRetryInterval * time.Millisecond)
				continue
			}
			if reply.Err == ErrOutdatedConfig {
				// my shard config seems outdated, going to update it and retry later
				select {
				case kv.configFetcherTrigger <- true:
				default:
				}
				time.Sleep(serverWaitAndRetryInterval * time.Millisecond)
				continue
			}
			if reply.Err == OK {
				// migration done, TODO: raft consensus to let group know it and update MigrateOut
				return
			}
		}

		// migration not done in this turn, wait a while and retry this group
		time.Sleep(serverWaitAndRetryInterval * time.Millisecond)
	}
}

// MigrateShards RPC handler
func (kv *ShardKV) MigrateShards(args *MigrateShardsArgs, reply *MigrateShardsReply) {
	if kv.killed() {
		reply.Err = ErrShutdown
		return
	}

	kv.mu.Lock()
	lablog.ShardDebug(kv.gid, kv.me, lablog.Migrate, "My CN:%d, get mig@%d,%v", kv.config.Num, args.ConfigNum, args.Todos)
	reply.Err, _ = kv.commonHandler(args.ConfigNum, Op{Payload: *args})
}

/********************************* Migrate In *********************************/

// after shard config updated, wait-in data from other group to my group
type waitIn struct {
	Leader    int          // this group's known leader
	Trigger   chan bool    // trigger signal to call RequestShards RPC to this group
	ConfigNum int          // last configNum when Shards updated
	Shards    map[int]bool // set of shards which wait migration data in
}

// create a waitIn for this group,
// also start shardsRequester goroutine for this group
func (kv *ShardKV) createWaitIn(gid int) (in *waitIn) {
	in = &waitIn{Trigger: make(chan bool, 1), ConfigNum: kv.config.Num, Shards: make(map[int]bool)}
	kv.WaitIn[gid] = in
	// kick off this target group's shardsRequester
	go kv.shardsRequester(gid, in.Trigger)
	return
}

// after shard config updated, compare oldConfig with newly updated config,
// update set of shards of each group that my group is waiting for migration data in,
// with mutex held
func (kv *ShardKV) updateWaitIn(oldConfig shardctrler.Config) {
	if oldConfig.Num <= 0 {
		return
	}

	for shard, oldGid := range oldConfig.Shards {
		if newGid := kv.config.Shards[shard]; oldGid != kv.gid && newGid == kv.gid {
			// shard not in my group of oldConfig, but in my group of current config,
			// so need to wait migration in

			in, ok := kv.WaitIn[oldGid]
			if !ok {
				in = kv.createWaitIn(oldGid)
			} else if in.ConfigNum < kv.config.Num {
				// update last configNum for this group
				in.ConfigNum = kv.config.Num
			} else {
				// this group's waitIn config is already latest, no need to set waitIn any more
				continue
			}

			// wait this shard's data migrate into my group, from this group
			in.Shards[shard] = true
		}
	}
}

// The shardsRequester go routine act as a long-run goroutine to request shards from ONE group,
// it is created when my group is waiting migration from this group at first time,
// and upon receiving trigger signal, call RequestShards RPC to this group
func (kv *ShardKV) shardsRequester(gid int, ch <-chan bool) {
	for !kv.killed() {
		_, ok := <-ch
		if !ok {
			return
		}
		kv.requestShards(gid)
	}
}

// RequestShards RPC caller, request shards from a group
func (kv *ShardKV) requestShards(gid int) {
	kv.mu.Lock()
	in, ok := kv.WaitIn[gid]
	if !ok {
		// waiting done, no need to request any more
		kv.mu.Unlock()
		return
	}

	shardIds := []int{}
	shards := make(map[int]bool)
	for shard := range in.Shards {
		shardIds = append(shardIds, shard)
		shards[shard] = true
	}
	lablog.ShardDebug(kv.gid, kv.me, lablog.Migrate, "Request->G%d@%d, %v", gid, in.ConfigNum, shardIds)
	args := &RequestShardsArgs{
		ConfigNum: kv.config.Num,
		Shards:    shards,
		ClientId:  kv.ClientId,
		OpId:      kv.OpId,
	}
	kv.OpId++
	kv.mu.Unlock()

	for !kv.killed() {
		_, isLeader := kv.rf.GetState()
		if !isLeader {
			// not leader any more, abort
			return
		}

		kv.mu.Lock()
		servers := kv.config.Groups[gid]
		serverId := kv.WaitIn[gid].Leader // start from last known leader of target group
		kv.mu.Unlock()

		for i, nServer := 0, len(servers); i < nServer && !kv.killed(); {
			srv := kv.make_end(servers[serverId])
			reply := &RequestShardsReply{}
			ok := srv.Call("ShardKV.RequestShards", args, reply)
			if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrShutdown {
				serverId = (serverId + 1) % nServer
				i++
				continue
			}

			kv.mu.Lock()
			kv.WaitIn[gid].Leader = serverId // remember this leader of target group
			kv.mu.Unlock()
			if reply.Err == ErrUnknownConfig {
				// target server is trying to update shard config, so wait a while and retry this server
				time.Sleep(serverWaitAndRetryInterval * time.Millisecond)
				continue
			}
			if reply.Err == ErrOutdatedConfig {
				// my shard config seems outdated, going to update it and retry later
				select {
				case kv.configFetcherTrigger <- true:
				default:
				}
				time.Sleep(serverWaitAndRetryInterval * time.Millisecond)
				continue
			}
			if reply.Err == OK {
				// request done, TODO: raft consensus to let group know it and update WaitIn
				return
			}
		}

		// request not done in this turn, wait a while and retry this group
		time.Sleep(serverWaitAndRetryInterval * time.Millisecond)
	}
}

// RequestShards RPC handler
func (kv *ShardKV) RequestShards(args *RequestShardsArgs, reply *RequestShardsReply) {
	if kv.killed() {
		reply.Err = ErrShutdown
		return
	}

	kv.mu.Lock()
	e, r := kv.commonHandler(args.ConfigNum, Op{Payload: *args})
	reply.Err = e
	if e == OK {
		reply.Shards = r.(map[int]kvTable)
	}
}

/********************************* Migration **********************************/

// install migration's shards data into my kv table, with mutex held
func (kv *ShardKV) installMigration(fromGid int, todos migrateTodoQueue) {
	for _, t := range todos {
		for shard, shardData := range t.Shards {
			for k, v := range shardData {
				kv.Tbl[k] = v
			}

			in, ok := kv.WaitIn[fromGid]
			if !ok {
				in = kv.createWaitIn(fromGid)
			}

			delete(in.Shards, shard)
		}
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
	labgob.Register(GetArgs{})
	labgob.Register(PutAppendArgs{})
	labgob.Register(shardctrler.Config{})
	labgob.Register(MigrateShardsArgs{})
	labgob.Register(RequestShardsArgs{})

	kv := new(ShardKV)
	kv.me = me
	kv.make_end = make_end
	kv.gid = gid
	kv.maxraftstate = float64(maxraftstate)
	applyCh := make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, applyCh)
	kv.sm = shardctrler.MakeClerk(ctrlers)
	kv.configFetcherTrigger = make(chan bool, 1)

	kv.appliedCommandIndex = kv.rf.LastIncludedIndex
	kv.commandTbl = make(map[int]commandEntry)
	kv.Tbl = make(kvTable)
	kv.ClientTbl = make(map[int64]applyResult)
	kv.ClientId = labutil.Nrand()
	kv.OpId = 1
	kv.MigrateOut = make(map[int]*migrateOut)
	kv.WaitIn = make(map[int]*waitIn)

	// initialize from snapshot persisted before a crash
	kv.readSnapshot(persister.ReadSnapshot())

	// communication between applier and snapshoter,
	// let applier trigger snapshoter to take a snapshot when certain amount of msgs have been applied
	snapshotTrigger := make(chan bool, 1)

	go kv.applier(applyCh, snapshotTrigger, kv.appliedCommandIndex)

	kv.configFetcherTrigger <- true // trigger very first time config fetch
	go kv.configFetcher(kv.configFetcherTrigger)

	go kv.snapshoter(persister, snapshotTrigger)

	return kv
}

// update my group's shard config, (it's the ONLY place where shard config can be updated)
// and if leader, try to migrate shards out and request shards in, if any,
// with mutex held
func (kv *ShardKV) installConfig(config shardctrler.Config) {
	if config.Num <= kv.config.Num {
		// outdated config, ignore it
		return
	}

	oldConfig := kv.config
	kv.config = config

	kv.updateMigrateOut(oldConfig)
	kv.updateWaitIn(oldConfig)

	_, isLeader := kv.rf.GetState()
	if !isLeader {
		// only leader can migrate any shards data out, or request any shards data in
		return
	}

	for _, out := range kv.MigrateOut {
		go func(ch chan<- bool) {
			select {
			case ch <- true:
			default:
			}
		}(out.Trigger)
	}
	for _, in := range kv.WaitIn {
		go func(ch chan<- bool) {
			select {
			case ch <- true:
			default:
			}
		}(in.Trigger)
	}
}

// The applier go routine accept applyMsg from applyCh (from underlying raft),
// modify key-value table accordingly,
// reply modified result back to KVServer's RPC handler, if any, through channel identified by commandIndex
// after every snapshoterAppliedMsgInterval msgs, trigger a snapshot
func (kv *ShardKV) applier(applyCh <-chan raft.ApplyMsg, snapshotTrigger chan<- bool, lastSnapshoterTriggeredCommandIndex int) {
	var r interface{}
	var e Err
	var clientId int64
	var opId int

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

		if config, ok := op.Payload.(shardctrler.Config); ok {
			kv.installConfig(config)
			kv.mu.Unlock()
			continue
		}

		clientId, opId = op.getClientIdAndOpId()

		lastOpResult := kv.ClientTbl[clientId]
		if lastOpResult.OpId >= opId {
			// detect duplicated operation
			// reply with cached result, don't update kv table
			r, e = lastOpResult.Value, lastOpResult.Err
		} else {
			switch payload := op.Payload.(type) {
			case GetArgs:
				r, e = kv.Tbl[payload.Key], OK
			case PutAppendArgs:
				if payload.Op == opPut {
					kv.Tbl[payload.Key] = payload.Value
				} else {
					kv.Tbl[payload.Key] += payload.Value
				}
				r, e = nil, OK
			case MigrateShardsArgs:
				if kv.config.Num > payload.ConfigNum {
					// other group start migrate shards to my group,
					// but when this request reach applier at this point,
					// my group's shard config has been updated,
					// and so the request's migration is outdated, reply the same error
					r, e = nil, ErrOutdatedConfig
				} else {
					// migration request accepted, install shards' data from this migration
					kv.installMigration(payload.FromGid, payload.Todos)
					r, e = nil, OK
				}
			case RequestShardsArgs:
				if kv.config.Num > payload.ConfigNum {
					r, e = nil, ErrOutdatedConfig
				} else {
					// TODO: fetch requested shards' data
					r, e = make(map[int]kvTable), OK
				}
			}

			// cache operation result
			kv.ClientTbl[clientId] = applyResult{Err: e, Value: r, OpId: opId}
		}

		ce, ok := kv.commandTbl[m.CommandIndex]
		if ok {
			delete(kv.commandTbl, m.CommandIndex) // delete won't-use reply channel
		}
		kv.mu.Unlock()

		// only leader server maintains commandTbl, followers just apply kv modification
		if ok {
			if ceClientId, ceOpId := ce.op.getClientIdAndOpId(); ceClientId != clientId || ceOpId != opId {
				// Your solution needs to handle a leader that has called Start() for a Clerk's RPC,
				// but loses its leadership before the request is committed to the log.
				// In this case you should arrange for the Clerk to re-send the request to other servers
				// until it finds the new leader.
				//
				// One way to do this is for the server to detect that it has lost leadership,
				// by noticing that a different request has appeared at the index returned by Start()
				ce.replyCh <- applyResult{Err: ErrWrongLeader}
			} else {
				ce.replyCh <- applyResult{Err: e, Value: r}
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

// The configFetcher go routine periodically poll the shardctrler,
// to learn about new configurations
func (kv *ShardKV) configFetcher(trigger <-chan bool) {
	for !kv.killed() {
		// get triggered by immediate fetch request,
		// or fetch periodically
		select {
		case _, ok := <-trigger:
			if !ok {
				return
			}
		case <-time.After(serverRefreshConfigInterval * time.Millisecond):
		}

		_, isLeader := kv.rf.GetState()
		if !isLeader {
			// only leader should update shard config
			continue
		}

		config := kv.sm.Query(-1)
		if config.Num >= 0 { // config.Num maybe < 0 when shardctrler not respond for a while
			kv.mu.Lock()
			if config.Num > kv.config.Num {
				// fetch newer config, start Raft consensus to reach agreement and tell all followers
				lablog.ShardDebug(kv.gid, kv.me, lablog.Ctrler, "To update config %d>%d", config.Num, kv.config.Num)
				kv.rf.Start(Op{Payload: config})
			}
			kv.mu.Unlock()
		}
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
				lablog.ShardDebug(kv.gid, kv.me, lablog.Error, "Write snapshot failed")
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

// get KVServer instance state to be snapshotted, with mutex held
func (kv *ShardKV) kvServerSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(kv.Tbl) != nil ||
		e.Encode(kv.ClientTbl) != nil ||
		e.Encode(kv.ClientId) != nil ||
		e.Encode(kv.OpId) != nil ||
		e.Encode(kv.MigrateOut) != nil ||
		e.Encode(kv.WaitIn) != nil {
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
	var tbl kvTable
	var clientTbl map[int64]applyResult
	var clientId int64
	var opId int
	var migrateOut map[int]*migrateOut
	var waitIn map[int]*waitIn
	if d.Decode(&tbl) != nil ||
		d.Decode(&clientTbl) != nil ||
		d.Decode(&clientId) != nil ||
		d.Decode(&opId) != nil ||
		d.Decode(&migrateOut) != nil ||
		d.Decode(&waitIn) != nil {
		lablog.ShardDebug(kv.gid, kv.me, lablog.Error, "Read broken snapshot")
		return
	}
	kv.Tbl = tbl
	kv.ClientTbl = clientTbl
	kv.ClientId = clientId
	kv.OpId = opId
	kv.MigrateOut = migrateOut
	kv.WaitIn = waitIn
}
