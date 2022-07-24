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

// Op stringer
func (op Op) String() string {
	switch payload := op.Payload.(type) {
	case GetArgs:
		return fmt.Sprintf("{G%sS%d}", payload.Key, key2shard(payload.Key))
	case PutAppendArgs:
		if payload.Op == opPut {
			return fmt.Sprintf("{P%sS%d}", payload.Key, key2shard(payload.Key))
		}
		return fmt.Sprintf("{A%sS%d}", payload.Key, key2shard(payload.Key))
	case MigrateShardsArgs:
		return fmt.Sprintf("{MG%d %v}", payload.FromGid, payload.Shards)
	default:
		return ""
	}
}

func (op *Op) getClientIdAndOpId() (clientId int64, opId int) {
	switch payload := op.Payload.(type) {
	case GetArgs:
		return payload.ClientId, payload.OpId
	case PutAppendArgs:
		return payload.ClientId, payload.OpId
	case MigrateShardsArgs:
		return payload.ClientId, payload.OpId
	}
	return
}

func (op *Op) getConfigNumAndShard() (configNum int, shard int) {
	switch payload := op.Payload.(type) {
	case GetArgs:
		return payload.ConfigNum, key2shard(payload.Key)
	case PutAppendArgs:
		return payload.ConfigNum, key2shard(payload.Key)
	case MigrateShardsArgs:
		return payload.ConfigNum, -1
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

// copy my kvTable into dst kvTable, dst CANNOT be nil
func (src kvTable) copyInto(dst kvTable) {
	for k, v := range src {
		dst[k] = v
	}
}

// set
type set map[int]bool

// set stringer
func (s set) String() (r string) {
	elems := []int{}
	for e := range s {
		elems = append(elems, e)
	}
	sort.Ints(elems)
	for _, e := range elems {
		r += strconv.Itoa(e)
	}
	return "[" + r + "]"
}

// shard's kvTable, key by shard
type kvTableByShard map[int]kvTable

// kvTableByShard stringer
func (byShard kvTableByShard) String() string {
	set := set{}
	for s := range byShard {
		set[s] = true
	}
	return set.String()
}

// other group's info
type groupInfo struct {
	leader           int       // leader of the group, send MigrateShards RPC start from leader
	migrationTrigger chan bool // trigger MigrateShards RPC call for this group
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	sm           *shardctrler.Clerk // shard manager
	dead         int32              // set by Kill()
	make_end     func(string) *labrpc.ClientEnd
	gid          int     // my group id
	maxraftstate float64 // snapshot if log grows this big

	commandTbl           map[int]commandEntry // map from commandIndex to commandEntry, maintained by leader, initialized to empty when restart
	appliedCommandIndex  int                  // last applied commandIndex from applyCh
	configFetcherTrigger chan bool            // trigger configFetcher to update shard config
	cluster              map[int]*groupInfo   // map of gid -> group info

	// need to persist between restart
	Tbl        kvTable               // key-value table
	ClientTbl  map[int64]applyResult // map from clientId to last RPC operation result (for duplicated operation detection)
	ClientId   int64                 // when migrate shards data to other group, act as a ShardKV client, so need a ClientId
	OpId       int                   // also for duplicated migration detection, need an OpId
	Config     shardctrler.Config    // latest known shard config
	MigrateOut map[int]migrateOut    // for each shard, map of shard -> migrateOut{ToGid,Data}
	WaitIn     map[int]int           // for each shard, map of shard -> gid
}

// test if my group should serve shard, with mutex held
func (kv *ShardKV) shouldServeShard(shard int) bool {
	return kv.Config.Shards[shard] == kv.gid
}

// test if shard's data is in the process of migration from other group, with mutex held
func (kv *ShardKV) isInMigration(shard int) bool {
	_, ok := kv.WaitIn[shard]
	return ok
}

// common logic for RPC handler
//
// 1. check not killed
// 2. check is leader
// 3. check request's configNum
// 4. if is request for a shard, check if can be served
// 5. start Raft consensus
// 6. wait for agreement result from applier goroutine
// 7. return with RPC reply result
func (kv *ShardKV) commonHandler(op Op) (e Err, r interface{}) {
	if kv.killed() {
		e = ErrShutdown
		return
	}

	_, isLeader := kv.rf.GetState()
	if !isLeader {
		// only leader should take responsibility to check request's configNum, then start Raft consensus
		e = ErrWrongLeader
		return
	}

	requestConfigNum, shard := op.getConfigNumAndShard()

	// IMPORTANT: lock before rf.Start,
	// to avoid raft finish too quick before kv.commandTbl has set replyCh for this commandIndex
	kv.mu.Lock()

	if args, ok := op.Payload.(MigrateShardsArgs); ok {
		lablog.ShardDebug(kv.gid, kv.me, lablog.Log2, "C%d HMG%d-> %v", kv.Config.Num, args.FromGid, args.Shards)
	}
	if kv.Config.Num < requestConfigNum {
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
	if kv.Config.Num > requestConfigNum {
		// request's config is outdated, abort request, and tell client to update its shard config
		kv.mu.Unlock()
		e = ErrOutdatedConfig
		return
	}

	if kv.Config.Num == 0 || (shard >= 0 && !kv.shouldServeShard(shard)) {
		// no config fetched, or is not responsible for key's shard
		kv.mu.Unlock()
		e = ErrWrongGroup
		return
	}
	if shard >= 0 && kv.isInMigration(shard) {
		// key's shard is in process of migration
		kv.mu.Unlock()
		e = ErrInMigration
		return
	}

	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		kv.mu.Unlock()
		e = ErrWrongLeader
		return
	}

	// clientId, opId := op.getClientIdAndOpId()
	//* lablog.ShardDebug(kv.gid, kv.me, lablog.Server, "Start %v@%d, %d/%d", op, index, opId, clientId%100)

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
			e, r = result.Err, result.Value
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
	e, r := kv.commonHandler(Op{Payload: *args})
	reply.Err = e
	if e == OK {
		reply.Value = r.(string)
	}
}

// PutAppend RPC handler
func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	reply.Err, _ = kv.commonHandler(Op{Payload: *args})
}

/********************************* Migration **********************************/

// MigrateShards RPC handler
func (kv *ShardKV) MigrateShards(args *MigrateShardsArgs, reply *MigrateShardsReply) {
	e, r := kv.commonHandler(Op{Payload: *args})
	reply.Err = e
	if e == OK {
		reply.FromGid = kv.gid
		reply.Installed = r.(set)
	}
}

// after shard config updated, migrate-out data from my group to other group
type migrateOut struct {
	ToGid int
	Data  kvTable
}

// after shard config updated, compare oldConfig with newly updated config,
// add shards that need to migrate out from my group,
// with mutex held
func (kv *ShardKV) updateMigrateOut(oldConfig shardctrler.Config) {
	if oldConfig.Num <= 0 {
		return
	}

	for shard, oldGid := range oldConfig.Shards {
		if newGid := kv.Config.Shards[shard]; oldGid == kv.gid && newGid != kv.gid {
			// shard in my group of oldConfig, but not in my group of current config, so need to migrate out
			kv.MigrateOut[shard] = migrateOut{ToGid: newGid, Data: kvTable{}}
		}
	}

	for k, v := range kv.Tbl {
		if out, ok := kv.MigrateOut[key2shard(k)]; ok {
			// move into kvTable of this shard, from my group's kvTable
			out.Data[k] = v
			// TODO: remove key from kv.Tbl
		}
	}
}

// after shard config updated, compare oldConfig with newly updated config,
// add shards that my group is waiting for migration data in,
// with mutex held
func (kv *ShardKV) updateWaitIn(oldConfig shardctrler.Config) {
	if oldConfig.Num <= 0 {
		return
	}

	for shard, oldGid := range oldConfig.Shards {
		if newGid := kv.Config.Shards[shard]; oldGid != kv.gid && newGid == kv.gid {
			// shard not in my group of oldConfig, but in my group of current config, so need to wait migration in
			kv.WaitIn[shard] = oldGid
		}
	}
}

// trigger a MigrateShards RPC call to gid, with mutex held
func (kv *ShardKV) triggerMigration(gid int) {
	group, ok := kv.cluster[gid]
	if !ok {
		// create group info, and kick off shardsMigrator for this group
		group = &groupInfo{leader: 0, migrationTrigger: make(chan bool, 1)}
		kv.cluster[gid] = group
		go kv.shardsMigrator(gid, group.migrationTrigger)
	}

	// trigger migration for this group
	go func(trigger chan<- bool) {
		select {
		case trigger <- true:
		default:
		}
	}(group.migrationTrigger)
}

// The shardsMigrator go routine act as a long-run goroutine to migrate shards for ONE group,
// it is created when my group try to migrate shards to this group at first time,
// and upon receiving trigger signal, call MigrateShards RPC to this group
func (kv *ShardKV) shardsMigrator(gid int, trigger <-chan bool) {
	for !kv.killed() {
		_, ok := <-trigger
		if !ok {
			return
		}
		kv.migrateShards(gid)
	}
}

// MigrateShards RPC caller, migrate shards to a group
func (kv *ShardKV) migrateShards(gid int) {
	kv.mu.Lock()
	shards := kvTableByShard{}
	for shard, out := range kv.MigrateOut {
		if out.ToGid == gid && !kv.isInMigration(shard) {
			shards[shard] = kvTable{}
			out.Data.copyInto(shards[shard]) // TODO: need copy ?
		}
	}
	if len(shards) == 0 {
		kv.mu.Unlock()
		return
	}

	args := &MigrateShardsArgs{
		FromGid:  kv.gid,
		Shards:   shards,
		ClientId: kv.ClientId,
		OpId:     kv.OpId, // opId is fixed for this migration
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
		args.ConfigNum = kv.Config.Num // update args.ConfigNum to reflect my group's current configNum
		servers := kv.Config.Groups[gid]
		serverId := kv.cluster[gid].leader // start from last known leader of target group
		kv.mu.Unlock()

		lablog.ShardDebug(kv.gid, kv.me, lablog.Migrate, "C%d CM->G%d %v", args.ConfigNum, gid, args.Shards)

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
			kv.cluster[gid].leader = serverId // remember this leader of target group
			kv.mu.Unlock()
			if reply.Err == ErrUnknownConfig {
				// target server is trying to update shard config, so wait a while and retry this server
				time.Sleep(serverWaitAndRetryInterval * time.Millisecond)
				continue
			}
			if reply.Err == ErrOutdatedConfig {
				// my shard config seems outdated, going to update it and retry later in a new turn
				select {
				case kv.configFetcherTrigger <- true:
				default:
				}
				break
			}
			if reply.Err == OK {
				// migration done, start raft consensus to let group know it and update MigrateOut
				lablog.ShardDebug(kv.gid, kv.me, lablog.Info, "OK CM->G%d %v", gid, args.Shards)
				kv.rf.Start(Op{Payload: *reply})
				return
			}
		}

		// migration not done in this turn, wait a while and retry this group
		time.Sleep(serverWaitAndRetryInterval * time.Millisecond)
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
	labgob.Register(shardctrler.Config{})
	labgob.Register(Op{})
	labgob.Register(GetArgs{})
	labgob.Register(PutAppendArgs{})
	labgob.Register(MigrateShardsArgs{})
	labgob.Register(MigrateShardsReply{})
	labgob.Register(set{})

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
	kv.configFetcherTrigger = make(chan bool, 1)
	kv.cluster = make(map[int]*groupInfo)

	kv.Tbl = kvTable{}
	kv.ClientTbl = make(map[int64]applyResult)
	kv.ClientId = labutil.Nrand()
	kv.OpId = 1
	kv.Config = shardctrler.Config{Num: 0}
	kv.MigrateOut = make(map[int]migrateOut)
	kv.WaitIn = make(map[int]int)

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
// and if leader, try to migrate shards out,
// with mutex held
func (kv *ShardKV) installConfig(config shardctrler.Config) {
	if config.Num <= kv.Config.Num {
		// outdated config, ignore it
		return
	}

	oldConfig := kv.Config
	kv.Config = config

	kv.updateMigrateOut(oldConfig)
	kv.updateWaitIn(oldConfig)

	_, isLeader := kv.rf.GetState()
	if !isLeader {
		// only leader can migrate any shards out
		return
	}

	lablog.ShardDebug(kv.gid, kv.me, lablog.Ctrler, "Ins C%d->%d", oldConfig.Num, config.Num)

	// trigger fetcher to see if any newer config, in order to update my shard config ASAP
	select {
	case kv.configFetcherTrigger <- true:
	default:
	}

	groupSet := set{}
	for _, out := range kv.MigrateOut {
		groupSet[out.ToGid] = true
	}
	for gid := range groupSet {
		kv.triggerMigration(gid)
	}
}

// apply Get RPC request, with mutex held
func (kv *ShardKV) applyGetArgs(args *GetArgs) (r interface{}, e Err) {
	if !kv.shouldServeShard(key2shard(args.Key)) {
		return nil, ErrWrongGroup
	}
	return kv.Tbl[args.Key], OK
}

// apply PutAppend RPC request, with mutex held
func (kv *ShardKV) applyPutAppendArgs(args *PutAppendArgs) (r interface{}, e Err) {
	if !kv.shouldServeShard(key2shard(args.Key)) {
		return nil, ErrWrongGroup
	}
	if args.Op == opPut {
		kv.Tbl[args.Key] = args.Value
	} else {
		kv.Tbl[args.Key] += args.Value
	}
	return nil, OK
}

// migration request accepted, install shards' data from this migration, with mutex held
func (kv *ShardKV) applyMigrateShardsArgs(args *MigrateShardsArgs) (r interface{}, e Err) {
	// reply to caller with set of installed shards
	installed := set{}
	// groups that need to trigger migration
	groupSet := set{}

	for shard, shardData := range args.Shards {
		if waitGid, ok := kv.WaitIn[shard]; ok && waitGid == args.FromGid {
			// migration shard is my group's wait and match the wanted group

			if out, ok := kv.MigrateOut[shard]; ok {
				// this shard need to migrate out, so no need to copy into my group's kvTable,
				// instead, copy this shard to migrateOut
				shardData.copyInto(out.Data)
				groupSet[out.ToGid] = true
			} else {
				shardData.copyInto(kv.Tbl) // copy migration data into my group's kvTable
			}

			delete(kv.WaitIn, shard) // remove shard from my group's WaitIn
			installed[shard] = true  // tell caller this shard is installed to my group
		}
	}

	for gid := range groupSet {
		kv.triggerMigration(gid)
	}
	return installed, OK
}

// migration done, remove shard from my group's MigrateOut, with mutex held
func (kv *ShardKV) applyMigrateShardsReply(reply *MigrateShardsReply) {
	for shard, out := range kv.MigrateOut {
		if reply.FromGid == out.ToGid && reply.Installed[shard] {
			delete(kv.MigrateOut, shard)
		}
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

		// internal Raft consensus, no need to reply to client
		switch payload := op.Payload.(type) {
		case shardctrler.Config:
			kv.installConfig(payload)
			kv.mu.Unlock()
			continue
		case MigrateShardsReply:
			kv.applyMigrateShardsReply(&payload)
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
				r, e = kv.applyGetArgs(&payload)
			case PutAppendArgs:
				r, e = kv.applyPutAppendArgs(&payload)
			case MigrateShardsArgs:
				r, e = kv.applyMigrateShardsArgs(&payload)
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
				if _, ok := op.Payload.(MigrateShardsArgs); ok {
					lablog.ShardDebug(kv.gid, kv.me, lablog.Migrate, "Done %v@%d", op, m.CommandIndex)
				}
				ce.replyCh <- applyResult{Err: e, Value: r}
			}
		}
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()
	// close all pending RPC handler reply channel to avoid goroutine resource leak
	for _, ce := range kv.commandTbl {
		close(ce.replyCh)
	}
	// close trigger of each group to avoid goroutine resource leak
	for _, groupInfo := range kv.cluster {
		close(groupInfo.migrationTrigger)
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

		// IMPORTANT: Process re-configurations one at a time, in order.
		kv.mu.Lock()
		num := kv.Config.Num + 1
		kv.mu.Unlock()

		config := kv.sm.Query(num)

		kv.mu.Lock()
		if config.Num > kv.Config.Num {
			// fetch newer config, start Raft consensus to reach agreement and tell all followers
			kv.rf.Start(Op{Payload: config})
		}
		kv.mu.Unlock()
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
		e.Encode(kv.Config) != nil ||
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
	var config shardctrler.Config
	var migrateOut map[int]migrateOut
	var waitIn map[int]int
	if d.Decode(&tbl) != nil ||
		d.Decode(&clientTbl) != nil ||
		d.Decode(&clientId) != nil ||
		d.Decode(&opId) != nil ||
		d.Decode(&config) != nil ||
		d.Decode(&migrateOut) != nil ||
		d.Decode(&waitIn) != nil {
		lablog.ShardDebug(kv.gid, kv.me, lablog.Error, "Read broken snapshot")
		return
	}
	kv.Tbl = tbl
	kv.ClientTbl = clientTbl
	kv.ClientId = clientId
	kv.OpId = opId
	kv.Config = config
	kv.MigrateOut = migrateOut
	kv.WaitIn = waitIn
}
