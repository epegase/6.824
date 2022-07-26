package shardkv

import (
	"bytes"
	"fmt"
	"sort"
	"strconv"
	"strings"
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

// shardDate
type shardData struct {
	Gid       int     // resident group
	ConfigNum int     // of which config should this shard in Gid group
	Data      kvTable // this shard's data
}

// shardDate copier
func (s shardData) Copy() shardData {
	t := shardData{Gid: s.Gid, ConfigNum: s.ConfigNum, Data: kvTable{}}
	s.Data.copyInto(t.Data)
	return t
}

// set
type set map[int]bool

// set equality test
func (s set) Equal(another set) bool {
	if len(s) != len(another) {
		return false
	}
	for e := range s {
		if !another[e] {
			return false
		}
	}
	return true
}

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

// shards, key by shard
type shards map[int]shardData

// set of all shards
func (s shards) toSet() set {
	set := set{}
	for shard := range s {
		set[shard] = true
	}
	return set
}

// shards stringer
func (s shards) String() string {
	return s.toSet().String()
}

// shards group by gid
func (s shards) ByGroup() string {
	group := make(map[int]set)
	for shard, data := range s {
		if _, ok := group[data.Gid]; !ok {
			group[data.Gid] = set{}
		}
		group[data.Gid][shard] = true
	}

	r := []string{}
	for gid, s := range group {
		r = append(r, fmt.Sprintf("G%d %v", gid, s))
	}
	return strings.Join(r, "/")
}

// info of a group
type groupInfo struct {
	Leader           int       // leader of the group, send MigrateShards RPC start from leader
	Servers          []string  // servers of the group
	MigrationTrigger chan bool // trigger MigrateShards RPC call for this group
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

	// need to persist between restart
	Tbl        shards                // key-value table by shard
	ClientTbl  map[int64]applyResult // map from clientId to last RPC operation result (for duplicated operation detection)
	ClientId   int64                 // when migrate shards data to other group, act as a ShardKV client, so need a ClientId
	OpId       int                   // also for duplicated migration detection, need an OpId
	Config     shardctrler.Config    // latest known shard config
	Cluster    map[int]*groupInfo    // map of gid -> group info
	MigrateOut shards                // shards that need to migrate out
	WaitIn     shards                // shards that wait migration in
}

// test if my group should serve shard, with mutex held
func (kv *ShardKV) shouldServeShard(shard int) bool {
	return kv.Config.Shards[shard] == kv.gid
}

// test if shard is in my group's WaitIn, with mutex held
func (kv *ShardKV) isInWaitIn(shard int) bool {
	_, ok := kv.WaitIn[shard]
	return ok
}

// common logic for RPC handler
//
// 1. check not killed
// 2. check is leader
// 3. check request's configNum, and trigger config fetcher if necessary
// 4. if is request of Get or PutAppend RPC, check if the resident shard can be served
// 5. start Raft consensus
// 6. wait for agreement result from applier goroutine
// 7. return with RPC reply result
func (kv *ShardKV) commonHandler(op Op) (e Err, r interface{}) {
	defer func() {
		if e != OK {
			lablog.ShardDebug(kv.gid, kv.me, lablog.Error, "%s %v", string(e), op)
		}
	}()

	if kv.killed() {
		e = ErrShutdown
		return
	}

	if _, isLeader := kv.rf.GetState(); !isLeader {
		// only leader should take responsibility to check request's configNum, then start Raft consensus
		e = ErrWrongLeader
		return
	}

	requestConfigNum, shard := op.getConfigNumAndShard()

	// IMPORTANT: lock before rf.Start,
	// to avoid raft finish too quick before kv.commandTbl has set replyCh for this commandIndex
	kv.mu.Lock()

	switch op.Payload.(type) {
	case MigrateShardsArgs:
		if kv.Config.Num < requestConfigNum {
			// my shard config seems outdated, tell configFetcher to update
			kv.triggerConfigFetch()
		}
	default:
		if kv.Config.Num < requestConfigNum {
			// my shard config seems outdated, tell configFetcher to update
			kv.mu.Unlock()
			kv.triggerConfigFetch()
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
		if kv.Config.Num == 0 || !kv.shouldServeShard(shard) {
			// no config fetched, or is not responsible for key's shard
			kv.mu.Unlock()
			e = ErrWrongGroup
			return
		}
		if kv.isInWaitIn(shard) {
			// key's shard is in process of migration
			kv.mu.Unlock()
			e = ErrInMigration
			return
		}
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
			if t, _ := kv.rf.GetState(); term != t {
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

// trigger a MigrateShards RPC call to gid, with mutex held
func (kv *ShardKV) triggerMigration(gid int) {
	go func(trigger chan<- bool) {
		select {
		case trigger <- true:
		default:
		}
	}(kv.Cluster[gid].MigrationTrigger)
}

// trigger migration for each group, with mutex held
func (kv *ShardKV) triggerMigrationForEachGroup() {
	groupSet := set{}
	for _, out := range kv.MigrateOut {
		groupSet[out.Gid] = true
	}
	for gid := range groupSet {
		kv.triggerMigration(gid)
	}
}

// The shardsMigrator go routine act as a long-run goroutine to migrate shards for ONE group,
// it is created when my group try to migrate shards to this group at first time,
// and upon receiving trigger signal, call MigrateShards RPC to this group
func (kv *ShardKV) shardsMigrator(gid int, trigger <-chan bool) {
	for !kv.killed() {
		if _, ok := <-trigger; !ok {
			return
		}
		kv.migrateShards(gid)
	}
}

// MigrateShards RPC caller, migrate shards to a group
func (kv *ShardKV) migrateShards(gid int) {
	args := &MigrateShardsArgs{
		FromGid:  kv.gid,
		ClientId: kv.ClientId,
	}

	for !kv.killed() {
		if _, isLeader := kv.rf.GetState(); !isLeader {
			return // not leader, abort
		}

		// in each turn, set new shards that need to migrate out, in case of concurrent MigrateOut update
		args.Shards = shards{}
		kv.mu.Lock()
		for shard, out := range kv.MigrateOut {
			if in, ok := kv.WaitIn[shard]; out.Gid == gid && (!ok || out.ConfigNum < in.ConfigNum) {
				// gid is migrate-out shard's target,
				// and this shard is not in my group's WaitIn,
				// or if is in my group's WaitIn, this wait-in shard is waited in the future,
				// after it migrates out from my group,
				// so this shard still needs to migrate out first, then my group waits it back in
				args.Shards[shard] = out.Copy()
			}
		}
		if len(args.Shards) == 0 {
			// no shards need to migrate out, migration is done in this turn
			kv.mu.Unlock()
			return
		}

		args.ConfigNum = kv.Config.Num // update args.ConfigNum to reflect my group's current configNum
		args.OpId = kv.OpId
		kv.OpId++

		servers := kv.Cluster[gid].Servers
		serverId := kv.Cluster[gid].Leader // start from last known leader of target group
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
			kv.Cluster[gid].Leader = serverId // remember this leader of target group
			kv.mu.Unlock()
			if reply.Err == ErrUnknownConfig {
				// target server is trying to update shard config, so wait a while and retry in a new turn
				break
			}
			if reply.Err == ErrOutdatedConfig {
				// my shard config seems outdated, going to update it and retry later in a new turn
				kv.triggerConfigFetch()
				break
			}
			if reply.Err == OK {
				r := "OK"
				if !args.Shards.toSet().Equal(reply.Installed) {
					r = fmt.Sprintf("I%v", reply.Installed)
				}
				lablog.ShardDebug(kv.gid, kv.me, lablog.Info, "CM->G%d %v, %s", gid, args.Shards, r)

				// migration done, start raft consensus to let group know it and update MigrateOut
				reply.Accepted = args.Shards.toSet()
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

	kv.Tbl = shards{}
	kv.ClientTbl = make(map[int64]applyResult)
	kv.ClientId = labutil.Nrand()
	kv.OpId = 1
	kv.Config = shardctrler.Config{Num: 0}
	kv.Cluster = make(map[int]*groupInfo)
	kv.MigrateOut = shards{}
	kv.WaitIn = shards{}

	// initialize from snapshot persisted before a crash
	kv.readSnapshot(persister.ReadSnapshot())

	// create MigrationTrigger for each group
	for gid, group := range kv.Cluster {
		group.MigrationTrigger = make(chan bool, 1)
		go kv.shardsMigrator(gid, group.MigrationTrigger)
	}
	// trigger any MigrateOut
	kv.triggerMigrationForEachGroup()

	// communication between applier and snapshoter,
	// let applier trigger snapshoter to take a snapshot when certain amount of msgs have been applied
	snapshotTrigger := make(chan bool, 1)

	go kv.applier(applyCh, snapshotTrigger, kv.appliedCommandIndex)

	go kv.configFetcher(kv.configFetcherTrigger)
	kv.triggerConfigFetch() // trigger very first time config fetch

	go kv.snapshoter(persister, snapshotTrigger)

	return kv
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

			// move this shard into migration, and remove from my group's shards
			kv.MigrateOut[shard] = shardData{Gid: newGid, ConfigNum: kv.Config.Num, Data: kv.Tbl[shard].Data}
			delete(kv.Tbl, shard)
		}
	}

	if _, isLeader := kv.rf.GetState(); isLeader {
		lablog.ShardDebug(kv.gid, kv.me, lablog.Trace, "U MO %v", kv.MigrateOut.ByGroup())
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

			if !kv.isInWaitIn(shard) {
				// shard has not reached in prior, so my group need to wait this shard
				kv.WaitIn[shard] = shardData{Gid: newGid, ConfigNum: kv.Config.Num, Data: nil}
			} else if in, ok := kv.WaitIn[shard]; ok && in.ConfigNum == kv.Config.Num {
				// shard has reached before my group is aware of the shard config change,
				// so when my group know this shard config change,
				// my group can happily accept this shard and install into my kvTable
				kv.Tbl[shard] = shardData{Gid: kv.gid, ConfigNum: kv.Config.Num, Data: in.Copy().Data}
				delete(kv.WaitIn, shard)
			}
		}
	}

	if _, isLeader := kv.rf.GetState(); isLeader {
		lablog.ShardDebug(kv.gid, kv.me, lablog.Trace, "U WI %v", kv.WaitIn.ByGroup())
	}
}

func (kv *ShardKV) updateCluster() {
	for gid, servers := range kv.Config.Groups {
		if _, ok := kv.Cluster[gid]; !ok {
			// create group info, and kick off shardsMigrator for this group
			kv.Cluster[gid] = &groupInfo{Leader: 0, Servers: nil, MigrationTrigger: make(chan bool, 1)}
			go kv.shardsMigrator(gid, kv.Cluster[gid].MigrationTrigger)
		}
		kv.Cluster[gid].Servers = servers
	}
	// TODO: update ownership of each shard
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
	kv.updateCluster()

	if _, isLeader := kv.rf.GetState(); !isLeader {
		// only leader can migrate any shards out
		return
	}

	lablog.ShardDebug(kv.gid, kv.me, lablog.Ctrler, "Ins C%d->%d", oldConfig.Num, config.Num)

	// trigger fetcher to see if any newer config, in order to update my shard config ASAP
	kv.triggerConfigFetch()
	// trigger any MigrateOut
	kv.triggerMigrationForEachGroup()
}

// apply Get RPC request, with mutex held
func (kv *ShardKV) applyGetArgs(args *GetArgs) (r string, e Err) {
	// TODO: wait in ?
	shard := key2shard(args.Key)
	if !kv.shouldServeShard(shard) {
		return "", ErrWrongGroup
	}
	return kv.Tbl[shard].Data[args.Key], OK
}

// apply PutAppend RPC request, with mutex held
func (kv *ShardKV) applyPutAppendArgs(args *PutAppendArgs) (r interface{}, e Err) {
	// TODO: wait in ?
	shard := key2shard(args.Key)
	if !kv.shouldServeShard(shard) {
		return nil, ErrWrongGroup
	}
	if _, ok := kv.Tbl[shard]; !ok {
		kv.Tbl[shard] = shardData{Gid: kv.gid, ConfigNum: kv.Config.Num, Data: kvTable{}}
	}
	if args.Op == opPut {
		kv.Tbl[shard].Data[args.Key] = args.Value
	} else {
		kv.Tbl[shard].Data[args.Key] += args.Value
	}
	return nil, OK
}

// migration request accepted, install shards' data from this migration, with mutex held
func (kv *ShardKV) applyMigrateShardsArgs(args *MigrateShardsArgs) (r set, e Err) {
	e = OK
	// reply to caller with set of installed shards
	r = set{}
	// groups that need to trigger migration
	migrateOutGroups := set{}

	for shard, migration := range args.Shards {
		if migration.ConfigNum > kv.Config.Num {
			// migration shard reaches before my group is aware of the shard config change,
			// and my group trusts the migration source group,
			// believing this migration shard will be needed in the future config,
			// so accept this migration shard, store it into my WaitIn
			kv.WaitIn[shard] = migration
		}

		if in, ok := kv.WaitIn[shard]; ok && migration.ConfigNum == in.ConfigNum {
			// migration shard matches the same shard in my group's WaitIn,
			// they are of the same shard config, so my group should accept this migration shard

			if out, ok := kv.MigrateOut[shard]; ok && out.ConfigNum > in.ConfigNum {
				// this migration shard need to migrate out, so no need to install into my group's shards,
				// instead, move this migration shard to MigrateOut
				kv.MigrateOut[shard] = shardData{Gid: out.Gid, ConfigNum: out.ConfigNum, Data: migration.Data}
				migrateOutGroups[out.Gid] = true
				delete(kv.WaitIn, shard) // remove shard from my group's WaitIn
				r[shard] = true          // tell caller this shard is installed to my group
			} else if kv.shouldServeShard(shard) {
				// my group should serve request for this shard, so install shard into my group's kvTable
				kv.Tbl[shard] = shardData{Gid: kv.gid, ConfigNum: kv.Config.Num, Data: migration.Copy().Data}
				delete(kv.WaitIn, shard) // remove shard from my group's WaitIn
				r[shard] = true          // tell caller this shard is installed to my group
			}
		}
	}

	if _, isLeader := kv.rf.GetState(); !isLeader {
		return
	}
	lablog.ShardDebug(kv.gid, kv.me, lablog.Trace, "A WI %v", kv.WaitIn.ByGroup())

	for gid := range migrateOutGroups {
		kv.triggerMigration(gid)
	}
	return
}

// migration done, remove shard from my group's MigrateOut, with mutex held
func (kv *ShardKV) applyMigrateShardsReply(reply *MigrateShardsReply) {
	for shard, out := range kv.MigrateOut {
		if reply.FromGid == out.Gid && reply.Accepted[shard] {
			// migration target group accepted this shard, so remove from my group's MigrateOut
			delete(kv.MigrateOut, shard)
		}
	}

	if _, isLeader := kv.rf.GetState(); isLeader {
		lablog.ShardDebug(kv.gid, kv.me, lablog.Trace, "R MO %v", kv.MigrateOut.ByGroup())
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

		if lastOpResult, ok := kv.ClientTbl[clientId]; ok && lastOpResult.OpId >= opId {
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
				switch result := r.(type) {
				case string: // Get
				case nil: // PutAppend
				case set: // MigrateShards
					lablog.ShardDebug(kv.gid, kv.me, lablog.Migrate, "Done %v@%d %v", op, m.CommandIndex, result)
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
	for _, groupInfo := range kv.Cluster {
		if groupInfo.MigrationTrigger != nil {
			close(groupInfo.MigrationTrigger)
		}
	}
}

// trigger configFetcher
func (kv *ShardKV) triggerConfigFetch() {
	select {
	case kv.configFetcherTrigger <- true:
	default:
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

		if _, isLeader := kv.rf.GetState(); !isLeader {
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
		e.Encode(kv.Cluster) != nil ||
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
	var tbl shards
	var clientTbl map[int64]applyResult
	var clientId int64
	var opId int
	var config shardctrler.Config
	var cluster map[int]*groupInfo
	var migrateOut shards
	var waitIn shards
	if d.Decode(&tbl) != nil ||
		d.Decode(&clientTbl) != nil ||
		d.Decode(&clientId) != nil ||
		d.Decode(&opId) != nil ||
		d.Decode(&config) != nil ||
		d.Decode(&cluster) != nil ||
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
	kv.Cluster = cluster
	kv.MigrateOut = migrateOut
	kv.WaitIn = waitIn
}
