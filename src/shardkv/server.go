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
	// migrator periodically check any pending migration and trigger shardsMigrator for that group
	migratorInterval = 100
)

type Op struct {
	Payload interface{}
}

// Op stringer
func (op Op) String() string {
	switch a := op.Payload.(type) {
	case GetArgs:
		return fmt.Sprintf("{G%s %d%s %d#%d}", a.Key, key2shard(a.Key), labutil.ToSubscript(a.ConfigNum), a.ClientId%100, a.OpId)
	case PutAppendArgs:
		if a.Op == opPut {
			return fmt.Sprintf("{P%s=%s %d%s %d#%d}", a.Key, labutil.Suffix(a.Value, 4), key2shard(a.Key), labutil.ToSubscript(a.ConfigNum), a.ClientId%100, a.OpId)
		}
		return fmt.Sprintf("{A%s+=%s %d%s %d#%d}", a.Key, labutil.Suffix(a.Value, 4), key2shard(a.Key), labutil.ToSubscript(a.ConfigNum), a.ClientId%100, a.OpId)
	case MigrateShardsArgs:
		return fmt.Sprintf("{I%s G%d-> %v}", labutil.ToSubscript(a.ConfigNum), a.Gid, a.Shards)
	default:
		return ""
	}
}

// Op equalizer
func (op *Op) Equal(another *Op) bool {
	switch a := op.Payload.(type) {
	case ClerkRequest:
		b, isClerkRequest := another.Payload.(ClerkRequest)
		return isClerkRequest && a.getClientId() == b.getClientId() && a.getOpId() == b.getOpId()
	case MigrateShardsArgs:
		b, isMigrate := another.Payload.(MigrateShardsArgs)
		return isMigrate && a.Gid == b.Gid && a.ConfigNum == b.ConfigNum && a.Shards.toShardSet().Equal(b.Shards.toShardSet())
	default:
		return false
	}
}

// channel message from applier to RPC handler,
type requestResult struct {
	Err   Err
	Value interface{}
}

// reply channel from applier to RPC handler, of a commandIndex
type commandEntry struct {
	op      Op
	replyCh chan requestResult
}

// cached operation result, to respond to duplicated operation
type cache struct {
	OpId   int
	Shard  int
	Result requestResult
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

// shardData stringer
func (s shardData) String() string {
	return fmt.Sprintf("%s @G%d", labutil.ToSubscript(s.ConfigNum), s.Gid)
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

// ordered set
func (s set) toOrdered() []int {
	elems := []int{}
	for e := range s {
		elems = append(elems, e)
	}
	sort.Ints(elems)
	return elems
}

// set stringer
func (s set) String() (r string) {
	for _, e := range s.toOrdered() {
		r += strconv.Itoa(e)
	}
	return "[" + r + "]"
}

// shards, key by shard
type shards map[int]shardData

// set of all shards
func (s shards) toShardSet() (r set) {
	r = set{}
	for shard := range s {
		r[shard] = true
	}
	return
}

// set of all groups
func (s shards) toGroupSet() (r set) {
	r = set{}
	for _, data := range s {
		r[data.Gid] = true
	}
	return
}

// shards stringer
func (s shards) String() string {
	r := []string{}
	for _, shard := range s.toShardSet().toOrdered() {
		r = append(r, fmt.Sprintf("%d%s", shard, labutil.ToSubscript(s[shard].ConfigNum)))
	}
	return "[" + strings.Join(r, " ") + "]"
}

// shards group by gid
func (s shards) ByGroup() string {
	group := make(map[int]shards)
	for shard, data := range s {
		if _, ok := group[data.Gid]; !ok {
			group[data.Gid] = shards{}
		}
		group[data.Gid][shard] = data
	}

	r := []string{}
	for gid, s := range group {
		r = append(r, fmt.Sprintf("G%d%v", gid, s))
	}
	return strings.Join(r, "|")
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
	migrationTrigger     chan bool            // trigger migrator to check any pending migration

	// need to persist between restart
	Tbl       shards          // key-value table by shard
	ClientTbl map[int64]cache // map from clientId to last RPC operation result (for duplicated operation detection)

	Config     shardctrler.Config // latest known shard config
	Cluster    map[int]*groupInfo // map of gid -> group info
	MigrateOut shards             // shards that need to migrate out
	WaitIn     shards             // shards that wait migration in
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

// check if this ClerkRequest is applicable on my group, with mutex held
func (kv *ShardKV) checkClerkRequest(req ClerkRequest) (result requestResult, applicable bool) {
	if lastOpCache, ok := kv.ClientTbl[req.getClientId()]; ok && lastOpCache.Result.Err == OK && lastOpCache.OpId >= req.getOpId() {
		// detect duplicated SUCCESSFUL operation, reply with cached result
		return lastOpCache.Result, false
	}
	if kv.Config.Num > req.getConfigNum() {
		// request's config is outdated, tell client to update its shard config
		return requestResult{Err: ErrOutdatedConfig}, false
	}
	shard := req.getShard()
	if kv.Config.Num == 0 || !kv.shouldServeShard(shard) {
		// no config fetched, or is not responsible for key's shard
		return requestResult{Err: ErrWrongGroup}, false
	}
	if kv.isInWaitIn(shard) {
		// key's shard is in process of migration
		return requestResult{Err: ErrInMigration}, false
	}
	return result, true
}

// common logic for RPC handler
//
// 1. check not killed
// 2. check is leader
// 3. check request's configNum, and trigger config fetcher if necessary
// 4. if is request of Get or PutAppend RPC
// 4.1. check if any cached result
// 4.2. check if the resident shard can be served
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

	// IMPORTANT: lock before rf.Start,
	// to avoid raft finish too quick before kv.commandTbl has set replyCh for this commandIndex
	kv.mu.Lock()

	switch payload := op.Payload.(type) {
	case MigrateShardsArgs:
		if kv.Config.Num < payload.ConfigNum {
			// my shard config seems outdated, tell configFetcher to update
			kv.triggerConfigFetch()
		}
	case ClerkRequest:
		if kv.Config.Num < payload.getConfigNum() {
			// my shard config seems outdated, tell configFetcher to update
			kv.mu.Unlock()
			kv.triggerConfigFetch()
			// cannot accept request because of unknown config from future, tell client to try later
			e = ErrUnknownConfig
			return
		}
		if result, applicable := kv.checkClerkRequest(payload); !applicable {
			kv.mu.Unlock()
			e, r = result.Err, result.Value
			return
		}
	}

	index, term, isLeader := kv.rf.Start(op)
	if term == 0 {
		kv.mu.Unlock()
		e = ErrInitElection
		return
	}
	if !isLeader {
		kv.mu.Unlock()
		e = ErrWrongLeader
		return
	}

	lablog.ShardDebug(kv.gid, kv.me, lablog.Server, "Start %v@%d", op, index)

	c := make(chan requestResult) // reply channel for applier goroutine
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
				lablog.ShardDebug(kv.gid, kv.me, lablog.Server, "Start %v@%d but NOT leader, term changed", op, index)
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
	reply.Err, _ = kv.commonHandler(Op{Payload: *args})
}

// MigrateShards RPC caller, migrate shards to a group
func (kv *ShardKV) migrateShards(gid int) {
	args := &MigrateShardsArgs{
		Gid: kv.gid,
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

		// IMPORTANT: need to provide at-most-once semantics (duplicate detection),
		// 						for client requests across shard movement
		args.ClientTbl = make(map[int64]cache)
		for clientId, cache := range kv.ClientTbl {
			if _, ok := args.Shards[cache.Shard]; ok {
				args.ClientTbl[clientId] = cache
			}
		}

		args.ConfigNum = kv.Config.Num // update args.ConfigNum to reflect my group's current configNum

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
				// migration done, start raft consensus to let group know it and update MigrateOut
				reply.Gid = gid
				reply.Shards = shards{}
				for shard, data := range args.Shards {
					reply.Shards[shard] = shardData{Gid: data.Gid, ConfigNum: data.ConfigNum, Data: nil}
				}
				_, _, isLeader := kv.rf.Start(Op{Payload: *reply})

				extra := ""
				if !isLeader {
					extra = " BUT NOT LEADER"
				}
				lablog.ShardDebug(kv.gid, kv.me, lablog.Log2, "CM->G%d %v%s", gid, args.Shards, extra)
				return
			}
		}

		// migration not done in this turn, wait a while and retry this group
		time.Sleep(serverWaitAndRetryInterval * time.Millisecond)
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
	labgob.Register(cache{})
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
	kv.migrationTrigger = make(chan bool, 1)

	kv.Tbl = shards{}
	kv.ClientTbl = make(map[int64]cache)
	kv.Config = shardctrler.Config{Num: 0}
	kv.Cluster = make(map[int]*groupInfo)
	kv.MigrateOut = shards{}
	kv.WaitIn = shards{}

	// initialize from snapshot persisted before a crash
	kv.readSnapshot(persister.ReadSnapshot())

	// communication between applier and snapshoter,
	// let applier trigger snapshoter to take a snapshot when certain amount of msgs have been applied
	snapshotTrigger := make(chan bool, 1)
	go kv.applier(applyCh, snapshotTrigger, kv.appliedCommandIndex)
	go kv.snapshoter(persister, snapshotTrigger)

	go kv.migrator(kv.migrationTrigger)
	kv.triggerMigration() // trigger any pending migration if remain in snapshot

	go kv.configFetcher(kv.configFetcherTrigger)
	kv.triggerConfigFetch() // trigger very first time config fetch

	return kv
}

// learn about new servers for each group, with mutex held
func (kv *ShardKV) updateCluster() {
	for gid, servers := range kv.Config.Groups {
		if _, ok := kv.Cluster[gid]; !ok {
			// create group info
			kv.Cluster[gid] = &groupInfo{Leader: 0, Servers: nil, MigrationTrigger: nil}
		}
		kv.Cluster[gid].Servers = servers
	}
}

// compare oldConfig with newConfig, reply whether this newConfig can be installed or not
// add shards that need to migrate out from my group,
// add shards that my group is waiting for migration data in,
// with mutex held
func (kv *ShardKV) updateMigrateOutAndWaitIn(oldConfig, newConfig shardctrler.Config) bool {
	if oldConfig.Num <= 0 {
		return true
	}

	addedMigrateOut, addedWaitIn := shards{}, shards{}
	for shard, oldGid := range oldConfig.Shards {
		newGid := newConfig.Shards[shard]

		out, outOk := kv.MigrateOut[shard]
		if oldGid == kv.gid && newGid != kv.gid {
			// shard in my group of oldConfig, but not in my group of newConfig,
			// so need to migrate out
			switch {
			case !outOk:
				// move this shard into migration
				addedMigrateOut[shard] = shardData{Gid: newGid, ConfigNum: newConfig.Num, Data: kv.Tbl[shard].Data}
			default:
				// there is a migrate-out existing for the same shard, don't update shard config
				return false
			}
		}

		in, inOk := kv.WaitIn[shard]
		if oldGid != kv.gid && newGid == kv.gid {
			// shard not in my group of oldConfig, but in my group of current config,
			// so need to wait in
			switch {
			case !inOk:
				// shard has not reached before, so my group need to wait this shard
				addedWaitIn[shard] = shardData{Gid: kv.gid, ConfigNum: newConfig.Num, Data: nil}
			case outOk && in.ConfigNum < out.ConfigNum:
				// this shard is already waited-in, and need to migrate out after migrate in,
				// so in order to not mess this wait-in -> migrate-out order,
				// don't update shard config
				return false
			case in.ConfigNum == newConfig.Num && in.Gid == kv.gid:
				// in this new config, this shard should be in my group
			default:
				lablog.ShardDebug(kv.gid, kv.me, lablog.Error, "UWI %d%v", shard, in)
				panic(shard)
			}
		}
	}

	// check passed, can update shard config

	// add to MigrateOut, remove from my group's shards
	for shard, out := range addedMigrateOut {
		kv.MigrateOut[shard] = out
		delete(kv.Tbl, shard)
	}
	// add to WaitIn
	for shard, in := range addedWaitIn {
		kv.WaitIn[shard] = in
	}
	for shard, in := range kv.WaitIn {
		if in.Data != nil && in.ConfigNum == newConfig.Num && in.Gid == kv.gid {
			// shard has reached before my group is aware of the shard config change,
			// so when my group know this shard config change,
			// my group can happily accept this shard and install into my shards
			kv.Tbl[shard] = in.Copy()
			delete(kv.WaitIn, shard)
		}
	}
	return true
}

// update my group's shard config, (it's the ONLY place where shard config can be updated)
// and if leader, migrate shards out,
// with mutex held
func (kv *ShardKV) installConfig(config shardctrler.Config) {
	if config.Num <= kv.Config.Num || !kv.updateMigrateOutAndWaitIn(kv.Config, config) {
		// outdated config, ignore it
		// or cannot update MigrateOut or WaitIn, so abort installing config
		return
	}

	kv.Config = config
	kv.updateCluster()

	if _, isLeader := kv.rf.GetState(); !isLeader {
		// only leader can migrate any shards out
		return
	}

	lablog.ShardDebug(kv.gid, kv.me, lablog.Trace, "CN->%d MO %v,WI %v", config.Num, kv.MigrateOut.ByGroup(), kv.WaitIn.ByGroup())

	// trigger fetcher to see if any newer config, in order to update my shard config ASAP
	kv.triggerConfigFetch()
	// trigger migration for each group
	kv.triggerMigration()
}

// apply Get RPC request, with mutex held
func (kv *ShardKV) applyGetArgs(args *GetArgs) requestResult {
	shard := key2shard(args.Key)
	return requestResult{Err: OK, Value: kv.Tbl[shard].Data[args.Key]}
}

// apply PutAppend RPC request, with mutex held
func (kv *ShardKV) applyPutAppendArgs(args *PutAppendArgs) requestResult {
	shard := key2shard(args.Key)
	if _, ok := kv.Tbl[shard]; !ok {
		kv.Tbl[shard] = shardData{Gid: kv.gid, ConfigNum: kv.Config.Num, Data: kvTable{}}
	}
	if args.Op == opPut {
		kv.Tbl[shard].Data[args.Key] = args.Value
	} else {
		kv.Tbl[shard].Data[args.Key] += args.Value
	}
	return requestResult{Err: OK, Value: nil}
}

// migration request accepted, install shards' data from this migration, with mutex held
func (kv *ShardKV) applyMigrateShardsArgs(args *MigrateShardsArgs) requestResult {
	// reply to caller with set of installed shards
	installed := set{}

	for shard, migration := range args.Shards {
		switch in, inOk := kv.WaitIn[shard]; {
		case migration.ConfigNum > kv.Config.Num:
			// migration shard reaches before my group is aware of the shard config change,
			// and my group trusts the migration source group,
			// believing this migration shard will be needed in the future config,
			// so accept this migration shard, store it into my WaitIn
			kv.WaitIn[shard] = migration
		case !inOk:
			// migration already accepted, and wait-in of this shard is deleted
			continue
		case migration.ConfigNum == in.ConfigNum:
			// migration shard matches the same shard in my group's WaitIn,
			// and they are of the same shard config, so my group should accept this migration shard

			switch out, outOk := kv.MigrateOut[shard]; {
			case kv.shouldServeShard(shard):
				// my group should serve request for this shard, so install shard into my group's kvTable
				kv.Tbl[shard] = shardData{Gid: kv.gid, ConfigNum: kv.Config.Num, Data: migration.Copy().Data}
				// remove shard from my group's WaitIn
				delete(kv.WaitIn, shard)
				// tell caller this shard is installed to my group
				installed[shard] = true
			case !outOk:
				// no need to migrate out at this shard config, but will migrate out at later shard config
			case out.ConfigNum > in.ConfigNum:
				// this migration shard need to migrate out, so no need to install into my group's shards,
				// instead, move this migration shard to MigrateOut
				kv.MigrateOut[shard] = shardData{Gid: out.Gid, ConfigNum: out.ConfigNum, Data: migration.Data}
				// remove shard from my group's WaitIn
				delete(kv.WaitIn, shard)
				// tell caller this shard is installed to my group
				installed[shard] = true
			default:
				lablog.ShardDebug(kv.gid, kv.me, lablog.Error, "HMI G%d-> %v, inner default %d%v", args.Gid, args.Shards, shard, in)
				panic(shard)
			}

		case migration.ConfigNum < in.ConfigNum:
			// migration not match wait-in, maybe arrive late
			installed[shard] = true
		default:
			lablog.ShardDebug(kv.gid, kv.me, lablog.Error, "HMI G%d-> %v, outer default %d%v", args.Gid, args.Shards, shard, in)
			panic(shard)
		}
	}

	// also install client request cache for these shards
	for clientId, cache := range args.ClientTbl {
		if existingCache, ok := kv.ClientTbl[clientId]; !ok || cache.OpId > existingCache.OpId {
			kv.ClientTbl[clientId] = cache
		}
	}

	if _, isLeader := kv.rf.GetState(); isLeader {
		lablog.ShardDebug(kv.gid, kv.me, lablog.Trace, "HMI MO %v,WI %v", kv.MigrateOut.ByGroup(), kv.WaitIn.ByGroup())

		// MigrateOut updated, trigger migration
		kv.triggerMigration()
		// WaitIn updated, trigger config fetch, maybe there is a new shard config can be updated
		kv.triggerConfigFetch()
	}

	return requestResult{Err: OK, Value: installed}
}

// migration done, remove shard from my group's MigrateOut, with mutex held
func (kv *ShardKV) applyMigrateShardsReply(reply *MigrateShardsReply) {
	for shard, accepted := range reply.Shards {
		if out, ok := kv.MigrateOut[shard]; ok && out.Gid == accepted.Gid && out.ConfigNum == accepted.ConfigNum {
			// accepted migration version matches current migrate-out, can remove from my group's MigrateOut
			delete(kv.MigrateOut, shard)
		}
	}

	if _, isLeader := kv.rf.GetState(); isLeader {
		lablog.ShardDebug(kv.gid, kv.me, lablog.Trace, "AMO MO %v,WI %v", kv.MigrateOut.ByGroup(), kv.WaitIn.ByGroup())

		// MigrateOut updated, trigger config fetch, maybe there is a new shard config can be updated
		kv.triggerConfigFetch()
		return
	}
}

// The applier go routine accept applyMsg from applyCh (from underlying raft),
// modify key-value table accordingly,
// reply modified result back to KVServer's RPC handler, if any, through channel identified by commandIndex
// after every snapshoterAppliedMsgInterval msgs, trigger a snapshot
func (kv *ShardKV) applier(applyCh <-chan raft.ApplyMsg, snapshotTrigger chan<- bool, lastSnapshoterTriggeredCommandIndex int) {
	defer func() {
		kv.mu.Lock()
		// close all pending RPC handler reply channel to avoid goroutine resource leak
		for _, ce := range kv.commandTbl {
			close(ce.replyCh)
		}
		kv.mu.Unlock()
	}()

	for m := range applyCh {
		if m.SnapshotValid {
			// is snapshot, reset kv server state according to this snapshot
			kv.mu.Lock()
			kv.appliedCommandIndex = m.SnapshotIndex
			kv.readSnapshot(m.Snapshot)
			// clear all pending reply channel, to avoid goroutine resource leak
			for _, ce := range kv.commandTbl {
				ce.replyCh <- requestResult{Err: ErrWrongLeader}
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

		var result requestResult
		switch payload := op.Payload.(type) {
		case ClerkRequest:
			if r, applicable := kv.checkClerkRequest(payload); !applicable {
				result = r
			} else {
				switch args := payload.(type) {
				case GetArgs:
					result = kv.applyGetArgs(&args)
				case PutAppendArgs:
					result = kv.applyPutAppendArgs(&args)
				}
				// cache operation result
				kv.ClientTbl[payload.getClientId()] = cache{OpId: payload.getOpId(), Shard: payload.getShard(), Result: result}
			}
		case MigrateShardsArgs:
			result = kv.applyMigrateShardsArgs(&payload)
		case shardctrler.Config:
			kv.installConfig(payload)
			kv.mu.Unlock()
			continue
		case MigrateShardsReply:
			kv.applyMigrateShardsReply(&payload)
			kv.mu.Unlock()
			continue
		}

		ce, ok := kv.commandTbl[m.CommandIndex]
		if ok {
			delete(kv.commandTbl, m.CommandIndex) // delete won't-use reply channel
		}
		kv.mu.Unlock()

		// only leader server maintains commandTbl, followers just apply kv modification
		if ok {
			if !ce.op.Equal(&op) {
				// Your solution needs to handle a leader that has called Start() for a Clerk's RPC,
				// but loses its leadership before the request is committed to the log.
				// In this case you should arrange for the Clerk to re-send the request to other servers
				// until it finds the new leader.
				//
				// One way to do this is for the server to detect that it has lost leadership,
				// by noticing that a different request has appeared at the index returned by Start()
				ce.replyCh <- requestResult{Err: ErrWrongLeader}
			} else {
				switch r := result.Value.(type) {
				case string: // Get
				case nil: // PutAppend
				case set: // MigrateShards
					lablog.ShardDebug(kv.gid, kv.me, lablog.Migrate, "Done %v@%d %v", op, m.CommandIndex, r)
				}
				ce.replyCh <- result
			}
		}
	}
}

// trigger migrator
func (kv *ShardKV) triggerMigration() {
	select {
	case kv.migrationTrigger <- true:
	default:
	}
}

// The migrator go routine check my group's MigrateOut,
// if any shards need to migrate out, trigger the corresponding shardsMigrator of that group
func (kv *ShardKV) migrator(trigger <-chan bool) {
	defer func() {
		kv.mu.Lock()
		// close trigger of each group to avoid goroutine resource leak
		for _, groupInfo := range kv.Cluster {
			if groupInfo.MigrationTrigger != nil {
				close(groupInfo.MigrationTrigger)
			}
		}
		kv.mu.Unlock()
	}()

	for !kv.killed() {
		select {
		case _, ok := <-trigger:
			if !ok {
				return
			}
		case <-time.After(migratorInterval * time.Millisecond):
			if kv.killed() {
				return
			}
		}

		if _, isLeader := kv.rf.GetState(); !isLeader {
			// only leader should migrate shards out
			continue
		}

		kv.mu.Lock()
		for gid := range kv.MigrateOut.toGroupSet() {
			// create new shardsMigrator for this group if not started
			switch group, ok := kv.Cluster[gid]; {
			case !ok:
				kv.Cluster[gid] = &groupInfo{Leader: 0, Servers: nil, MigrationTrigger: make(chan bool)}
				go kv.shardsMigrator(gid, kv.Cluster[gid].MigrationTrigger)
			case group.MigrationTrigger == nil:
				group.MigrationTrigger = make(chan bool)
				go kv.shardsMigrator(gid, kv.Cluster[gid].MigrationTrigger)
			}

			// trigger shards migration for this group
			select {
			case kv.Cluster[gid].MigrationTrigger <- true:
			default:
			}
		}
		kv.mu.Unlock()
	}
}

// trigger configFetcher
func (kv *ShardKV) triggerConfigFetch() {
	select {
	case kv.configFetcherTrigger <- true:
	default:
	}
}

// The configFetcher go routine polls the shardctrler,
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
			if kv.killed() {
				return
			}
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
	var clientTbl map[int64]cache
	var config shardctrler.Config
	var cluster map[int]*groupInfo
	var migrateOut shards
	var waitIn shards
	if d.Decode(&tbl) != nil ||
		d.Decode(&clientTbl) != nil ||
		d.Decode(&config) != nil ||
		d.Decode(&cluster) != nil ||
		d.Decode(&migrateOut) != nil ||
		d.Decode(&waitIn) != nil {
		lablog.ShardDebug(kv.gid, kv.me, lablog.Error, "Read broken snapshot")
		return
	}
	kv.Tbl = tbl
	kv.ClientTbl = clientTbl
	kv.Config = config
	kv.Cluster = cluster
	kv.MigrateOut = migrateOut
	kv.WaitIn = waitIn
}
