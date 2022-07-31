package shardkv

import "6.824/shardctrler"

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

//
// which shard is a key in?
// please use this function,
// and please do not change it.
//
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}

type Err string

const (
	OK       Err = "OK"
	ErrNoKey Err = "ErrNoKey"
	// server reply when its group is not responsible for key's shard,
	// client should update shard config and retry
	ErrWrongGroup Err = "ErrWrongGroup"
	// server reply when it is not the group leader,
	// client should retry for another server in this group
	ErrWrongLeader Err = "ErrWrongLeader"
	// server reply when it is "dead" or going to down (Raft not functional),
	// client should retry for another server in this group
	ErrShutdown Err = "ErrShutdown"
	// server reply when it is in the very fist election, no leader elected,
	// client should wait for a while and retry to the same server,
	// hoping this time election is done
	ErrInitElection Err = "ErrInitElection"
	// server reply when its shard config is newer than request's,
	// client should update its shard config from shardctrler
	ErrOutdatedConfig Err = "ErrOutdatedConfig"
	// server reply when its shard config is older than request's,
	// server will try to update its shard config from shardctrler,
	// client should wait for a while and retry to the same server,
	// hoping this time server's shard config is updated
	ErrUnknownConfig Err = "ErrUnknownConfig"
	// server reply when requested key's shard is in migration,
	// client should wait for a while and retry to the same server,
	// hoping this time this shard's migration is done
	ErrInMigration Err = "ErrInMigration"
)

type ClerkRequest interface {
	getClientId() int64
	getOpId() int
	getShard() int
	getConfigNum() int
}

type opType string

const (
	opGet    opType = "G"
	opPut    opType = "P"
	opAppend opType = "A"
)

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    opType // "Put" or "Append"

	ClientId  int64 // id of client
	OpId      int   // client operation id
	ConfigNum int   // num of client's shard config
}

func (args PutAppendArgs) getClientId() int64 {
	return args.ClientId
}
func (args PutAppendArgs) getOpId() int {
	return args.OpId
}
func (args PutAppendArgs) getShard() int {
	return key2shard(args.Key)
}
func (args PutAppendArgs) getConfigNum() int {
	return args.ConfigNum
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string

	ClientId  int64 // id of client
	OpId      int   // client operation id
	ConfigNum int   // num of client's shard config
}

func (args GetArgs) getClientId() int64 {
	return args.ClientId
}
func (args GetArgs) getOpId() int {
	return args.OpId
}
func (args GetArgs) getShard() int {
	return key2shard(args.Key)
}
func (args GetArgs) getConfigNum() int {
	return args.ConfigNum
}

type GetReply struct {
	Err   Err
	Value string
}

type MigrateShardsArgs struct {
	Gid       int             // gid that send this request
	Shards    shards          // migration shards
	ClientTbl map[int64]cache // client request cache for these shards

	ConfigNum int // num of client's shard config
}

type MigrateShardsReply struct {
	Err Err

	Gid    int // gid that send this reply
	Shards shards
}
