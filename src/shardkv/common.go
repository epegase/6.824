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
	OK             Err = "OK"
	ErrNoKey       Err = "ErrNoKey"
	ErrWrongGroup  Err = "ErrWrongGroup"
	ErrWrongLeader Err = "ErrWrongLeader"
	ErrShutdown    Err = "ErrShutdown"
)

type opType string

const (
	opGet    opType = "G"
	opPut    opType = "P"
	opAppend opType = "A"
)

// Put or Append
type PutAppendArgs struct {
	Key      string
	Value    string
	Op       opType // "Put" or "Append"
	ClientId int64  // id of client
	OpId     int    // client operation id
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key      string
	ClientId int64 // id of client
	OpId     int   // client operation id
}

type GetReply struct {
	Err   Err
	Value string
}
