package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardctrler to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	"time"

	"6.824/labrpc"
	"6.824/labutil"
	"6.824/shardctrler"
)

const (
	// periodically poll shardctrler to learn about new shard config
	clientRefreshConfigInterval = 100
	// client be told to wait a while and retry
	clientWaitAndRetryInterval = 50
)

type Clerk struct {
	sm       *shardctrler.Clerk // shard manager
	config   shardctrler.Config // latest known shard config
	make_end func(string) *labrpc.ClientEnd
	// You will have to modify this struct.
	me          int64       // my client id
	groupLeader map[int]int // for each group, remember which server turned out to be the leader for the last RPC
	opId        int         // operation id, increase monotonically
}

//
// the tester calls MakeClerk.
//
// ctrlers[] is needed to call shardctrler.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
//
func MakeClerk(ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardctrler.MakeClerk(ctrlers)
	ck.make_end = make_end
	// You'll have to add code here.
	ck.me = labutil.Nrand()
	ck.groupLeader = make(map[int]int)
	ck.opId = 1
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
//
func (ck *Clerk) Get(key string) string {
	args := &GetArgs{
		Key:      key,
		ClientId: ck.me,
		OpId:     ck.opId,
	}
	ck.opId++
	shard := key2shard(key)

	for {
		args.ConfigNum = ck.config.Num
		sleepInterval := clientRefreshConfigInterval

		gid := ck.config.Shards[shard]
		servers := ck.config.Groups[gid]
		serverId := ck.groupLeader[gid]
		for i, nServer := 0, len(servers); i < nServer; {
			srv := ck.make_end(servers[serverId])
			reply := &GetReply{}
			ok := srv.Call("ShardKV.Get", args, reply)
			if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrShutdown {
				serverId = (serverId + 1) % nServer
				i++
				continue
			}
			if reply.Err == ErrInitElection {
				time.Sleep(clientWaitAndRetryInterval * time.Millisecond)
				continue
			}

			ck.groupLeader[gid] = serverId
			if reply.Err == ErrUnknownConfig || reply.Err == ErrInMigration {
				time.Sleep(clientWaitAndRetryInterval * time.Millisecond)
				continue
			}
			if reply.Err == ErrWrongGroup {
				break
			}
			if reply.Err == ErrOutdatedConfig {
				sleepInterval = 0
				break
			}
			if reply.Err == ErrNoKey {
				return ""
			}
			if reply.Err == OK {
				return reply.Value
			}
		}

		time.Sleep(time.Duration(sleepInterval) * time.Millisecond)
		// ask controller for the latest configuration.
		config := ck.sm.Query(-1)
		if config.Num < 0 {
			return ""
		}
		ck.config = config
	}
}

//
// shared by Put and Append.
// You will have to modify this function.
//
func (ck *Clerk) PutAppend(key string, value string, op opType) {
	args := &PutAppendArgs{
		Key:      key,
		Value:    value,
		Op:       op,
		ClientId: ck.me,
		OpId:     ck.opId,
	}
	ck.opId++
	shard := key2shard(key)

	for {
		args.ConfigNum = ck.config.Num
		sleepInterval := clientRefreshConfigInterval

		gid := ck.config.Shards[shard]
		servers := ck.config.Groups[gid]
		serverId := ck.groupLeader[gid]
		for i, nServer := 0, len(servers); i < nServer; {
			srv := ck.make_end(servers[serverId])
			reply := &PutAppendReply{}
			ok := srv.Call("ShardKV.PutAppend", args, reply)
			if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrShutdown {
				serverId = (serverId + 1) % nServer
				i++
				continue
			}
			if reply.Err == ErrInitElection {
				time.Sleep(clientWaitAndRetryInterval * time.Millisecond)
				continue
			}

			ck.groupLeader[gid] = serverId
			if reply.Err == ErrUnknownConfig || reply.Err == ErrInMigration {
				time.Sleep(clientWaitAndRetryInterval * time.Millisecond)
				continue
			}
			if reply.Err == ErrWrongGroup {
				break
			}
			if reply.Err == ErrOutdatedConfig {
				sleepInterval = 0
				break
			}
			if reply.Err == OK {
				return
			}
		}

		time.Sleep(time.Duration(sleepInterval) * time.Millisecond)
		// ask controller for the latest configuration.
		config := ck.sm.Query(-1)
		if config.Num < 0 {
			return
		}
		ck.config = config
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, opPut)
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, opAppend)
}
