package shardctrler

//
// Shardctrler clerk.
//

import (
	"time"

	"6.824/labrpc"
	"6.824/labutil"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	me     int64 // my client id
	leader int   // remember which serer turned out to be the leader for the last RPC
	opId   int   // operation id, increase monotonically
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// Your code here.
	ck.me = labutil.Nrand()
	ck.leader = 0
	ck.opId = 1
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{
		Num:      num,
		ClientId: ck.me,
		OpId:     ck.opId,
	}
	ck.opId++
	serverId := ck.leader // send to leader first

	for nNotOk := 0; nNotOk < 10; {
		reply := &QueryReply{}
		ok := ck.servers[serverId].Call("ShardCtrler.Query", args, reply)
		if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrShutdown {
			serverId = (serverId + 1) % len(ck.servers)
			if !ok {
				nNotOk++ // shardctrler maybe down, retry some times, but DEFINITELY not keep trying
			} else {
				nNotOk = 0
			}
			continue
		}
		if reply.Err == ErrInitElection {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		ck.leader = serverId
		return reply.Config
	}

	return Config{Num: -1} // client tried its best, but shardctrler not respond
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{
		Servers:  servers,
		ClientId: ck.me,
		OpId:     ck.opId,
	}
	ck.opId++
	serverId := ck.leader // send to leader first
	for {
		reply := &JoinReply{}
		ok := ck.servers[serverId].Call("ShardCtrler.Join", args, reply)
		if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrShutdown {
			serverId = (serverId + 1) % len(ck.servers)
			continue
		}
		if reply.Err == ErrInitElection {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		ck.leader = serverId
		return
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{
		GIDs:     gids,
		ClientId: ck.me,
		OpId:     ck.opId,
	}
	ck.opId++
	serverId := ck.leader // send to leader first
	for {
		reply := &LeaveReply{}
		ok := ck.servers[serverId].Call("ShardCtrler.Leave", args, reply)
		if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrShutdown {
			serverId = (serverId + 1) % len(ck.servers)
			continue
		}
		if reply.Err == ErrInitElection {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		ck.leader = serverId
		return
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{
		Shard:    shard,
		GID:      gid,
		ClientId: ck.me,
		OpId:     ck.opId,
	}
	ck.opId++
	serverId := ck.leader // send to leader first
	for {
		reply := &MoveReply{}
		ok := ck.servers[serverId].Call("ShardCtrler.Move", args, reply)
		if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrShutdown {
			serverId = (serverId + 1) % len(ck.servers)
			continue
		}
		if reply.Err == ErrInitElection {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		ck.leader = serverId
		return
	}
}
