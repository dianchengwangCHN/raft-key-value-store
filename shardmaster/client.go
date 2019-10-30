package shardmaster

//
// Shardmaster clerk.
//

import (
	"crypto/rand"
	"math/big"
	"time"

	"github.com/dianchengwangCHN/raft-key-value-store/labrpc"
)

const retryInterval time.Duration = 100

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	leaderID   int
	id         int64
	opSerialID uint
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// Your code here.
	ck.leaderID = 0
	ck.id = nrand()
	ck.opSerialID = 1
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{}
	// Your code here.
	args.Num = num
	args.ClerkInfo = ClientInfo{
		ClientID: ck.id,
		SerialID: ck.opSerialID,
	}
	ck.opSerialID++
	for {
		// try each known server.
		reply := QueryReply{
			// WrongLeader: true,
		}
		dPrintf("clerk sent Query to %d, Num %d\n", ck.leaderID, args.Num)
		ok := ck.servers[ck.leaderID].Call("ShardMaster.Query", args, &reply)
		dPrintf("clerk got reply from %d, ok: %v, reply WrongLeader: %v, Err: %v, Num %d\n", ck.leaderID, ok, reply.WrongLeader, reply.Err, reply.Config.Num)
		if ok && reply.Err == OK {
			return reply.Config
		}
		// if reply.WrongLeader {
		ck.leaderID = (ck.leaderID + 1) % len(ck.servers)
		// }
		time.Sleep(retryInterval * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{}
	// Your code here.
	args.Servers = servers
	args.ClerkInfo = ClientInfo{
		ClientID: ck.id,
		SerialID: ck.opSerialID,
	}
	ck.opSerialID++
	for {
		// try each known server.
		reply := JoinReply{
			// WrongLeader: true,
		}
		ok := ck.servers[ck.leaderID].Call("ShardMaster.Join", args, &reply)
		if ok && reply.Err == OK {
			return
		}
		// if reply.WrongLeader {
		ck.leaderID = (ck.leaderID + 1) % len(ck.servers)
		// }
		time.Sleep(retryInterval * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{}
	// Your code here.
	args.GIDs = gids
	args.ClerkInfo = ClientInfo{
		ClientID: ck.id,
		SerialID: ck.opSerialID,
	}
	ck.opSerialID++
	for {
		// try each known server.
		reply := LeaveReply{
			// WrongLeader: true,
		}
		ok := ck.servers[ck.leaderID].Call("ShardMaster.Leave", args, &reply)
		if ok && reply.Err == OK {
			return
		}
		// if reply.WrongLeader {
		ck.leaderID = (ck.leaderID + 1) % len(ck.servers)
		// }
		time.Sleep(retryInterval * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{}
	// Your code here.
	args.Shard = shard
	args.GID = gid
	args.ClerkInfo = ClientInfo{
		ClientID: ck.id,
		SerialID: ck.opSerialID,
	}
	ck.opSerialID++
	for {
		// try each known server.
		reply := MoveReply{
			// WrongLeader: true,
		}
		ok := ck.servers[ck.leaderID].Call("ShardMaster.Move", args, &reply)
		if ok && reply.Err == OK {
			return
		}
		// if reply.WrongLeader {
		ck.leaderID = (ck.leaderID + 1) % len(ck.servers)
		// }
		time.Sleep(retryInterval * time.Millisecond)
	}
}
