package raftkv

import (
	"crypto/rand"
	"math/big"

	// mrand "math/rand"

	// "sync"

	"github.com/dianchengwangCHN/raft-key-value-store/labrpc"
)

type Clerk struct {
	servers    []*labrpc.ClientEnd
	id         int64
	leaderID   int
	opSerialID uint
	// mu         sync.Mutex
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
	ck.id = nrand()
	ck.leaderID = 0
	// ck.leaderID = mrand.Intn(len(servers))
	ck.opSerialID = 0
	return ck
}

func (ck *Clerk) sendRPC(args interface{}, reply ClerkRPCReply, op string) bool {
	var ok bool
	DPrintf("client %d sent %v RPC to %d, serialID: %d\n", ck.id, op, ck.leaderID, ck.opSerialID-1)
	switch op {
	case "Get":
		ok = ck.servers[ck.leaderID].Call("KVServer.Get", args, reply)
	case "PutAppend":
		ok = ck.servers[ck.leaderID].Call("KVServer.PutAppend", args, reply)
	default:
		return ok
	}

	DPrintf("client %d got %v reply from %d, ok: %v, serialID: %d, WrongLeader: %v\n", ck.id, op, ck.leaderID, ok, ck.opSerialID-1, reply.GetWrongLeader())
	if ok {
		if !reply.GetWrongLeader() {
			DPrintf("client %d got %v reply from %d, serialID: %d, success\n", ck.id, op, ck.leaderID, ck.opSerialID-1)
			return ok
		}
		// If request is rejected because of incorrect leader, then change leaderID
		if reply.GetLeaderID() != -1 {
			// ck.leaderID = reply.GetLeaderID()
			ck.leaderID = (ck.leaderID + 1) % len(ck.servers)
			reply.SetLeaderID(-1)
		} else { // If the server also do not know the LeaderID
			// ck.leaderID = mrand.Intn(len(ck.servers))
			ck.leaderID = (ck.leaderID + 1) % len(ck.servers)
		}
	} else {
		// Whenever RPC timeout, should retry with another randomly-chosen server
		// ck.leaderID = mrand.Intn(len(ck.servers))
		ck.leaderID = (ck.leaderID + 1) % len(ck.servers)
	}
	ok = false
	return ok
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	// ck.mu.Lock()
	opSerialID := ck.opSerialID
	ck.opSerialID++
	// ck.mu.Unlock()

	args := &GetArgs{
		Key:      key,
		ClientID: ck.id,
		SerialID: opSerialID,
	}

	for {
		reply := &GetReply{
			LeaderID: -1,
		}
		ok := ck.sendRPC(args, reply, "Get")
		if ok {
			if reply.Err == OK {
				return reply.Value
			}
			return ""
		}
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// ck.mu.Lock()
	opSerialID := ck.opSerialID
	ck.opSerialID++
	// ck.mu.Unlock()

	args := &PutAppendArgs{
		Key:      key,
		Value:    value,
		Op:       op,
		ClientID: ck.id,
		SerialID: opSerialID,
	}

	for {
		reply := &PutAppendReply{
			LeaderID: -1,
		}
		ok := ck.sendRPC(args, reply, "PutAppend")
		if ok {
			break
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
