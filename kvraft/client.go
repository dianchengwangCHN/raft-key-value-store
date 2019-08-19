package raftkv

import (
	"crypto/rand"
	"math/big"
	mrand "math/rand"

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
	ck.leaderID = mrand.Intn(len(servers))
	ck.opSerialID = 0
	return ck
}

func (ck *Clerk) sendRPC(args interface{}, reply ClerkRPCReply, op string) {
	for {
		var ok bool
		if op == "Get" {
			ok = ck.servers[ck.leaderID].Call("KVServer.Get", args, reply)
		} else {
			ok = ck.servers[ck.leaderID].Call("KVServer.PutAppend", args, reply)
		}
		if ok {
			if !reply.GetWrongLeader() {
				return
			}
			// If request is rejected because of incorrect leader, then change leaderID
			if reply.GetLeaderID() != -1 {
				ck.leaderID = reply.GetLeaderID()
				reply.SetLeaderID(-1)
			} else { // If the server also do not know the LeaderID
				ck.leaderID = mrand.Intn(len(ck.servers))
			}
		} else {
			// Whenever RPC timeout, should retry with another randomly-chosen server
			ck.leaderID = mrand.Intn(len(ck.servers))
		}
	}
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
	reply := &GetReply{
		LeaderID: -1,
	}
	ck.sendRPC(args, reply, "Get")
	if reply.Err == OK {
		return reply.Value
	}
	return ""
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
	reply := &PutAppendReply{
		LeaderID: -1,
	}
	ck.sendRPC(args, reply, "PutAppend")
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
