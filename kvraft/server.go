package raftkv

import (
	"log"
	"sync"
	"time"

	"github.com/dianchengwangCHN/raft-key-value-store/labgob"
	"github.com/dianchengwangCHN/raft-key-value-store/labrpc"
	"github.com/dianchengwangCHN/raft-key-value-store/raft"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

// AGREEMENTTIMEOUTInterval is the timeout interval for reaching agreement to append a log entry
const AGREEMENTTIMEOUTInterval time.Duration = 1000

// OpType defines the type of each command that is logged
type OpType string

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type     OpType
	Key      string
	Value    string
	ClientID int64
	SerialID uint
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kvMap             map[string]string     // map used to store key-value data
	lastClerkSerialID map[int64]uint        // store the last received SerialID for each clerk (Since it's OK to assume that a client will make only one call into a Clerk at a time.)
	entryAppliedChs   map[int]chan struct{} // store the channels used to notify operation success on each log index
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	_, isLeader := kv.rf.GetState()
	succeed := false
	if isLeader {
		command := Op{
			Type:     "Get",
			Key:      args.Key,
			ClientID: args.ClientID,
			SerialID: args.SerialID,
		}
		index, term, isLeader := kv.rf.Start(command)
		_, _, _ = index, term, isLeader
		if _, ok := kv.entryAppliedChs[index]; !ok {
			kv.entryAppliedChs[index] = make(chan struct{})
		}
		doneCh := kv.entryAppliedChs[index]
		select {
		case <-time.After(AGREEMENTTIMEOUTInterval * time.Millisecond):
		case <-doneCh:

		}
	}

	if succeed {
		reply.WrongLeader = false

	} else {
		reply.WrongLeader = true
		if ID := kv.rf.GetLeaderID(); ID != -1 {
			reply.LeaderID = ID
		}
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	_, isLeader := kv.rf.GetState()
	if isLeader {

	} else {

	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *KVServer) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.

	return kv
}
