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
	kvMap             map[string]string    // map used to store key-value data
	lastClerkSerialID map[int64]uint       // store the last received SerialID for each clerk (Since it's OK to assume that a client will make only one call into a Clerk at a time.)
	entryAppliedChs   map[int]chan DoneMsg // store the channels used to notify operation success on each log index
}

func (kv *KVServer) isDone(clientID int64, serialID uint) bool {
	kv.mu.Lock()
	v, ok := kv.lastClerkSerialID[clientID]
	kv.mu.Unlock()
	if ok {
		return v >= serialID
	}
	return false
}

func (kv *KVServer) startAgreement(command Op) bool {
	index, _, isLeader := kv.rf.Start(command)
	if isLeader {
		DPrintf("server %d start agreement on %d\n", kv.me, index)
		kv.mu.Lock()
		if _, ok := kv.entryAppliedChs[index]; !ok {
			kv.entryAppliedChs[index] = make(chan DoneMsg)
		}
		DPrintf("server %d create channel for index%d\n", kv.me, index)
		doneCh := kv.entryAppliedChs[index]
		kv.mu.Unlock()
		select {
		case <-time.After(AGREEMENTTIMEOUTInterval * time.Millisecond):
			DPrintf("server %d agreement timeout\n", kv.me)
		case msg := <-doneCh:
			if msg.ClientID == command.ClientID && msg.SerialID == command.SerialID {
				// kv.mu.Lock()
				// if kv.lastClerkSerialID[msg.ClientID] <= msg.SerialID {
				// 	kv.lastClerkSerialID[msg.ClientID] = msg.SerialID
				// }
				// kv.mu.Unlock()
				DPrintf("server %d update lastSerialID to %d for client %d\n", kv.me, msg.SerialID, msg.ClientID)
				return true
			}
			// doneCh <- msg
		}
	}
	return false
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	var succeed bool
	if kv.isDone(args.ClientID, args.SerialID) {
		succeed = true
	} else {
		_, isLeader := kv.rf.GetState()
		if isLeader {
			command := Op{
				Type:     "Get",
				Key:      args.Key,
				ClientID: args.ClientID,
				SerialID: args.SerialID,
			}
			succeed = kv.startAgreement(command)
		}
	}

	if succeed {
		reply.WrongLeader = false
		kv.mu.Lock()
		v, ok := kv.kvMap[args.Key]
		kv.mu.Unlock()
		reply.Value = v
		if ok {
			reply.Err = OK
		} else {
			reply.Err = ErrNoKey
		}
		return
	}
	reply.WrongLeader = true
	if ID := kv.rf.GetLeaderID(); ID != -1 {
		reply.LeaderID = ID
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	succeed := false
	if kv.isDone(args.ClientID, args.SerialID) {
		succeed = true
	} else {
		_, isLeader := kv.rf.GetState()
		if isLeader {
			command := Op{
				Type:     OpType(args.Op),
				Key:      args.Key,
				Value:    args.Value,
				ClientID: args.ClientID,
				SerialID: args.SerialID,
			}
			succeed = kv.startAgreement(command)
		}
	}

	if succeed {
		reply.WrongLeader = false
		DPrintf("server %d replied: Succeed, WrongLeader: %v\n", kv.me, reply.WrongLeader)
		return
	}
	DPrintf("server %d replied: WrongLeader\n", kv.me)
	reply.WrongLeader = true
	if ID := kv.rf.GetLeaderID(); ID != -1 {
		reply.LeaderID = ID
	}
	return
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
	kv.kvMap = make(map[string]string)
	kv.lastClerkSerialID = make(map[int64]uint)
	kv.entryAppliedChs = make(map[int]chan DoneMsg)

	go kv.startApplyMsgExecutor()

	return kv
}

// DoneMsg models the message passed to doneCh
type DoneMsg struct {
	ClientID int64
	SerialID uint
}

func (kv *KVServer) startApplyMsgExecutor() {
	for {
		msg := <-kv.applyCh
		command := msg.Command.(Op)
		kv.mu.Lock()
		switch command.Type {
		case "Get":

		case "Put":
			kv.kvMap[command.Key] = command.Value
		case "Append":
			kv.kvMap[command.Key] += command.Value
		}
		index := msg.CommandIndex
		if kv.lastClerkSerialID[command.ClientID] <= command.SerialID {
			kv.lastClerkSerialID[command.ClientID] = command.SerialID
		}
		kv.mu.Unlock()
		if ch, ok := kv.entryAppliedChs[index]; ok {
			ch <- DoneMsg{
				ClientID: command.ClientID,
				SerialID: command.SerialID,
			}
		}
		DPrintf("Server %d applied entry %d\n", kv.me, index)
	}
}
