package raftkv

import (
	"bytes"
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
		DPrintf("server %d started agreement on %d\n", kv.me, index)
		kv.mu.Lock()
		if _, ok := kv.entryAppliedChs[index]; !ok {
			kv.entryAppliedChs[index] = make(chan DoneMsg, 1)
		}
		doneCh := kv.entryAppliedChs[index]
		kv.mu.Unlock()
		select {
		case <-time.After(AGREEMENTTIMEOUTInterval * time.Millisecond):
			DPrintf("server %d agreement on %d timeout\n", kv.me, index)
		case msg := <-doneCh:
			if msg.ClientID == command.ClientID && msg.SerialID == command.SerialID {
				close(doneCh)
				kv.mu.Lock()
				delete(kv.entryAppliedChs, index)
				kv.mu.Unlock()
				DPrintf("server %d update lastSerialID to %d for client %d\n", kv.me, msg.SerialID, msg.ClientID)
				return true
			}
		}
	}
	return false
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	var succeed bool
	reply.WrongLeader = true
	if kv.isDone(args.ClientID, args.SerialID) {
		DPrintf("server %d replied SerialID: %d is done\n", kv.me, args.SerialID)
		succeed = true
	} else {
		_, isLeader := kv.rf.GetState()
		if isLeader {
			reply.WrongLeader = false
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
	reply.Err = ErrOpFail
	if ID := kv.rf.GetLeaderID(); ID != -1 {
		reply.LeaderID = ID
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	var succeed bool
	reply.WrongLeader = true
	if kv.isDone(args.ClientID, args.SerialID) {
		succeed = true
	} else {
		_, isLeader := kv.rf.GetState()
		if isLeader {
			reply.WrongLeader = false
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
		reply.Err = OK
		DPrintf("server %d replied: succeed: %v, WrongLeader: %v\n", kv.me, succeed, reply.WrongLeader)
		return
	}
	DPrintf("server %d replied: succeed: %v, Wrongleader: %v\n", kv.me, succeed, reply.WrongLeader)
	reply.Err = ErrOpFail
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
		if msg.CommandValid {
			command := msg.Command.(Op)
			kv.mu.Lock()
			if command.SerialID > kv.lastClerkSerialID[command.ClientID] {
				switch command.Type {
				case "Put":
					kv.kvMap[command.Key] = command.Value
				case "Append":
					kv.kvMap[command.Key] += command.Value
				}
				kv.lastClerkSerialID[command.ClientID] = command.SerialID
				DPrintf("server %d updated SerialID of Client %d to %d\n", kv.me, command.ClientID, command.SerialID)
			}
			index := msg.CommandIndex
			DPrintf("Server %d applied entry %d\n", kv.me, index)
			if ch, ok := kv.entryAppliedChs[index]; ok {
				select {
				case <-ch:
				default:
				}
				kv.entryAppliedChs[index] <- DoneMsg{
					ClientID: command.ClientID,
					SerialID: command.SerialID,
				}
			} else {
				ch = make(chan DoneMsg, 1)
			}

			// check if need log compaction
			if kv.maxraftstate != -1 && kv.maxraftstate <= kv.rf.GetStateSize() {
				w := new(bytes.Buffer)
				e := labgob.NewEncoder(w)
				e.Encode(kv.kvMap)
				e.Encode(kv.lastClerkSerialID)
				snapshot := w.Bytes()
				go kv.rf.StartSnapshot(snapshot, msg.CommandIndex)
			}
			kv.mu.Unlock()
		} else {
			r := bytes.NewBuffer(msg.Snapshot)
			d := labgob.NewDecoder(r)
			kv.mu.Lock()
			kv.kvMap = make(map[string]string)
			kv.lastClerkSerialID = make(map[int64]uint)
			d.Decode(&kv.kvMap)
			d.Decode(&kv.lastClerkSerialID)
			kv.mu.Unlock()
		}
	}
}
