package shardmaster

import (
	"log"
	"sync"
	"time"

	"github.com/dianchengwangCHN/raft-key-value-store/labgob"
	"github.com/dianchengwangCHN/raft-key-value-store/labrpc"
	"github.com/dianchengwangCHN/raft-key-value-store/raft"
)

const debug = 0

func dPrintf(format string, a ...interface{}) (n int, err error) {
	if debug > 0 {
		log.Printf(format, a...)
	}
	return
}

const agreementTimeoutInterval time.Duration = 2000

type doneMsg struct {
	ClientID int64
	SerialID uint
	Num      int
}

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	lastClerkSerialID map[int64]uint
	entryAppliedChs   map[int]chan doneMsg

	configs []Config // indexed by config num
}

func (sm *ShardMaster) isDone(clerkInfo ClientInfo) bool {
	sm.mu.Lock()
	v, ok := sm.lastClerkSerialID[clerkInfo.ClientID]
	sm.mu.Unlock()
	if ok {
		return v >= clerkInfo.SerialID
	}
	return false
}

// Returns wrongLeader, err, configIndex
func (sm *ShardMaster) startAgreement(args ClerkRPCArgs) (bool, Err, int) {
	clientInfo := args.GetClerkInfo()
	if sm.isDone(clientInfo) {
		return false, OK, -1
	}
	index, _, isLeader := sm.rf.Start(args)
	if !isLeader {
		return true, FAIL, -1
	}
	sm.mu.Lock()
	if _, ok := sm.entryAppliedChs[index]; !ok {
		sm.entryAppliedChs[index] = make(chan doneMsg, 1)
	}
	doneCh := sm.entryAppliedChs[index]
	sm.mu.Unlock()
	select {
	case <-time.After(agreementTimeoutInterval * time.Millisecond):
	case msg := <-doneCh:
		if msg.ClientID == clientInfo.ClientID && msg.SerialID == clientInfo.SerialID {
			close(doneCh)
			sm.mu.Lock()
			delete(sm.entryAppliedChs, index)
			sm.mu.Unlock()
			return false, OK, msg.Num
		}
		doneCh <- msg
	}
	return false, FAIL, -1
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	reply.WrongLeader, reply.Err, _ = sm.startAgreement(args.copy())
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	reply.WrongLeader, reply.Err, _ = sm.startAgreement(args.copy())
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	reply.WrongLeader, reply.Err, _ = sm.startAgreement(args.copy())
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	sm.mu.Lock()
	lastConfigIndex := sm.configs[len(sm.configs)-1].Num
	sm.mu.Unlock()

	if args.Num >= 0 && args.Num <= lastConfigIndex {
		reply.Config = sm.configs[args.Num].Copy()
		reply.Err = OK
		return
	}

	var index int
	reply.WrongLeader, reply.Err, index = sm.startAgreement(args.copy())
	if reply.WrongLeader || reply.Err != OK {
		return
	}
	reply.Config = sm.configs[index].Copy()
}

//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	sm.applyCh = make(chan raft.ApplyMsg)

	// Your code here.
	labgob.Register(JoinArgs{})
	labgob.Register(LeaveArgs{})
	labgob.Register(MoveArgs{})
	labgob.Register(QueryArgs{})

	sm.lastClerkSerialID = make(map[int64]uint)
	sm.entryAppliedChs = make(map[int]chan doneMsg)

	go sm.startApplyMsgDaemon()

	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	return sm
}

func (sm *ShardMaster) startApplyMsgDaemon() {
	for {
		msg := <-sm.applyCh
		if msg.CommandValid {
			index := msg.CommandIndex
			doneMsg := doneMsg{}
			clientInfo := ClientInfo{}
			if args, ok := msg.Command.(ClerkRPCArgs); ok {
				clientInfo = args.GetClerkInfo()
				doneMsg.ClientID, doneMsg.SerialID = clientInfo.ClientID, clientInfo.SerialID
			} else {
				continue
			}

			sm.mu.Lock()
			newConfig := sm.configs[len(sm.configs)-1].Copy()
			switch command := msg.Command.(type) {
			case JoinArgs:
				for gid, servers := range command.Servers {
					newConfig.Groups[gid] = append(newConfig.Groups[gid], servers...)
				}
				sm.reassignShards(&newConfig.Shards, newConfig.Groups)
				// dPrintf("server %d finished Join operation, now having %d groups\n", sm.me, len(newConfig.Groups))
			case LeaveArgs:
				for _, gid := range command.GIDs {
					delete(newConfig.Groups, gid)
				}
				sm.reassignShards(&newConfig.Shards, newConfig.Groups)
				// dPrintf("server %d finished Leave operation, now having %d groups\n", sm.me, len(newConfig.Groups))
			case MoveArgs:
				newConfig.Shards[command.Shard] = command.GID
			case QueryArgs:
				if command.Num < 0 || command.Num > sm.configs[len(sm.configs)-1].Num {
					doneMsg.Num = sm.configs[len(sm.configs)-1].Num
				} else {
					doneMsg.Num = command.Num
				}
				// dPrintf("server %d finished Query operation, configs lenght: %d, return index: %d\n", sm.me, len(sm.configs), doneMsg.Num)
			}
			if _, ok := msg.Command.(*QueryArgs); !ok {
				newConfig.Num++
				sm.configs = append(sm.configs, newConfig)
				dPrintf("server %d now has %d configs, last config Num: %d\n", sm.me, len(sm.configs), newConfig.Num)
			}
			if clientInfo.SerialID > sm.lastClerkSerialID[clientInfo.ClientID] {
				sm.lastClerkSerialID[clientInfo.ClientID] = clientInfo.SerialID
			}
			if ch, ok := sm.entryAppliedChs[index]; ok {
				select {
				case <-ch:
				default:
				}
				ch <- doneMsg
			}
			sm.mu.Unlock()
		}
	}
}

func (sm *ShardMaster) reassignShards(shards *[NShards]int, groups map[int][]string) {
	size := len(groups)
	if size == 0 {
		for i := range shards {
			shards[i] = 0
		}
		return
	}
	num, remainder := NShards/size, NShards%size
	counts := make(map[int]int) // gid -> count of shards
	for k := range groups {
		counts[k] = 0
	}
	unassigned := []int{}
	for i, gid := range shards {
		if gid == 0 {
			unassigned = append(unassigned, i)
		} else if _, ok := counts[gid]; !ok {
			unassigned = append(unassigned, i)
		} else if counts[gid] < num {
			counts[gid]++
		} else if counts[gid] == num && remainder > 0 {
			counts[gid]++
			remainder--
		} else {
			unassigned = append(unassigned, i)
		}
	}
	// dPrintf("server %d now has %d unassinged shards\n", sm.me, len(unassigned))
	index := 0
	for gid, count := range counts {
		for count < num {
			shards[unassigned[index]] = gid
			count++
			index++
		}
		if remainder > 0 {
			shards[unassigned[index]] = gid
			remainder--
			count++
			index++
		}
	}
}
