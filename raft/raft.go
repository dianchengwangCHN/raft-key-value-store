package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"math/rand"
	"sync"
	"time"

	"github.com/dianchengwangCHN/raft-key-value-store/labgob"
	"github.com/dianchengwangCHN/raft-key-value-store/labrpc"
)

// ServerState defines the 3 server states
type ServerState int

const (
	LEADER    ServerState = 0
	CANDIDATE ServerState = 1
	FOLLOWER  ServerState = 2

	HEARTBEATInterval int = 100
	TIMEOUTInterval   int = 500
	RANDOMInterval    int = 500
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	Snapshot     []byte
}

// LogEntry defines the data model of each log entry
type LogEntry struct {
	Command    interface{}
	EntryTerm  int
	EntryIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state       ServerState // state of server
	currentTerm int         // latest term server has seen
	votedFor    int         // candidateID
	log         []LogEntry  // log entries
	commitIndex int         // index of highest log entry known to be committed
	lastApplied int         // index of highest log entry applied to state machine

	seed     rand.Source // the source used to generate random timeout duration
	timer    *time.Timer // the timer used for timeout
	voteRecv int         // the number of votes that have been received

	//state the Leader need to maintain
	nextIndex  []int // index of the next log entry
	matchIndex []int // index of the highest log entry known to be replicated on each server

	applyCh        chan ApplyMsg // channel to send ApplyMsg
	stateUpdateCh  chan struct{} // channel to receive signal indicating server state has changed
	commitUpdateCh chan struct{} // channel to receive signal indicating commitIndex has changed
}

// GetState returns currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// rf.mu.Lock()
	// defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == LEADER
}

// GetLeaderID returns the server ID that get the vote from the current server
func (rf *Raft) GetLeaderID() int {
	return rf.votedFor
}

// GetStateSize returns the current size of raft state in persister
func (rf *Raft) GetStateSize() int {
	return rf.persister.RaftStateSize()
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	data := rf.encodeState()
	rf.persister.SaveRaftState(data)
}

func (rf *Raft) encodeState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	return data
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersistState(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []LogEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil {
		// DPrintf("Error: server %d cannot resotre previously persisted state\n", rf.me)
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
	}
}

func (rf *Raft) readPersistSnapshot(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}

	DPrintf("server %d sent Snapshot\n", rf.me)
	rf.applyCh <- ApplyMsg{
		CommandValid: false,
		Snapshot:     data,
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// RequestVote is the RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	DPrintf("server %d tried to get the Lock to 'Vote'\n", rf.me)
	rf.mu.Lock()
	DPrintf("server %d got the Lock to 'Vote'\n", rf.me)
	defer rf.mu.Unlock()
	reply.VoteGranted = false

	// Reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}

	// If RPC request or response contains term T > currentTerm: set currentTerm = T,
	// convert to FOLLOWER
	update := false
	if args.Term > rf.currentTerm {
		update = true
		rf.votedFor = -1
		rf.state = FOLLOWER
	}

	// If (votedFor is null or candidateId) and candidate's log is at least
	// as up-to-date as receiver's log, grant vote
	if rf.votedFor == -1 || rf.votedFor == args.CandidateID {
		if args.LastLogTerm > rf.log[len(rf.log)-1].EntryTerm || args.LastLogTerm == rf.log[len(rf.log)-1].EntryTerm && args.LastLogIndex >= rf.log[len(rf.log)-1].EntryIndex {
			rf.votedFor = args.CandidateID
			reply.VoteGranted = true
			if update {
				rf.currentTerm = args.Term
				reply.Term = rf.currentTerm
			}
			rf.state = FOLLOWER
			rf.stateUpdateCh <- struct{}{}
			rf.persist()
			// DPrintf("server %d votes to %d, term%d\n", rf.me, args.CandidateID, rf.currentTerm)
			return
		}
	}
	if update {
		rf.currentTerm = args.Term
		rf.persist()
	}
	reply.Term = rf.currentTerm
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	// DPrintf("server %d sent RequestVote to %d, term%d\n", rf.me, server, rf.currentTerm)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if ok {
		DPrintf("server %d tried to get the Lock to 'sendVote'\n", rf.me)
		rf.mu.Lock()
		DPrintf("server %d got the Lock to 'sendVote'\n", rf.me)
		defer rf.mu.Unlock()

		// If RPC request or response contains term T > currentTerm: set currentTerm = T,
		// convert to FOLLOWER
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.votedFor = -1
			rf.state = FOLLOWER
			rf.persist()
			return ok
		}

		if rf.state != CANDIDATE || rf.currentTerm > reply.Term {
			return ok
		}
		if reply.VoteGranted {
			rf.voteRecv++
			if rf.voteRecv > len(rf.peers)/2 {
				rf.state = LEADER
				// reintialize nextIndex and matchIndex after election
				for i := range rf.nextIndex {
					rf.nextIndex[i] = rf.log[len(rf.log)-1].EntryIndex + 1
					rf.matchIndex[i] = 0
				}
				// DPrintf("server %d claims to be the leader, term%d\n", rf.me, rf.currentTerm)
				rf.stateUpdateCh <- struct{}{}
			}
		}
	}
	return ok
}

// AppendEntriesArgs is the data model of the AppendEntries RPC arguments
type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

// AppendEntriesReply is the data model of the AppendEntries RPC reply
type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictIndex int
}

// AppendEntries is the AppendEntries RPC handler.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	DPrintf("server %d tried to get the Lock to 'appendEntries'\n", rf.me)
	rf.mu.Lock()
	DPrintf("server %d got the Lock to 'appendEntries'\n", rf.me)
	defer rf.mu.Unlock()

	// update term and state if needed
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = FOLLOWER
	}

	if rf.state == LEADER {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	defer rf.persist()

	if args.Term < rf.currentTerm {
		reply.Success = false
		// DPrintf("server %d rejects Heartbeat from %d, term%d, args{index: %d, term: %d}\n", rf.me, args.LeaderID, rf.currentTerm, args.PrevLogIndex, args.PrevLogTerm)
	} else if offset := rf.log[0].EntryIndex; args.PrevLogIndex > rf.log[len(rf.log)-1].EntryIndex ||
		args.PrevLogIndex < offset || args.PrevLogTerm != rf.log[args.PrevLogIndex-offset].EntryTerm {
		reply.Success = false
		if args.PrevLogIndex > rf.log[len(rf.log)-1].EntryIndex {
			reply.ConflictIndex = rf.log[len(rf.log)-1].EntryIndex + 1
		} else {
			index := args.PrevLogIndex - offset - 1
			for index > 1 && rf.log[index-1].EntryTerm == rf.log[index].EntryTerm {
				index--
			}
			if index < 0 {
				index = 0
			}
			reply.ConflictIndex = rf.log[index].EntryIndex
		}
		// DPrintf("server %d rejects Heartbeat from %d, term%d, args{index: %d, term: %d}\n", rf.me, args.LeaderID, rf.currentTerm, args.PrevLogIndex, args.PrevLogTerm)
	} else {
		// DPrintf("server %d, term%d accepts AppendEntries form %d, term%d, args{index: %d, term: %d, commit: %d}\n", rf.me, rf.currentTerm, args.LeaderID, args.Term, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit)
		reply.Success = true
		// valid request keeps server as follower
		rf.votedFor = -1
		rf.state = FOLLOWER
		rf.stateUpdateCh <- struct{}{}

		// update entries
		offset := rf.log[0].EntryIndex
		i, j := args.PrevLogIndex+1-offset, 0
		for ; i < len(rf.log) && j < len(args.Entries); i, j = i+1, j+1 {
			if rf.log[i].EntryTerm != args.Entries[j].EntryTerm {
				break
			}
		}

		// delete entries that do not match and append new entries if any
		if j < len(args.Entries) {
			rf.log = append(rf.log[:i], args.Entries[j:]...)
			// DPrintf("server %d appends entries from index%d to index%d, term%d\n", rf.me, args.PrevLogIndex+1, len(rf.log)-1, rf.currentTerm)
		}

		// update commitIndex
		if args.LeaderCommit > rf.commitIndex {
			lastNewLogIndex := rf.log[len(rf.log)-1].EntryIndex
			var min int
			if args.LeaderCommit < lastNewLogIndex {
				min = args.LeaderCommit
			} else {
				min = lastNewLogIndex
			}
			if rf.log[min-offset].EntryTerm == rf.currentTerm {
				rf.commitIndex = min
				rf.commitUpdateCh <- struct{}{}
				// DPrintf("server %d commits index %d, log length: %d, term%d\n", rf.me, rf.commitIndex, len(rf.log), rf.currentTerm)
			}
		}
		// DPrintf("server %d commitIndex: %d, leaderCommit: %d\n", rf.me, rf.commitIndex, args.LeaderCommit)
	}
	reply.Term = rf.currentTerm
	// if len(args.Entries) > 0 {
	// 	DPrintf("server %d receives from %d, args{index: %d, term: %d}, reply: %t\n", rf.me, args.LeaderID, args.PrevLogIndex, args.PrevLogTerm, reply.Success)
	// } else {
	// 	DPrintf("server %d receives Heartbeat from %d, term%d, args{index: %d, term: %d}\n", rf.me, args.LeaderID, rf.currentTerm, args.PrevLogIndex, args.PrevLogTerm)
	// }
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	// DPrintf("leader %d sends AppendEntries to %d, term %d, args{index: %d, term: %d}, return: %t\n", rf.me, server, rf.currentTerm, args.PrevLogIndex, args.PrevLogTerm, ok)
	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		/*
		 * If RPC request or response contains term T > currentTerm: set currentTerm = T,
		 * convert to FOLLOWER
		 */
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.votedFor = -1
			rf.state = FOLLOWER
			rf.stateUpdateCh <- struct{}{}
			rf.persist()
			// DPrintf("leader %d becomes follower\n", rf.me)
			return ok
		}

		if args.PrevLogIndex+len(args.Entries) >= rf.nextIndex[server] {
			if reply.Success {
				rf.nextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
				rf.matchIndex[server] = rf.nextIndex[server] - 1

				// update commitIndex
				if rf.matchIndex[server] > rf.commitIndex {
					offset, N := rf.log[0].EntryIndex, rf.matchIndex[server]
					// DPrintf("EntryIndex: %d, Index in log: %d, log length: %d\n", N, N-offset, len(rf.log))
					if rf.log[N-offset].EntryTerm == rf.currentTerm {
						count := 0
						for _, index := range rf.matchIndex {
							if index >= N {
								count++
								if count > len(rf.matchIndex)/2 {
									rf.commitIndex = N
									rf.commitUpdateCh <- struct{}{}
									// DPrintf("leader %d commits Entries %d\n", rf.me, rf.commitIndex)
									break
								}
							}
						}
					}
				}
			} else {
				if reply.ConflictIndex != -1 {
					rf.nextIndex[server] = reply.ConflictIndex
					ok = false
				}
			}
		}
	}
	return ok
}

// send AppendEntries RPC to all other servers
func (rf *Raft) broadcastAppendEntries(isHeartbeat bool) {
	for i := range rf.nextIndex {
		if rf.state != LEADER {
			return
		}
		if i == rf.me {
			continue
		}
		go func(i int) {
			for {
				var ok bool
				rf.mu.Lock()
				if rf.state != LEADER || !isHeartbeat && rf.log[len(rf.log)-1].EntryIndex < rf.nextIndex[i] {
					rf.mu.Unlock()
					break
				}
				offset := rf.log[0].EntryIndex
				term := rf.currentTerm
				leaderID := rf.me
				if offset < rf.nextIndex[i] { // regular Heartberat and AppendEntries
					prevLogIndex := rf.nextIndex[i] - 1
					leaderCommit := rf.commitIndex

					args := &AppendEntriesArgs{
						Term:         term,
						LeaderID:     leaderID,
						PrevLogIndex: prevLogIndex,
						PrevLogTerm:  rf.log[prevLogIndex-offset].EntryTerm,
						LeaderCommit: leaderCommit,
					}
					if isHeartbeat {
						args.Entries = make([]LogEntry, 0)
					} else {
						args.Entries = make([]LogEntry, rf.log[len(rf.log)-1].EntryIndex-prevLogIndex)
						copy(args.Entries, rf.log[prevLogIndex+1-offset:])
					}
					rf.mu.Unlock()
					reply := &AppendEntriesReply{
						ConflictIndex: -1,
					}
					ok = rf.sendAppendEntries(i, args, reply)
					// DPrintf("leader %d sends AppendEntries to %d, term%d, commit%d %t\n", rf.me, i, rf.currentTerm, args.LeaderCommit, ok)
				} else { // send snapshot
					lastIncludedIndex := rf.log[0].EntryIndex
					lastIncludedTerm := rf.log[0].EntryTerm
					data := rf.persister.ReadSnapshot()
					rf.mu.Unlock()

					args := &InstallSnapshotArgs{
						Term:              term,
						LeaderID:          leaderID,
						LastIncludedIndex: lastIncludedIndex,
						LastIncludedTerm:  lastIncludedTerm,
						Data:              data,
					}
					reply := &InstallSnapshotReply{}
					ok = rf.sendInstallSnapshot(i, args, reply)
				}
				if ok || isHeartbeat {
					break
				}
				time.Sleep(10 * time.Millisecond)
			}
			// if i != rf.me {
			// 	DPrintf("leader %d sends AppendEntries to %d, term%d\n", rf.me, i, rf.currentTerm)
			// }
		}(i)
	}
}

// InstallSnapshotArgs is the data model of the InstallSnapshot RPC arguments
type InstallSnapshotArgs struct {
	Term              int
	LeaderID          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

// InstallSnapshotReply is the data model of the InstallSnapshot RPC reply
type InstallSnapshotReply struct {
	Term int
}

// InstallSnapshot is the InstallSnapshot RPC handler
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	DPrintf("server %d tried to get the Lock to 'installSnapshot'\n", rf.me)
	rf.mu.Lock()
	DPrintf("server %d got the Lock to 'installSnapshot'\n", rf.me)

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = FOLLOWER
	}
	rf.stateUpdateCh <- struct{}{}

	reply.Term = rf.currentTerm

	if args.LastIncludedTerm <= rf.log[0].EntryIndex {
		rf.mu.Unlock()
		return
	}

	lastIndex := rf.log[len(rf.log)-1].EntryIndex
	size := 1
	if args.LastIncludedIndex < lastIndex {
		size += lastIndex - args.LastIncludedIndex
	}

	// update log
	newLog := make([]LogEntry, size)
	newLog[0] = LogEntry{
		EntryIndex: args.LastIncludedIndex,
		EntryTerm:  args.LastIncludedTerm,
	}
	if size > 1 {
		offset := rf.log[0].EntryIndex
		copy(newLog[1:], rf.log[args.LastIncludedIndex+1-offset:])
	}

	if args.LastIncludedIndex > rf.lastApplied {
		rf.lastApplied = args.LastIncludedIndex
		if args.LastIncludedIndex > rf.commitIndex {
			rf.commitIndex = rf.lastApplied
		}
	}
	// DPrintf("server %d first index: %d, lastIncludedIndex: %d\n", rf.me, newLog[0].EntryIndex, args.LastIncludedIndex)

	// if lastIndex := rf.log[len(rf.log)-1].EntryIndex; lastIndex > args.LastIncludedIndex {
	// offset := rf.log[0].EntryIndex
	// newLog = append(newLog, rf.log[args.LastIncludedIndex+1-offset:]...)
	// }
	// for i, offset, lastIndex := args.LastIncludedIndex+1, rf.log[0].EntryIndex, rf.log[len(rf.log)-1].EntryIndex; i <= lastIndex; i++ {
	// 	newLog = append(newLog, rf.log[i-offset])
	// }
	rf.log = newLog
	// DPrintf("server %d newLog length: %d, lastIncludeIndex: %d, first index: %d\n", rf.me, len(rf.log), args.LastIncludedIndex, newLog[0].EntryIndex)

	// persist state and snapshot
	stateData := rf.encodeState()
	rf.persister.SaveStateAndSnapshot(stateData, args.Data)
	rf.mu.Unlock()
	DPrintf("server %d send Snapshot\n", rf.me)
	rf.readPersistSnapshot(args.Data)
	DPrintf("server %d finished installSnapshot\n", rf.me)
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)

	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		/*
		 * If RPC request or response contains term T > currentTerm: set currentTerm = T,
		 * convert to FOLLOWER
		 */
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.votedFor = -1
			rf.state = FOLLOWER
			rf.stateUpdateCh <- struct{}{}
			rf.persist()
			// DPrintf("leader %d becomes follower\n", rf.me)
			return ok
		}

		rf.nextIndex[server] = args.LastIncludedIndex + 1
		rf.matchIndex[server] = args.LastIncludedIndex
	}

	return ok
}

// StartSnapshot let Raft install the snapshot and compact the log
func (rf *Raft) StartSnapshot(snapshot []byte, lastIncludedIndex int) {
	DPrintf("server %d tried to get the Lock to 'startSnapshot'\n", rf.me)
	rf.mu.Lock()
	DPrintf("server %d got the Lock to 'startSnapshot'\n", rf.me)
	defer rf.mu.Unlock()

	offset := rf.log[0].EntryIndex

	if lastIncludedIndex <= offset {
		return
	}
	// lastIndex := rf.log[len(rf.log)-1].EntryIndex
	// DPrintf("server %d lastIndex: %d, firstIndex: %d, lastIncludedIndex: %d\n", rf.me, lastIndex, offset, lastIncludedIndex)

	newLog := make([]LogEntry, 0)
	newLog = append(newLog, LogEntry{
		EntryIndex: lastIncludedIndex,
		EntryTerm:  rf.log[lastIncludedIndex-offset].EntryTerm,
	})
	// DPrintf("server %d first index: %d, lastIncludedIndex: %d\n", rf.me, newLog[0].EntryIndex, lastIncludedIndex)
	temp := make([]LogEntry, rf.log[len(rf.log)-1].EntryIndex-lastIncludedIndex)
	copy(temp, rf.log[lastIncludedIndex+1-offset:])
	newLog = append(newLog, temp...)
	// for i, offset, lastIndex := lastIncludedIndex+1, rf.log[0].EntryIndex, rf.log[len(rf.log)-1].EntryIndex; i <= lastIndex; i++ {
	// 	newLog = append(newLog, rf.log[i-offset])
	// }

	rf.log = newLog
	// DPrintf("server %d newLog length: %d, lastIncludedIndex: %d, first index: %d\n", rf.me, len(rf.log), lastIncludedIndex, newLog[0].EntryIndex)
	// persist state and snapshot
	stateData := rf.encodeState()
	rf.persister.SaveStateAndSnapshot(stateData, snapshot)
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	var index int

	DPrintf("server %d tried to get the Lock to 'Start'\n", rf.me)
	rf.mu.Lock()
	DPrintf("server %d got Lock to 'Start'\n", rf.me)
	term := rf.currentTerm
	isLeader := rf.state == LEADER

	if isLeader {
		index = rf.log[len(rf.log)-1].EntryIndex + 1
		rf.log = append(rf.log, LogEntry{
			Command:    command,
			EntryTerm:  rf.currentTerm,
			EntryIndex: index,
		})
		rf.persist()
		// DPrintf("leader %d appends index %d, log length: %d, term%d\n", rf.me, index, len(rf.log), rf.currentTerm)
		rf.nextIndex[rf.me] = index + 1
		rf.matchIndex[rf.me] = index
		rf.mu.Unlock()
		go rf.broadcastAppendEntries(false)
	} else {
		rf.mu.Unlock()
	}
	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	rf.timer.Stop()
	rf.state = FOLLOWER
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.state = FOLLOWER
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]LogEntry, 0)
	rf.log = append(rf.log, LogEntry{
		EntryTerm:  0,
		EntryIndex: 0,
	})

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.seed = rand.NewSource(int64(rf.me))
	rf.timer = time.NewTimer(0)
	rf.voteRecv = 0

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	rf.applyCh = applyCh
	rf.stateUpdateCh = make(chan struct{})
	rf.commitUpdateCh = make(chan struct{}, 100)

	// initialize from state persisted before a crash
	DPrintf("server %d started\n", rf.me)
	rf.readPersistState(rf.persister.ReadRaftState())
	rf.readPersistSnapshot(rf.persister.ReadSnapshot())

	// start goroutine to maintain the server state
	go rf.startStateMachineDaemon()

	// start goroutine to monitor log entry and apply command to state machine
	go rf.startLogCommitterDaemon()

	return rf
}

func (rf *Raft) startStateMachineDaemon() {
	for {
		state := rf.state
		switch state {
		case FOLLOWER:
			rf.timer.Reset(time.Duration(TIMEOUTInterval+rand.New(rf.seed).Intn(RANDOMInterval)) * time.Millisecond)
			select {
			case <-rf.timer.C:
				rf.mu.Lock()
				rf.state = CANDIDATE
				rf.mu.Unlock()
			case <-rf.stateUpdateCh:
				rf.timer.Stop()
			}
		case LEADER:
			go rf.broadcastAppendEntries(true)
			rf.timer.Reset(time.Duration(HEARTBEATInterval) * time.Millisecond)
			select {
			case <-rf.timer.C:
			case <-rf.stateUpdateCh:
				rf.timer.Stop()
			}
		case CANDIDATE:
			rf.mu.Lock()
			rf.currentTerm++
			// DPrintf("server %d starts the election, term%d\n", rf.me, rf.currentTerm)
			rf.votedFor = rf.me
			rf.voteRecv = 1
			term := rf.currentTerm
			lastLogIndex := rf.log[len(rf.log)-1].EntryIndex
			lastLogTerm := rf.log[len(rf.log)-1].EntryTerm
			rf.mu.Unlock()

			// start leader election
			args := &RequestVoteArgs{
				Term:         term,
				CandidateID:  rf.me,
				LastLogTerm:  lastLogTerm,
				LastLogIndex: lastLogIndex,
			}
			for i := range rf.peers {
				if rf.state == CANDIDATE && i != rf.me {
					go func(args *RequestVoteArgs, i int) {
						reply := &RequestVoteReply{}
						rf.sendRequestVote(i, args, reply)
					}(args, i)
				}
			}
			rf.timer.Reset(time.Duration(TIMEOUTInterval+rand.New(rf.seed).Intn(RANDOMInterval)) * time.Millisecond)
			select {
			case <-rf.timer.C:
			case <-rf.stateUpdateCh:
				rf.timer.Stop()
			}
		}
	}
}

func (rf *Raft) startLogCommitterDaemon() {
	for {
		<-rf.commitUpdateCh

		for i, commitIndex := rf.lastApplied+1, rf.commitIndex; i <= commitIndex; i++ {
			DPrintf("server %d LogCommitter tried to get the Lock\n", rf.me)
			rf.mu.Lock()
			DPrintf("server %d LogCommitter got the Lock\n", rf.me)
			if offset := rf.log[0].EntryIndex; i > offset {
				rf.applyCh <- ApplyMsg{
					CommandValid: true,
					Command:      rf.log[i-offset].Command,
					CommandIndex: rf.log[i-offset].EntryIndex,
				}
				rf.lastApplied = i
			} else {
				i = offset
			}
			rf.mu.Unlock()
			// DPrintf("server %d applies Entries %d\n", rf.me, i)
		}
	}
}
