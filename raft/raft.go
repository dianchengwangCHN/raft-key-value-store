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
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/dianchengwangCHN/raft-key-value-store/labrpc"
	// "bytes"
	// "labgob"
)

// ServerState defines the 3 server states
type ServerState int

const (
	LEADER    ServerState = 0
	CANDIDATE ServerState = 1
	FOLLOWER  ServerState = 2

	HEARTBEATInterval int = 125
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
}

// LogEntry defines the data model of each log entry
type LogEntry struct {
	Command   interface{}
	EntryTerm int
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

	applyCh        chan ApplyMsg    // channel to send ApplyMsg
	stateUpdateCh  chan ServerState // channel to receive signal indicating server state has changed
	commitUpdateCh chan struct{}    // channel to receive signal indicating commitIndex has changed
}

// GetState returns currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	return rf.currentTerm, rf.state == LEADER
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
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
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// RequestVote is the RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	reply.VoteGranted = false
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}

	/*
	 * If RPC request or response contains term T > currentTerm: set currentTerm = T,
	 * convert to FOLLOWER
	 */
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.stateUpdateCh <- FOLLOWER
	}

	/*
	 * If (votedFor is null or candidateId) and candidate's log is at least
	 * as up-to-date as receiver's log, grant vote
	 */
	if rf.votedFor == -1 || rf.votedFor == args.CandidateID {
		if args.Term > rf.currentTerm || args.LastLogIndex >= len(rf.log)-1 {
			rf.votedFor = args.CandidateID
			reply.VoteGranted = true
			fmt.Printf("server %d votes to %d, term%d\n", rf.me, args.CandidateID, rf.currentTerm)
		}
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
	// fmt.Printf("server %d sent RequestVote to %d, term%d\n", rf.me, server, rf.currentTerm)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		/*
		 * If RPC request or response contains term T > currentTerm: set currentTerm = T,
		 * convert to FOLLOWER
		 */
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.stateUpdateCh <- FOLLOWER
			return ok
		}

		if rf.state != CANDIDATE || rf.currentTerm > reply.Term {
			return ok
		}
		if reply.VoteGranted {
			rf.voteRecv++
			if rf.voteRecv > len(rf.peers)/2 {
				rf.stateUpdateCh <- LEADER

				// reintialize nextIndex and matchIndex after election
				for i := range rf.nextIndex {
					rf.nextIndex[i] = len(rf.log)
					rf.matchIndex[i] = 0
				}
				fmt.Printf("server %d claimed to be the leader\n", rf.me)
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
	Term    int
	Success bool
}

// AppendEntries is the AppendEntries RPC handler.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm || args.PrevLogIndex >= len(rf.log) || args.PrevLogTerm != rf.log[args.PrevLogIndex].EntryTerm {
		reply.Success = false
	} else {
		fmt.Printf("server %d receives AppendEntries form %d, term%d\n", rf.me, args.LeaderID, rf.currentTerm)
		reply.Success = true
		rf.state = FOLLOWER // to make sure the state change instantly
		rf.stateUpdateCh <- FOLLOWER

		// update term if needed
		if args.Term > rf.currentTerm {
			rf.currentTerm = args.Term
		}

		// update entries
		if len(args.Entries) > 0 {
			i, j := args.PrevLogIndex+1, 0
			for ; i < len(rf.log) && j < len(args.Entries); i, j = i+1, j+1 {
				if rf.log[i].EntryTerm != args.Entries[j].EntryTerm {
					break
				}
			}
			rf.log = append(rf.log[:i], args.Entries[j:]...)
		}

		// update commitIndex
		if args.LeaderCommit > rf.commitIndex {
			lastNewLogIndex := args.PrevLogIndex + len(args.Entries)
			if args.LeaderCommit < lastNewLogIndex {
				rf.commitIndex = args.LeaderCommit
			}
			rf.commitIndex = lastNewLogIndex
			rf.commitUpdateCh <- struct{}{}
		}
	}
	reply.Term = rf.currentTerm
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	fmt.Printf("server %d sends AppendEntries to %d, term%d, %v\n", rf.me, server, rf.currentTerm, rf.state)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		/*
		 * If RPC request or response contains term T > currentTerm: set currentTerm = T,
		 * convert to FOLLOWER
		 */
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.stateUpdateCh <- FOLLOWER
			return ok
		}

		if len(args.Entries) > 0 && args.PrevLogIndex == rf.nextIndex[server]-1 {
			if reply.Success {
				rf.nextIndex[server] += len(args.Entries)
				rf.matchIndex[server] = rf.nextIndex[server] - 1
				if rf.matchIndex[server] > rf.commitIndex {
					N := rf.matchIndex[server]
					count := 0
					for _, index := range rf.matchIndex {
						if index >= N {
							count++
							if count > len(rf.matchIndex)/2 && rf.log[N].EntryTerm == rf.currentTerm {
								rf.commitIndex = N
								rf.commitUpdateCh <- struct{}{}
								break
							}
						}
					}
				}
			} else {
				rf.nextIndex[server]--

			}
		}
	}
	return ok
}

// send AppendEntries RPC to all other servers
func (rf *Raft) broadcastAppendEntries(isHeartbeat bool) {
	for i, nextIndex := range rf.nextIndex {
		if rf.state != LEADER {
			return
		}
		if i != rf.me {
			if !isHeartbeat && len(rf.log) > nextIndex {
				continue
			}
			go func(i int) {
				rf.mu.Lock()
				term := rf.currentTerm
				prevLogIndex := rf.nextIndex[i] - 1
				leaderCommit := rf.commitIndex
				rf.mu.Unlock()

				args := &AppendEntriesArgs{
					Term:         term,
					LeaderID:     rf.me,
					PrevLogIndex: prevLogIndex,
					PrevLogTerm:  rf.log[prevLogIndex].EntryTerm,
					LeaderCommit: leaderCommit,
				}
				if isHeartbeat {
					args.Entries = make([]LogEntry, 0)
				} else {
					rf.mu.Lock()
					args.Entries = make([]LogEntry, len(rf.log)-prevLogIndex-1)
					copy(args.Entries, rf.log[prevLogIndex+1:])
					rf.mu.Unlock()
				}
				reply := &AppendEntriesReply{}
				rf.sendAppendEntries(i, args, reply)
			}(i)
		}
	}
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

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := rf.currentTerm
	isLeader := rf.state == LEADER

	if isLeader {
		rf.log = append(rf.log, LogEntry{
			Command:   command,
			EntryTerm: rf.currentTerm,
		})
		index = len(rf.log) - 1
		rf.nextIndex[rf.me] = index + 1
		rf.matchIndex[rf.me] = index
		go rf.broadcastAppendEntries(false)
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
	// Your code here, if desired.
}

func (rf *Raft) updateState(state ServerState) {
	rf.state = state
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

	// Your initialization code here (2A, 2B, 2C).
	rf.state = FOLLOWER
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]LogEntry, 0)
	rf.log = append(rf.log, LogEntry{
		EntryTerm: 0,
	})

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.seed = rand.NewSource(int64(rf.me))
	rf.timer = time.NewTimer(0)
	rf.voteRecv = 0

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	rf.applyCh = applyCh
	rf.stateUpdateCh = make(chan ServerState, 5)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// goroutine to maintain the state machine
	go func() {
		for {
			switch rf.state {
			case FOLLOWER:
				rf.timer.Reset(time.Duration(375+rand.New(rf.seed).Intn(375)) * time.Millisecond)
				select {
				case <-rf.timer.C:
					rf.updateState(CANDIDATE)
					// fmt.Printf("server %d becomes a CANDIDATE\n", rf.me)
				case state := <-rf.stateUpdateCh:
					rf.updateState(state)
					rf.timer.Stop()
				}
				break
			case LEADER:
				go rf.broadcastAppendEntries(true)
				time.Sleep(time.Duration(HEARTBEATInterval) * time.Millisecond)
				break
			case CANDIDATE:
				rf.mu.Lock()
				rf.currentTerm++
				rf.votedFor = rf.me
				rf.voteRecv = 1
				term := rf.currentTerm
				lastLogIndex := len(rf.log) - 1
				lastLogTerm := rf.log[lastLogIndex].EntryTerm
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
				rf.timer.Reset(time.Duration(375+rand.New(rf.seed).Intn(375)) * time.Millisecond)
				select {
				case <-rf.timer.C:
				case state := <-rf.stateUpdateCh:
					rf.updateState(state)
					rf.timer.Stop()
				}
				break
			}
		}
	}()

	go func() {
		for {
			select {
			case <-rf.commitUpdateCh:
				rf.mu.Lock()
				for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
					rf.applyCh <- ApplyMsg{
						CommandValid: true,
						Command:      rf.log[i].Command,
						CommandIndex: i,
					}
					rf.lastApplied = i
				}
				rf.mu.Unlock()
			}
		}
	}()
	return rf
}
