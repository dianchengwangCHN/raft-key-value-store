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

type ServerState int

const (
	LEADER    ServerState = 0
	CANDIDATE ServerState = 1
	FOLLOWER  ServerState = 2
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

type LogEntry struct {
	Command    interface{}
	EntryIndex int
	EntryTerm  int
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
	voteRecv int         // the number of votes that have been received

	//state the Leader need to maintain
	nextIndex  []int // index of the next log entry
	matchIndex []int // index of the highest log entry known to be replicated on each server

	applyCh     chan ApplyMsg // channel to send ApplyMsg
	heartbeatCh chan bool     // channel to receive timeout signal
	electionCh  chan bool     // channel to receive signal claim to be the leader
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
	voteGranted := false
	rf.mu.Lock()
	defer rf.mu.Lock()
	reply.Term = rf.currentTerm
	if args.Term >= rf.currentTerm && (rf.votedFor == -1 || args.LastLogTerm >= rf.currentTerm && args.LastLogIndex >= rf.lastApplied) {
		voteGranted = true
		rf.state = FOLLOWER
		fmt.Printf("server %d voted to %d\n", rf.me, args.CandidateID)
	}
	reply.VoteGranted = voteGranted
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
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	fmt.Printf("server %d got the response from %d\n", rf.me, server)
	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if rf.state != CANDIDATE || rf.currentTerm > reply.Term {
			return ok
		}
		if reply.VoteGranted {
			rf.voteRecv++
			if rf.voteRecv > len(rf.peers)/2 {
				rf.state = LEADER
				rf.electionCh <- true
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
	success := false
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm

	reply.Success = success
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
	var index int = -1
	var term int = -1
	var isLeader bool = true

	// Your code here (2B).

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
		EntryIndex: 0,
		EntryTerm:  0,
	})
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, 0)
	rf.matchIndex = make([]int, 0)

	rf.applyCh = applyCh
	rf.heartbeatCh = make(chan bool)
	rf.electionCh = make(chan bool)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// goroutine to maintain the state machine
	go func() {
		for {
			switch rf.state {
			case FOLLOWER:
				rf.seed = rand.NewSource(int64(rf.me))
				for rf.state == FOLLOWER {
					select {
					case <-time.After(time.Duration(300+rand.New(rf.seed).Intn(300)) * time.Millisecond):
						rf.state = CANDIDATE
						fmt.Printf("server %d becomes a CANDIDATE\n", rf.me)
						break
					}
				}
				break
			case LEADER:
				break
			case CANDIDATE:
				for rf.state == CANDIDATE {
					rf.mu.Lock()
					rf.currentTerm++
					rf.votedFor = rf.me
					term := rf.currentTerm
					lastLogEntry := rf.log[len(rf.log)-1]
					rf.voteRecv = 0
					rf.mu.Unlock()

					// start leader election
					args := &RequestVoteArgs{
						Term:         term,
						CandidateID:  rf.me,
						LastLogTerm:  lastLogEntry.EntryTerm,
						LastLogIndex: lastLogEntry.EntryIndex,
					}
					go func(args *RequestVoteArgs) {
						for i := range rf.peers {
							if rf.state == CANDIDATE && i != rf.me {
								reply := &RequestVoteReply{}
								go rf.sendRequestVote(i, args, reply)
								fmt.Printf("server %d sent RequestVote to %d\n", rf.me, i)
							}
						}
					}(args)

					select {
					case <-time.After(time.Duration(300+rand.New(rf.seed).Intn(300)) * time.Millisecond):
						break
					case <-rf.electionCh:
						break
					}
				}
				break
			}
		}
	}()

	return rf
}
