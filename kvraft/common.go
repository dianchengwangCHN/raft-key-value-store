package raftkv

const (
	OK        = "OK"
	ErrNoKey  = "ErrNoKey"
	ErrOpFail = "ErrOpFail"
)

type Err string

// ClerkRPCReply defines two commonly used methods for different types of client RPC replies.
type ClerkRPCReply interface {
	GetWrongLeader() bool
	GetErr() Err
	GetLeaderID() int
	SetLeaderID(leaderID int)
}

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientID int64
	SerialID uint
}

type PutAppendReply struct {
	WrongLeader bool
	Err         Err
	LeaderID    int
}

// GetWrongLeader returns the WrongLeader field of PutAppendReply
func (reply *PutAppendReply) GetWrongLeader() bool {
	return reply.WrongLeader
}

// GetErr returns the Err field of PutAppendReply
func (reply *PutAppendReply) GetErr() Err {
	return reply.Err
}

// GetLeaderID returns the LeaderID field of PutAppendReply
func (reply *PutAppendReply) GetLeaderID() int {
	return reply.LeaderID
}

// SetLeaderID sets the LeaderID field of PutAppendReply
func (reply *PutAppendReply) SetLeaderID(leaderID int) {
	reply.LeaderID = leaderID
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientID int64
	SerialID uint
}

type GetReply struct {
	WrongLeader bool
	Err         Err
	Value       string
	LeaderID    int
}

// GetWrongLeader returns the WrongLeader field of GetReply
func (reply *GetReply) GetWrongLeader() bool {
	return reply.WrongLeader
}

// GetErr returns the Err field of GetReply
func (reply *GetReply) GetErr() Err {
	return reply.Err
}

// GetLeaderID returns the LeaderID field of GetReply
func (reply *GetReply) GetLeaderID() int {
	return reply.LeaderID
}

// SetLeaderID sets the LeaderID field of GetReply
func (reply *GetReply) SetLeaderID(leaderID int) {
	reply.LeaderID = leaderID
}
