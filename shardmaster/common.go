package shardmaster

//
// Master shard server: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// The number of shards.
const NShards = 10

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

// Copy returns a new copy of a given Config
func (config Config) Copy() Config {
	newConfig := Config{
		Num:    config.Num,
		Shards: config.Shards,
		Groups: copyConfigGroups(config.Groups),
	}
	return newConfig
}

func copyConfigGroups(groups map[int][]string) map[int][]string {
	newGroups := make(map[int][]string)
	for gid, servers := range groups {
		serversCopy := make([]string, len(servers))
		copy(serversCopy, servers)
		newGroups[gid] = serversCopy
	}
	return newGroups
}

// ClientInfo defines the client information needed to be included in arguments
type ClientInfo struct {
	ClientID int64
	SerialID uint
}

type ClerkRPCArgs interface {
	GetClerkInfo() ClientInfo
}

const (
	OK   = "OK"
	FAIL = "FAIL"
)

type Err string

// JoinArgs defines the arguments used for Jion RPC
type JoinArgs struct {
	Servers   map[int][]string // new GID -> servers mappings
	ClerkInfo ClientInfo
}

func (args JoinArgs) GetClerkInfo() ClientInfo {
	return args.ClerkInfo
}

func (args *JoinArgs) copy() JoinArgs {
	return JoinArgs{
		Servers:   copyConfigGroups(args.Servers),
		ClerkInfo: args.ClerkInfo,
	}
}

type JoinReply struct {
	WrongLeader bool
	Err         Err
}

type LeaveArgs struct {
	GIDs      []int
	ClerkInfo ClientInfo
}

func (args LeaveArgs) GetClerkInfo() ClientInfo {
	return args.ClerkInfo
}

func (args *LeaveArgs) copy() LeaveArgs {
	return LeaveArgs{
		GIDs:      append([]int{}, args.GIDs...),
		ClerkInfo: args.ClerkInfo,
	}
}

type LeaveReply struct {
	WrongLeader bool
	Err         Err
}

type MoveArgs struct {
	Shard     int
	GID       int
	ClerkInfo ClientInfo
}

func (args MoveArgs) GetClerkInfo() ClientInfo {
	return args.ClerkInfo
}

func (args *MoveArgs) copy() MoveArgs {
	return MoveArgs{
		Shard:     args.Shard,
		GID:       args.GID,
		ClerkInfo: args.ClerkInfo,
	}
}

type MoveReply struct {
	WrongLeader bool
	Err         Err
}

type QueryArgs struct {
	Num       int // desired config number
	ClerkInfo ClientInfo
}

func (args QueryArgs) GetClerkInfo() ClientInfo {
	return args.ClerkInfo
}

func (args *QueryArgs) copy() QueryArgs {
	return QueryArgs{
		Num:       args.Num,
		ClerkInfo: args.ClerkInfo,
	}
}

type QueryReply struct {
	WrongLeader bool
	Err         Err
	Config      Config
}
