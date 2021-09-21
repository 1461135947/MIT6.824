package shardkv

import (
	"fmt"
	"src/shardmaster"
)

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	ErrNoAccept    ="ErrNoAccept"
	ErrNotReady="ErrNotReady"
)


type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientID uint64
	OpID  uint64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	ClientID uint64
	OpID  uint64

	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}

type AcquireShardsArg struct {
	CfgNum int
	Shards []int
}
type AcquireShardsReply struct {
	Shards map[int]Shard
	CfgNum int
	Err Err
}

type DeleteShardsArg struct {
	CfgNum int
	Shards []int
}
type DeleteShardsReply struct {

	Err Err
}
func (command Command)String()string  {
	return fmt.Sprintf("{Type:%v Data:%v}",command.Type,command.Data)
}

type Command struct {
	Type CommandType
	Data interface{}
}

func NewOpCommand(op *Op)Command  {
	DPrintf("new op command %v",op)
	return Command{
		Type: ClientOp,
		Data: *op,
	}
}

func NewInsertShardsCommand(arg *AcquireShardsReply) Command {
	return Command{
		Type: AddShards,
		Data: *arg,
	}
}
func NewDeleteShardsCommand(arg *DeleteShardsArg)Command  {
	return Command{
		Type: DeleteShard,
		Data: *arg,
	}
}
func NewConfigurationCommand(cfg *shardmaster.Config)Command  {
	return Command{
		Type: Configuration,
		Data: *cfg,
	}
}
