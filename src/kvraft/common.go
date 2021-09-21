package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrNoAccept ="ErrNoAccept"
	ErrWrongLeader = "ErrWrongLeader"
	ErrExecuted="ErrExecuted"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	ClientID int
	OpId uint64
	Key   string
	Value string

	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err

}

type GetArgs struct {
	ClientID int
	OpId uint64
	Key string

	// You'll have to add definitions here.
}

type GetReply struct {

	Err   Err
	Value string

}
