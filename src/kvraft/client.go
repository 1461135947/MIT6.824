package kvraft

import (
	"math/rand"
	"src/labrpc"
	"sync"
	"sync/atomic"
	"time"
)


var clerkID int32=0

type Clerk struct {
	mu sync.Mutex
	me int
	servers []*labrpc.ClientEnd
	leaderId int
	OpID uint64
	// You will have to modify this struct.
}

//func nrand() int64 {
//	max := big.NewInt(int64(1) << 62)
//	bigx, _ := rand.Int(rand.Reader, max)
//	x := bigx.Int64()
//	return x
//}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)

	ck.servers = servers
	ck.OpID=1
	ck.leaderId=rand.Int()%len(servers)
	atomic.AddInt32(&clerkID,1)
	ck.me= int(atomic.LoadInt32(&clerkID))
	DPrintf("New Clerk %d",ck.me)
	// You'll have to add code here.
	return ck
}
func (ck *Clerk)changeLeader()  {
	ck.leaderId=(ck.leaderId+1)%len(ck.servers)
}
func (ck *Clerk)sendGetRpc(index int,arg *GetArgs,reply *GetReply )bool  {
	ok:=ck.servers[index].Call("KVServer.Get",arg,reply)
	return ok
}
func (ck *Clerk) sendPutAppendRpc(index int,arg *PutAppendArgs,reply *PutAppendReply)bool  {
	ok:=ck.servers[index].Call("KVServer.PutAppend",arg,reply)
	return ok
}


//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	ck.mu.Lock()
	defer ck.mu.Unlock()

	arg:=GetArgs{
		Key: key,
		OpId: ck.OpID,
		ClientID: ck.me,
	}
	ck.OpID++

	DPrintf("client Get %s OpID:%d",key,arg.OpId)

	for {
		reply:=GetReply{}
		if ck.sendGetRpc(ck.leaderId,&arg,&reply){
			if reply.Err ==OK||reply.Err==ErrExecuted{
				return reply.Value
			}
			ck.changeLeader()
		}else{
			ck.changeLeader()
		}
		time.Sleep(time.Millisecond)

	}

}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	ck.mu.Lock()
	defer ck.mu.Unlock()

	arg:=PutAppendArgs{
		Key: key,
		Value: value,
		Op: op,
		OpId: ck.OpID,
		ClientID: ck.me,
	}
	ck.OpID++
	DPrintf("client putAppend opID:%d op:%s key:%s value:%s",arg.OpId,op,key,value)

	for {
		reply:=PutAppendReply{}
		if ck.sendPutAppendRpc(ck.leaderId,&arg,&reply){
			if reply.Err ==OK||reply.Err==ErrExecuted{
				return
			}
			ck.changeLeader()
		}else{
			ck.changeLeader()
		}
		time.Sleep(time.Millisecond)
	}

}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

func init()  {
	rand.Seed(time.Now().UnixNano())
}
