package shardmaster

//
// Shardmaster clerk.
//

import (
	"log"
	"src/labrpc"
)
import "time"
import "crypto/rand"
import "math/big"
var IDGenerate *Worker

type Clerk struct {
	servers []*labrpc.ClientEnd
	me uint64
	OpID uint64
	ServerID int
	// Your data here.
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}
func (ck *Clerk)changeServer()  {

	ck.ServerID=(ck.ServerID+1)%len(ck.servers)
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.me,_= IDGenerate.NextID()
	ck.OpID = 0
	ck.ServerID=0
	// Your code here.
	return ck
}

func (ck *Clerk) Query(num int) Config {
	ck.OpID++
	args := &QueryArgs{
		OpID: ck.OpID,
		ClientId: ck.me,
	}
	// Your code here.
	DPrintf("%d开始执行Query",ck.me)
	args.Num = num
	for {
		reply:=QueryReply{}
		if ck.servers[ck.ServerID].Call("ShardMaster.Query", args, &reply){
			DPrintf("返回结果%v ServerID:%d",reply,ck.ServerID)
			if reply.WrongLeader||reply.Err!=OK{
				ck.changeServer()
			}else {
				return reply.Config
			}
		}else {
			ck.changeServer()
		}

		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	ck.OpID++
	args := &JoinArgs{
		OpID: ck.OpID,
		ClientId: ck.me,
	}
	// Your code here.
	DPrintf("%d开始执行Join",ck.me)
	args.Servers = servers

	for {
		reply:=JoinReply{}
		if ck.servers[ck.ServerID].Call("ShardMaster.Join", args, &reply){
			if reply.WrongLeader||reply.Err!=OK{
				ck.changeServer()
			}else {
				return
			}
		}else {
			ck.changeServer()
		}

		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	ck.OpID++
	args := &LeaveArgs{
		OpID: ck.OpID,
		ClientId: ck.me,
	}
	// Your code here.
	DPrintf("%d开始执行Leave",ck.me)
	args.GIDs = gids

	for {
		reply:=LeaveReply{}
		if ck.servers[ck.ServerID].Call("ShardMaster.Leave", args, &reply){
			if reply.WrongLeader||reply.Err!=OK{
				ck.changeServer()
			}else {
				return
			}
		}else {
			ck.changeServer()
		}

		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	ck.OpID++
	args := &MoveArgs{
		OpID: ck.OpID,
		ClientId: ck.me,

	}
	// Your code here.
	args.Shard = shard
	args.GID = gid
	DPrintf("%d开始执行Move",ck.me)
	for {
		reply:=MoveReply{}
		if ck.servers[ck.ServerID].Call("ShardMaster.Move", args, &reply){
			if reply.WrongLeader||reply.Err!=OK{
				ck.changeServer()
			}else {
				return
			}
		}else {
			ck.changeServer()
		}

		time.Sleep(100 * time.Millisecond)
	}
}

func init()  {
	IDGenerate=NewWorker(0,0)
	log.SetFlags(log.Lmicroseconds)
}