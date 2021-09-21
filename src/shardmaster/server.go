package shardmaster


import (

	"sort"
	"src/raft"
	"sync/atomic"
)
import "src/labrpc"
import "sync"
import "src/labgob"
type CommandType int

const  (
	Join CommandType=iota
	Leave
	Move
	Query
)


type Op struct {
	ClientID uint64
	OpId     uint64
	Servers  map[int][]string //join 使用
	GIDs     []int //leave使用
	Shard    int //Move使用
	GID      int //Move使用
	Num      int   //query使用
	Type     CommandType
}

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	raftCommunication chan int
	dead    int32 // set by Kill()
	// Your data here.
	configs []Config // indexed by config num
	responseQueue map[int] chan ExecuteResult
	commandFilter map[uint64]uint64
}
type ExecuteResult struct {
	WrongLeader bool
	Term int
	Err         Err
	Config      Config
}


//type Op struct {
//	// Your data here.
//}

func (sm *ShardMaster)ExecuteRequest(command *Op)ExecuteResult  {
	reply :=ExecuteResult{}
	sm.mu.Lock()
	if index,term,isLeader:=sm.rf.Start(*command);isLeader{
		ch:=make(chan ExecuteResult)
		sm.responseQueue[index]=ch
		sm.mu.Unlock()
		select {
		case msg:=<-ch:
			if msg.Term!=term{
				reply.Err=ErrNoAccepted
				break
			}
			reply=msg
		}

	}else{
		reply.WrongLeader=true
		sm.mu.Unlock()
	}
	return  reply
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	command:= Op{
		ClientID: args.ClientId,
		OpId:     args.OpID,
		Type:     Join,
	}
	command.Servers=args.Servers
	res:=sm.ExecuteRequest(&command)
	reply.Err=res.Err
	reply.WrongLeader=res.WrongLeader

}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	command:= Op{
		ClientID: args.ClientId,
		OpId:     args.OpID,
		Type:     Leave,
	}
	command.GIDs=args.GIDs
	res:=sm.ExecuteRequest(&command)
	reply.Err=res.Err
	reply.WrongLeader=res.WrongLeader
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	command:= Op{
		ClientID: args.ClientId,
		OpId:     args.OpID,
		Type:     Move,
	}
	command.Shard=args.Shard
	command.GID=args.GID
	res:=sm.ExecuteRequest(&command)
	reply.Err=res.Err
	reply.WrongLeader=res.WrongLeader
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	command:= Op{
		ClientID: args.ClientId,
		OpId:     args.OpID,
		Type:     Query,
	}
	command.Num=args.Num
	res:=sm.ExecuteRequest(&command)
	reply.Err=res.Err
	reply.WrongLeader=res.WrongLeader
	reply.Config=res.Config
}


//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	atomic.StoreInt32(&sm.dead,1)
	sm.rf.Kill()
	// Your code here, if desired.
}

func (sm *ShardMaster) killed() bool {
	z := atomic.LoadInt32(&sm.dead)
	return z == 1
}
// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

func (sm *ShardMaster)RaftCommunication() {
	for !sm.killed() {
		select {
		case msg := <-sm.raftCommunication:
			if msg == raft.LossLeadership {
				DPrintf("raft loss leadership %d", sm.me)
			//向所有等待响应的Response发送结果
			res:=ExecuteResult{
				WrongLeader: false,
				Term: 0,
				Err: ErrNoAccepted,
			}
			sm.mu.Lock()
			for k,v:=range sm.responseQueue {
				v<-res
				delete(sm.responseQueue,k)
			}
			sm.mu.Unlock()
			}
		}
	}
}
func GetGIDWithMinimumShards(s2g map[int][]int) int {
	// make iteration deterministic
	var keys []int
	for k := range s2g {
		keys = append(keys, k)
	}
	sort.Ints(keys)
	// find GID with minimum shards
	index, min := -1, NShards+1
	for _, gid := range keys {
		if gid != 0 && len(s2g[gid]) < min {
			index, min = gid, len(s2g[gid])
		}
	}
	return index
}

func GetGIDWithMaximumShards(s2g map[int][]int) int {
	// always choose gid 0 if there is any
	if shards, ok := s2g[0]; ok && len(shards) > 0 {
		return 0
	}
	// make iteration deterministic
	var keys []int
	for k := range s2g {
		keys = append(keys, k)
	}
	sort.Ints(keys)
	// find GID with maximum shards
	index, max := -1, -1
	for _, gid := range keys {
		if len(s2g[gid]) > max {
			index, max = gid, len(s2g[gid])
		}
	}
	return index
}

func (sm *ShardMaster)getLastConfig()*Config  {
	return &sm.configs[len(sm.configs)-1]
}
func deepCopy(src map[int][]string) map[int][]string{
	res:=make(map[int][]string)
	for k,v:= range src {
		copyV:= make([]string,len(v))
		copy(copyV,v)
		res[k]=copyV
	}
	return res
}
func Group2Shards(config Config) map[int][]int {
	res:=make(map[int][]int,0)
	for k,_:=range config.Groups{
		res[k]=make([]int,0)
	}
	for i,v:=range config.Shards{
		res[v]=append(res[v],i)
	}
	return  res
}
func (sm *ShardMaster)handleJoin(op *Op)  {
	lastConfig := sm.getLastConfig()
	newConfig := Config{len(sm.configs), lastConfig.Shards, deepCopy(lastConfig.Groups)}
	for gid, servers := range op.Servers {
		if _, ok := newConfig.Groups[gid]; !ok {
			newServers := make([]string, len(servers))
			copy(newServers, servers)
			newConfig.Groups[gid] = newServers
		}
	}
	s2g := Group2Shards(newConfig)
	for {
		source, target := GetGIDWithMaximumShards(s2g), GetGIDWithMinimumShards(s2g)
		if source != 0 && len(s2g[source])-len(s2g[target]) <= 1 {
			break
		}
		s2g[target] = append(s2g[target], s2g[source][0])
		s2g[source] = s2g[source][1:]
	}
	var newShards [NShards]int
	for gid, shards := range s2g {
		for _, shard := range shards {
			newShards[shard] = gid
		}
	}
	newConfig.Shards = newShards
	DPrintf("执行Join后的结果:%v",newConfig)
	sm.configs= append(sm.configs, newConfig)

}
func (sm *ShardMaster)handleLeave(op *Op)  {
	lastConfig := sm.configs[len(sm.configs)-1]
	newConfig := Config{len(sm.configs), lastConfig.Shards, deepCopy(lastConfig.Groups)}
	s2g := Group2Shards(newConfig)
	orphanShards := make([]int, 0)
	for _, gid := range op.GIDs {
		if _, ok := newConfig.Groups[gid]; ok {
			delete(newConfig.Groups, gid)
		}
		if shards, ok := s2g[gid]; ok {
			orphanShards = append(orphanShards, shards...)
			delete(s2g, gid)
		}
	}
	var newShards [NShards]int
	// load balancing is performed only when raft groups exist
	if len(newConfig.Groups) != 0 {
		for _, shard := range orphanShards {
			target := GetGIDWithMinimumShards(s2g)
			s2g[target] = append(s2g[target], shard)
		}
		for gid, shards := range s2g {
			for _, shard := range shards {
				newShards[shard] = gid
			}
		}
	}
	newConfig.Shards = newShards
	sm.configs = append(sm.configs, newConfig)
}
func (sm *ShardMaster)handleQuery(op *Op,res *ExecuteResult)  {
	var src Config
	if op.Num<0||op.Num>=len(sm.configs){
		src=*sm.getLastConfig()
	}else{
		src=sm.configs[op.Num]
	}
	res.Config=Config{
		Num: src.Num,
		Shards: src.Shards,
		Groups: deepCopy(src.Groups),
	}
}
func (sm *ShardMaster)handleMove(op *Op)  {
	lastConfig := sm.configs[len(sm.configs)-1]
	newConfig := Config{len(sm.configs), lastConfig.Shards, deepCopy(lastConfig.Groups)}
	newConfig.Shards[op.Shard]=op.GID
	sm.configs = append(sm.configs, newConfig)
}

func (sm *ShardMaster)Applier()  {
	for !sm.killed(){
		select {
		case msg:=<-sm.applyCh:
			sm.mu.Lock()
			op:=msg.Command.(Op)
			res:=ExecuteResult{
				WrongLeader: false,
				Term: msg.CommandTerm,
				Err: OK,
			}
			//1.首先进行重复请求检查
			//不是重复指令
			if op.OpId>sm.commandFilter[op.ClientID]{
				//DPrintf("执行%v",op)
				switch op.Type {
				case Join:
					sm.handleJoin(&op)
				case Leave:
					sm.handleLeave(&op)
				case Move:
					sm.handleMove(&op)
				case Query:
					sm.handleQuery(&op,&res)
				}
				sm.commandFilter[op.ClientID]=op.OpId
			//重复指令;只执行重复的Query指令
			}else{
				if op.Type==Query{
					sm.handleQuery(&op,&res)
				}
			}

			//2.检查结果是否有注册接收
			if ch,ok:=sm.responseQueue[msg.CommandIndex];ok{
				ch<-res
				delete(sm.responseQueue, msg.CommandIndex)
			}
			sm.mu.Unlock()

		}
	}

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
	sm.responseQueue=make(map[int]chan ExecuteResult)
	sm.commandFilter=make(map[uint64]uint64)

	labgob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.raftCommunication=make(chan int)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)
	sm.rf.RegisterKVCommunication(sm.raftCommunication)
	//不进行快照
	sm.rf.SetMaxRaftState(-1)
	//接收Raft层发送的消息
	go sm.RaftCommunication()
	go sm.Applier()

	// Your code here.

	return sm
}
