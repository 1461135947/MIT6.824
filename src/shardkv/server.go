package shardkv


// import "../shardmaster"
import (
	"bytes"
	"src/labrpc"
	"src/shardmaster"
	"sync/atomic"
	"time"
)
import "src/raft"
import "sync"
import "src/labgob"

type OpType int

const  (
	ConfigurationTimeout=50*time.Millisecond
	MigrationShardsTimeout=50 *time.Millisecond
	DeleteShardsTimeout=50 *time.Millisecond
)

const (
	Get OpType=iota
	Append
	Put
)


type ShardState int
type CommandType int
const (
	Serving ShardState=iota
	Pulling
	BePulling
	GC
	)
const  (
	ClientOp CommandType=iota
	Configuration
	AddShards
	DeleteShard
)

//	apply层需要执行的指令包括以下几种:客户端请求执行指令、更新配置、添加shard、删除shard


//客户端请求执行的指令如:Put,Append,Get
type Op struct {
	ClientID uint64
	OpId     uint64
	Key      string
	Value    string
	Type     OpType
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}
type Shard struct {
	Data map[string]string
	DeduplicationTable map[uint64]uint64
	State ShardState
}


type ExecuteResult struct {
	WrongLeader bool
	Term int
	Err         Err
	Value string

}


type ShardKV struct {
	dead         int32
	mu           sync.Mutex
	me           int
	sc 			*shardmaster.Clerk
	rf                *raft.Raft
	applyCh           chan raft.ApplyMsg
	make_end          func(string) *labrpc.ClientEnd
	gid               int
	masters           []*labrpc.ClientEnd
	maxraftstate      int // snapshot if log grows this big
	responseQueue     map[int] chan ExecuteResult
	data              map[int]*Shard
	raftCommunication chan int
	currentConfig     shardmaster.Config
	lastConfig 		shardmaster.Config
	shard             Shard
	// Your definitions here.
}

func (sd *Shard)deepCopy()*Shard  {
	res:= &Shard{
		Data: make(map[string]string),
		DeduplicationTable: make(map[uint64]uint64),
	}
	for k,v:=range sd.Data{
		res.Data[k]=v
	}
	for k,v:=range sd.DeduplicationTable{
		res.DeduplicationTable[k]=v
	}
	return res
}
//执行指令并且添加到日志中
func (kv *ShardKV) Execute(command Command)ExecuteResult  {
	reply :=ExecuteResult{}

	kv.mu.Lock()
	if index,term,isLeader:= kv.rf.Start(command);isLeader{
		if command.Type==ClientOp{
			ch:=make(chan ExecuteResult)
			kv.responseQueue[index]=ch
			kv.mu.Unlock()
			select {
			case msg:=<-ch:
				if msg.Term!=term{
					reply.Err=ErrNoAccept
					break
				}
				reply=msg
			}
		}else {
			kv.mu.Unlock()
		}
	}else{
		reply.WrongLeader=true
		kv.mu.Unlock()
	}
	return  reply
}
//检测当前操作是否可以服务
func (kv *ShardKV)canServer(op *Op)  bool{

	shardNum:=key2shard(op.Key)
	if _,ok:=kv.data[shardNum];ok{
		DPrintf("gid:%d 存在shard shard的状态为:%v",kv.gid,kv.data[shardNum].State)
		if kv.data[shardNum].State==Serving||kv.data[shardNum].State==GC{
			return true
		}else {
			return false
		}
	}
	DPrintf("gid:%v configuration:%v",kv.gid,kv.currentConfig)
	DPrintf("不存在shard")
	return false

}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op:=Op{
		ClientID: args.ClientID,
		OpId: args.OpID,
	}
	op.Type=Get
	op.Key=args.Key
	DPrintf("进行Get请求前")
	if _,isLeader:=kv.rf.GetState();!isLeader{
		reply.Err=ErrWrongLeader
		return
	}
	kv.mu.Lock()
	canServer:=kv.canServer(&op)
	kv.mu.Unlock()
	DPrintf("进行Get请求后")
	DPrintf("server:%v canServer:%v shardNum:%d  gid:%d clientId:%v OpID:%v",kv.me,canServer,key2shard(op.Key),kv.gid,args.ClientID,args.OpID)
	DPrintf("configuration:%v",kv.currentConfig)
	if canServer{
		res:=kv.Execute(NewOpCommand(&op))
		reply.Err=res.Err
		reply.Value=res.Value
	}else {
		reply.Err=ErrWrongGroup
	}

}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op:=Op{
		ClientID: args.ClientID,
		OpId: args.OpID,
	}
	if args.Op=="Put"{
		op.Type=Put
	}else {
		op.Type=Append
	}
	if _,isLeader:=kv.rf.GetState();!isLeader{
		reply.Err=ErrWrongLeader
		return
	}
	op.Key=args.Key
	op.Value=args.Value
	DPrintf("进行PutAppend请求前")
	kv.mu.Lock()
	canServer:=kv.canServer(&op)
	kv.mu.Unlock()
	DPrintf("进行PutAppend请求后")
	if canServer{
		res:=kv.Execute(NewOpCommand(&op))
		reply.Err=res.Err
	}else {
		reply.Err=ErrWrongGroup
	}


}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead,1)
	kv.rf.Kill()
	// Your code here, if desired.
}
func (kv *ShardKV)killed() bool {
	return atomic.LoadInt32(&kv.dead)==1
}

//序列化kv层快照的数据
func (kv *ShardKV)serializeData()[]byte  {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	for k,v:=range kv.data{
		DPrintf("serializeData k:%v v:%v",k,v)
	}
	e.Encode(kv.lastConfig)
	e.Encode(kv.currentConfig)
	e.Encode(kv.data)

	data := w.Bytes()
	return data
}

func (kv *ShardKV) deserializeData(data []byte)  {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	d.Decode(&kv.lastConfig)
	d.Decode(&kv.currentConfig)
	d.Decode(&kv.data)

	for k,v:=range kv.data{
		DPrintf("deserializeData k:%v v:%v",k,v)
	}

}
//根据配置信息更新shard
func (kv *ShardKV)updateShardStatus(cfg *shardmaster.Config)  {
	//将在新版本中不属于当前replica的shard状态置为BePulling
	for k,v:=range kv.data{
		if cfg.Shards[k]!=kv.gid{
			v.State=BePulling
		}
	}
	//添加属于新版本的shard
	for i,v:=range cfg.Shards{
		if v==kv.gid{
			if _,ok:=kv.data[i];!ok{
				kv.data[i]=&Shard{
					Data: make(map[string]string),
					DeduplicationTable: make(map[uint64]uint64),
					State: Pulling,
				}

			}
			//如果上一轮配合shard的所有者是0号replica则直接将状态置为Serving
			if kv.currentConfig.Shards[i]==0{
				kv.data[i].State=Serving
			}
		}
	}
}
func (kv *ShardKV)applyConfigure(cfg  *shardmaster.Config)  {
	if cfg.Num==kv.currentConfig.Num+1{
		DPrintf("Server %v Group %v update currentConfig from %v to %v",kv.me,kv.gid,kv.currentConfig,cfg)
		kv.updateShardStatus(cfg)
		kv.lastConfig=kv.currentConfig
		kv.currentConfig=*cfg
	}
}
//kv层执行Client发送的指令
func (kv *ShardKV) applyClientOp(op Op,res *ExecuteResult)  {
	res.Err=OK
	DPrintf("server:%v gid:%v start execute op:%v",kv.me,kv.gid,op)
	shardNum:=key2shard(op.Key)
	//先检查是否可以执行
	if !kv.canServer(&op){
		res.Err=ErrWrongGroup
		return
	}

	//过滤执行重复的Put和Append指令
	if  op.Type!=Get&&kv.data[shardNum].DeduplicationTable[op.ClientID]>=op.OpId{
		return
	}
	DPrintf("server:%d group:%v shard:%v execute op:%v",kv.me,kv.gid,shardNum,op)
	switch op.Type {
	//执行Get指令
	case Get:
		if v,ok:=kv.data[shardNum].Data[op.Key] ;ok{
			res.Value=v
		}else{
			res.Err=ErrNoKey
		}
	//	执行Put指令
	case Put:
		kv.data[shardNum].Data[op.Key]=op.Value
	//	执行Append指令
	case Append:
		if _,ok:=kv.data[shardNum].Data[op.Key];ok{
			kv.data[shardNum].Data[op.Key]+=op.Value
		}else{
			kv.data[shardNum].Data[op.Key]=op.Value
		}
	}
	kv.data[shardNum].DeduplicationTable[op.ClientID]=op.OpId
}
//kv层接收Raft层需要apply的指令
func (kv *ShardKV)applier()  {
	for !kv.killed(){
		//应用快照的日志
		select {
		case msg:=<-kv.applyCh:
			if msg.SnapshotValid{
				//kv层开始应用快照
				kv.deserializeData(msg.Command.([]byte))
				kv.rf.CondInstallSnapshot(msg.CommandTerm,msg.CommandIndex,msg.Command.([]byte))
				break

			}
			res:=ExecuteResult{
				Term: msg.CommandTerm,
			}
			kv.mu.Lock()

			//1.执行指令
			command:=msg.Command.(Command)
			switch command.Type{
			case Configuration:
				cfg:=command.Data.(shardmaster.Config)
				kv.applyConfigure(&cfg)
			case ClientOp:
				op:=command.Data.(Op)
				kv.applyClientOp(op,&res)
			case AddShards:
				arg:=command.Data.(AcquireShardsReply)
				kv.applyAddShards(&arg)
			case DeleteShard:
				arg:=command.Data.(DeleteShardsArg)
				kv.applyDeleteShard(&arg)

			}
			//2.向所有注册结果的接受者发送执行结果
			if ch,ok:=kv.responseQueue[msg.CommandIndex];ok{
				ch<-res
				delete(kv.responseQueue,msg.CommandIndex)
			}
			kv.mu.Unlock()

		}

	}
}
//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(shardmaster.Config{})
	labgob.Register(Command{})
	labgob.Register(AcquireShardsArg{})
	labgob.Register(DeleteShardsArg{})
	labgob.Register(AcquireShardsReply{})
	labgob.Register(DeleteShardsReply{})
	labgob.Register(Shard{})
	labgob.Register(ExecuteResult{})


	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters

	kv.sc=shardmaster.MakeClerk(masters)
	kv.responseQueue=make(map[int] chan ExecuteResult)
	kv.raftCommunication=make(chan int)
	kv.data=make(map[int]*Shard)
	kv.lastConfig=shardmaster.Config{
		Num: 0,
	}
	kv.currentConfig =shardmaster.Config{
		Num: 0,
	}

	// Your initialization code here.

	// Use something like this to talk to the shardmaster:
	// kv.mck = shardmaster.MakeClerk(kv.masters)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.rf.SetMaxRaftState(kv.maxraftstate)
	kv.rf.RegisterKVCommunication(kv.raftCommunication)
	kv.deserializeData(kv.rf.GetSnapshotDat())
	go kv.RaftCommunication()
	go kv.applier()
	go kv.LeaderTask(kv.PullAction,ConfigurationTimeout)
	go kv.LeaderTask(kv.MigrationShardAction,MigrationShardsTimeout)
	go kv.LeaderTask(kv.GCAction,DeleteShardsTimeout)

	return kv
}
//根据shard的状态找出对应的pid与shard的映射集合
func (kv *ShardKV) GetShardIDsByStatus(status ShardState) map[int][]int {
	res:=make(map[int][]int)
	for k,v:=range kv.data{
		if v.State==status{
			gid:=kv.lastConfig.Shards[k]
			if _,ok:=res[gid];!ok{
				res[gid]=make([]int,0)
			}
			res[gid]=append(res[gid],k)
		}
	}
	return res
}

func (kv *ShardKV)LeaderTask( action func(),timeOut time.Duration)  {
	for !kv.killed(){
		if _,isLeader:=kv.rf.GetState();isLeader{
			action()
		}
		time.Sleep(timeOut)
	}
}

//apply协程应用插入shard的日志
func (kv *ShardKV)applyAddShards(arg *AcquireShardsReply)  {
	//只有当插入日志的配置与当前日志的配置相同才能进行覆盖
	if arg.CfgNum==kv.currentConfig.Num{
		DPrintf("server:%v group:%v add shards:%v when configure number is %v",kv.me,kv.gid,arg,arg.CfgNum)
		for shardNum,shardData:=range arg.Shards{
			shard:=kv.data[shardNum]
			DPrintf("shard:%v State:%v",shardNum,shard.State)
			//只有在Shard的状态为Pulling时才能覆盖;否则可能会造成请求数据的丢失(当shard为Serving和GC状态时就可以接受请求)
			if shard.State==Pulling{
				//将数据和去重表复制到shard中
				for k,v:=range shardData.Data{
					shard.Data[k]=v
				}
				for k,v:=range shardData.DeduplicationTable{
					shard.DeduplicationTable[k]=v
				}
				shard.State=GC
			}else {
				DPrintf("duplicated add Shards configure is %v",kv.currentConfig)
				break
			}
		}
	}else{
		DPrintf("server:%v group:%v reject add shard; add configuration num is %v current is %v",kv.me,kv.gid,arg.CfgNum,kv.currentConfig.Num)
	}
}
func (kv *ShardKV)applyDeleteShard(arg *DeleteShardsArg) {
	//只有当删除时的配置与当前配置一致时才进行删除
	if arg.CfgNum==kv.currentConfig.Num{
		DPrintf("server:%v group:%v delete shards:%v when configure number is %v",kv.me,kv.gid,arg,arg.CfgNum)
		for _,shardID:=range arg.Shards{
			shard,ok:=kv.data[shardID]
			if ok{
				//状态为GC时直接转换为Serving状态
				if shard.State==GC{
					shard.State=Serving
				}else if shard.State==BePulling {
					kv.data[shardID]=nil
					delete(kv.data,shardID)
					DPrintf("{Node %v}{Group %v} encounters duplicated shards deletion %v when currentConfig is %v", kv.me, kv.gid, arg, kv.currentConfig)
				}else{
					break
				}
			}

		}
	}else{
		DPrintf("server:%v group:%v reject add shard; add configuration num is %v current is %v",kv.me,kv.gid,arg.CfgNum,kv.currentConfig.Num)
	}

}
//通知上一轮Shard拥有者清除Shard
func (kv *ShardKV)GCAction()  {
	wait:=sync.WaitGroup{}
	kv.mu.Lock()
	//寻找要删除的日志
	shards:=kv.GetShardIDsByStatus(GC)
	for gid,shardIDs:=range shards{
		DPrintf("Server:%v Group:%v GC From Group:%v when config is %v",kv.me,kv.gid, gid,kv.currentConfig)
		wait.Add(1)
		go func(servers []string,cfgNum int,shardIDs []int) {
			defer wait.Done()
			arg:=DeleteShardsArg{
				Shards: shardIDs,
				CfgNum: cfgNum,
			}
			//通知上一个配置的拥有者删除shard
			for _,server:=range servers{
				reply := DeleteShardsReply{}
				ser:=kv.make_end(server)
				if ser.Call("ShardKV.DeleteShards",&arg,&reply)&&reply.Err==OK{
					DPrintf("Server:%v Group:%v delete shards from group:%v  reply:%v and try commit DeleteShards ConfigureNum:%v",kv.me,kv.gid,gid,reply,arg.CfgNum)
					//添加一条插入shard的日志
					kv.Execute(NewDeleteShardsCommand(&arg))
				}
			}
		}(kv.lastConfig.Groups[gid],kv.currentConfig.Num, shardIDs)
	}
	kv.mu.Unlock()
	wait.Wait()
}
func (kv *ShardKV)DeleteShards(arg *DeleteShardsArg,reply *DeleteShardsReply)  {
	//	只有Leader才能响应请求
	if _,isLeader:=kv.rf.GetState();!isLeader{
		reply.Err=ErrWrongLeader
		return
	}
	defer DPrintf("{Node %v}{Group %v} processes GCTaskRequest %v with response %v", kv.me, kv.gid, arg, reply)

	kv.mu.Lock()
	//如果当前的配置大于请求的配置返回ok;以便让请求的replica继续执行下去
	if kv.currentConfig.Num>arg.CfgNum{
		reply.Err=OK
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
	copyArg:=DeleteShardsArg{
		CfgNum: arg.CfgNum,
		Shards: make([]int,len(arg.Shards)),
	}
	copy(copyArg.Shards,arg.Shards)

	kv.Execute(NewDeleteShardsCommand(&copyArg))
}
//根据shard的状态进行迁移;模式为Pull;所以只需要拉取状态为Pulling的Shard即可
func (kv *ShardKV) MigrationShardAction()  {
	wait:=sync.WaitGroup{}
	kv.mu.Lock()
	shards:=kv.GetShardIDsByStatus(Pulling)
	for gid, shardIDs :=range shards{
		DPrintf("Server:%v Group:%v Pulling From Group:%v when config is %v",kv.me,kv.gid, gid,kv.currentConfig)
		wait.Add(1)
		go func(servers []string,cfgNum int,shardIDs []int) {
			defer wait.Done()
			arg:= AcquireShardsArg{
				Shards: shardIDs,
				CfgNum: cfgNum,
			}
			//
			for _,server:=range servers{
				reply := AcquireShardsReply{}
				ser:=kv.make_end(server)
				if ser.Call("ShardKV.AcquireShards",&arg,&reply)&&reply.Err==OK{
					DPrintf("Server:%v Group:%v gets AcquireShard from group:%v  reply:%v and try commit InsertShard ConfigureNum:%v",kv.me,kv.gid,gid,reply,arg.CfgNum)
				//添加一条插入shard的日志
				kv.Execute(NewInsertShardsCommand(&reply))
				}
			}
		}(kv.lastConfig.Groups[gid],kv.currentConfig.Num, shardIDs)

	}
	kv.mu.Unlock()
	//所有获取shard的请求完成以后进行下一轮
	wait.Wait()
}

//根据对端的请求将shard发送给对方
func (kv *ShardKV)AcquireShards(arg *AcquireShardsArg,reply *AcquireShardsReply)  {
//	只有Leader才能响应请求
	if _,isLeader:=kv.rf.GetState();!isLeader{
		reply.Err=ErrWrongLeader
		return
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	defer DPrintf("Server:%v Group:%v response AcquireShards shards arg:%v response:%v",kv.me,kv.gid,arg,reply)
	//当前版本小于请求的版本号表示当前replica没有更新到指定版本需要请求方等待
	if kv.currentConfig.Num<arg.CfgNum{
		reply.Err=ErrNotReady
		return
	}
	//拷贝复制shard
	shards:=make(map[int]Shard)
	for _,shardNum:=range arg.Shards{
		if _,ok:=kv.data[shardNum];ok{
			shards[shardNum]=*kv.data[shardNum].deepCopy()
		}

	}
	reply.Err=OK
	reply.CfgNum=arg.CfgNum
	reply.Shards=shards
}

//定期从服务器端拉取最新配置
func (kv *ShardKV) PullAction()  {
	canPerformNextConfig :=true
	//检查所有的shard是否处于默认状态;只有所有的shard处于默认状态才能更新配置
	kv.mu.Lock()
	for k,shard:=range kv.data{
		if shard.State!=Serving{
			canPerformNextConfig =false
			DPrintf("pull fail Shard:%v State:%v",k,shard.State)
			break
		}
	}
	currentConfigNum:=kv.currentConfig.Num+1
	kv.mu.Unlock()
	if canPerformNextConfig {
		queryConfig:=kv.sc.Query(currentConfigNum)
		//获得新配置开始更新配置
		if queryConfig.Num==currentConfigNum{

			kv.Execute(NewConfigurationCommand(&queryConfig))
		}
	}
}

//kv层接收Raft发送的信息;包括Raft失去Leadership和某个位置进行快照
func ( kv*ShardKV)RaftCommunication() {
	for !kv.killed() {
		select {
		case msg := <-kv.raftCommunication:
			//Raft在msg处进行快照
			if msg>0{
				DPrintf("开始进行快照")
				data:=kv.serializeData()
				kv.rf.StartSnapshot(msg,data)
			//	Raft失去Leadership
			}else if msg == raft.LossLeadership {
				DPrintf("raft loss leadership %d", kv.me)
				//向所有等待响应的Response发送结果
				res:=ExecuteResult{
					WrongLeader: false,
					Term: 0,
					Err: ErrNoAccept,
				}
				kv.mu.Lock()
				for k,v:=range kv.responseQueue {
					v<-res
					delete(kv.responseQueue,k)
				}
				kv.mu.Unlock()
			}
		}
	}
}