package kvraft

import (
	"bytes"
	"src/labgob"
	"src/labrpc"
	"log"
	"src/raft"
	"sync"
	"sync/atomic"

)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type OpResult struct {
	Err  Err
	Term int

	Value string
}
type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId int
	OpId uint64
	OpType string
	Key string
	Value string

}
type OpMap struct {
	data map[int]uint64
	sync.Mutex
}

type ResultChanMap struct {
	data map[int]chan OpResult
	sync.Mutex
}

type DataMap struct {
	data map[string] string
	sync.Mutex
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	raftCommunication chan int
	dead    int32 // set by Kill()

	maxraftstate  int // snapshot if log grows this big
	resultChanMap ResultChanMap
	dataMap DataMap
	OpMap OpMap
	lastApplied int

	// Your definitions here.
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	if kv.killed(){
		return
	}

	op:=Op{
		OpType: "Get",
		OpId: args.OpId,
		Key: args.Key,
		ClientId: args.ClientID,
	}
	//将操作传到底层Raft层
	kv.resultChanMap.Lock()
	// 确定指令执行顺序
	if index,term,isLeader:=kv.rf.Start(op);isLeader{
		resultChan:=make(chan OpResult)
		defer close(resultChan)
		kv.resultChanMap.data[index]=resultChan
		kv.resultChanMap.Unlock()
		select {
		case msg:=<-resultChan:
			if msg.Term!=term{
				reply.Err=ErrNoAccept
				return
			}
			reply.Err=msg.Err
			reply.Value=msg.Value
		}

	}else {
		reply.Err=ErrWrongLeader
		kv.resultChanMap.Unlock()

	}

}
//序列化kv层快照的数据
func (kv *KVServer)serializeData()[]byte  {
	kv.OpMap.Lock()
	kv.dataMap.Lock()
	defer kv.OpMap.Unlock()
	defer kv.dataMap.Unlock()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.OpMap.data)
	e.Encode(kv.dataMap.data)
	data := w.Bytes()
	return data
}

func (kv *KVServer) deserializeData(data []byte)  {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	kv.OpMap.Lock()
	kv.dataMap.Lock()
	defer kv.OpMap.Unlock()
	defer kv.dataMap.Unlock()
	d.Decode(&kv.OpMap.data)
	d.Decode(&kv.dataMap.data)

}
func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if kv.killed(){
		return
	}

	op:=Op{
		OpType: args.Op,
		Key: args.Key,
		Value:args.Value,
		OpId: args.OpId,
		ClientId: args.ClientID,
	}
	kv.resultChanMap.Lock()
	//DPrintf("server:%d PutAppend Op:%s OpID:%d",kv.me,args.Op,args.OpId)
	// 确定指令执行顺序
	if index,term,isLeader:=kv.rf.Start(op);isLeader{
		DPrintf("server:%d client:%d PutAppend Op:%s OpID:%d",kv.me,args.ClientID,args.Op,args.OpId)
		resultChan:=make(chan OpResult)
		defer close(resultChan)
		//DPrintf("提交server:%d PutAppend Op:%s OpID:%d",kv.me,args.Op,args.OpId)
		kv.resultChanMap.data[index]=resultChan
		kv.resultChanMap.Unlock()
		select {
		case msg:=<-resultChan:
			if msg.Term!=term{
				reply.Err=ErrNoAccept
				return
			}
			reply.Err=msg.Err
		}
	}else {
		reply.Err=ErrWrongLeader
		kv.resultChanMap.Unlock()

	}

}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	DPrintf("kvServer %d killed",kv.me)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//监听applyChan返回的结果并且将结果绑定的chan中
func (kv *KVServer)Applier()  {
	go func() {
		for !kv.killed(){
			select {
			case msg:=<-kv.applyCh:
				//应用快照的日志
				if msg.SnapshotValid{
					//kv层开始应用快照
					kv.deserializeData(msg.Command.([]byte))
					kv.lastApplied=msg.CommandIndex
					kv.rf.CondInstallSnapshot(msg.CommandTerm,msg.CommandIndex,msg.Command.([]byte))
					break
				}

				//普通日志
				//先检查指令是否已经被执行过一次
				op:=msg.Command.(Op)
				kv.OpMap.Lock()
				if msg.CommandIndex<=kv.lastApplied{
					kv.mu.Unlock()
					continue
				}
				kv.lastApplied=msg.CommandIndex

				var opResult OpResult
				opResult.Term=msg.CommandTerm
				//1.是新的指令则执行对应的指令
				if op.OpId>kv.OpMap.data[op.ClientId]{
					kv.dataMap.Lock()
					switch op.OpType{
					case "Get":
						if res,ok:=kv.dataMap.data[op.Key];ok{
							opResult.Err =OK
							opResult.Value=res
						}else {
							opResult.Err =ErrNoKey
						}

					case "Put":
						kv.dataMap.data[op.Key]=op.Value
						opResult.Err =OK
					case "Append":
						//在数据Map中有key对应的value则直接Append;否则将Append转换为Put操作
						if _,ok:=kv.dataMap.data[op.Key];ok{
							kv.dataMap.data[op.Key]+=op.Value
						}else {
							kv.dataMap.data[op.Key]=op.Value
						}
						opResult.Err =OK

					}
					kv.OpMap.data[op.ClientId]=op.OpId
					//DPrintf("执行成功 op:%s opID:%d key:%s value:%s",op.OpType,op.OpId,op.Key,op.Value)
					kv.dataMap.Unlock()
					//将执行后的结果插入的OpMap中
				//重复的指令则返回已经执行错误;执行重复的Get指令
				}else{
					opResult.Err=ErrExecuted
					if op.OpType=="Get"{
						kv.dataMap.Lock()
						opResult.Value=kv.dataMap.data[op.Key]
						kv.dataMap.Unlock()

					}
				}
				kv.OpMap.Unlock()

				//2.检查是否有注册接收结果的channel
				kv.resultChanMap.Lock()
				if ch,ok:=kv.resultChanMap.data[msg.CommandIndex]; ok{
					//将执行结果发送给注册接收的channel
					ch<-opResult
				}
				//解除注册
				delete(kv.resultChanMap.data,msg.CommandIndex)
				kv.resultChanMap.Unlock()

			}
		}
	}()
}

func (kv *KVServer)RaftCommunication()  {
	for  !kv.killed(){
		select {
		case msg:=<-kv.raftCommunication:
			if msg==raft.LossLeadership{
				DPrintf("raft loss leadership %d",kv.me)
				res:=OpResult{
					Err:  ErrNoAccept,
					Term: -1,
				}
				//向所有存在的注册接收消息的channel发送消息;并且删除Map中的映射
				kv.resultChanMap.Lock()
				for k,v:=range kv.resultChanMap.data{
					v<-res
					delete(kv.resultChanMap.data,k)
				}
				kv.resultChanMap.Unlock()
			//	开始进行快照
			}else {
				data:=kv.serializeData()
				kv.rf.StartSnapshot(msg,data)
			}


		}
	}
}
//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.raftCommunication=make (chan int)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.rf.RegisterKVCommunication(kv.raftCommunication)
	kv.rf.SetMaxRaftState(kv.maxraftstate)
	kv.dataMap=DataMap{
		data: make(map[string]string),
	}
	kv.resultChanMap=ResultChanMap{
		data: make(map[int]chan OpResult),
	}
	kv.OpMap=OpMap{
		data: make(map[int]uint64 ),
	}
	kv.deserializeData(kv.rf.GetSnapshotDat())
	kv.lastApplied=kv.rf.GetLastApplied()

	// You may need initialization code here.
	go kv.Applier()
	go kv.RaftCommunication()
	return kv
}
