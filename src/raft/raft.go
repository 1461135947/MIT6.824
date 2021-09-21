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
	"bytes"
	"fmt"
	"sort"
	"sync"
	"time"
)
import "src/labgob"
import "sync/atomic"
import "src/labrpc"


// import "bytes"
// import "../labgob"



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
const (
	Follower=iota
	Candidate
	Leader
)

const (
	LossLeadership=-1
)

const (
	NeedSnapShot=iota
	SnapShooting
	SnapSheet
)
type ApplyMsg struct {
	SnapshotValid bool
	CommandValid bool
	Command      interface{}
	CommandIndex int
	CommandTerm int
}
type Entry struct {
	Term int
	Data interface{}
	Index int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	state     int
	cond 	*sync.Cond
	applyCh chan ApplyMsg

	//这三个变量是需要持久化的
	currentTerm int		//当前的term
	votedFor    int		//投票对象
	logs        []Entry	//日志数组

	commitIndex int		//提交的日志
	lastApplied int		//被应用的日志
	enableApply bool 	//是否可以提交日志

	nextIndex   []int	//leader中的nextIndex数组
	matchIndex  []int	//leader中的matchIndex数组

	heartBeatTimer *time.Timer	//心跳定时器
	electTimer *time.Timer		//选举定时器

	KVCommunication         chan int
	registerKVCommunication bool
	maxRaftState            int
	SnapshotState           int

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state==Leader
}

func (rf *Raft) encodeState() []byte{
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	return  w.Bytes()

}
//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist(data []byte) {
	// Your code here (2C).
	// Example:

	rf.persister.SaveRaftState(data)
	//检测是否需要进行快照
	if rf.maxRaftState!=-1&&rf.persister.RaftStateSize()>rf.maxRaftState&&rf.SnapshotState!=SnapShooting && rf.lastApplied>rf.logs[0].Index{

		//没有日志进行提交则直接开始快照;如果有日志正在提交则标记跳过下一轮的日志应用以便尽早开始进行快照
		if rf.enableApply{
			DPrintf("server:%d 开始进行快照 快照前大小:%d",rf.me,rf.persister.RaftStateSize())

			//通知kv层需要进行快照
			rf.SnapshotState =SnapShooting
			DPrintf("server:%d KV 通信开始 lastApplied",rf.me)
			rf.KVCommunication<-rf.lastApplied
			DPrintf("server:%d KV 通信结束 lastApplied",rf.me)
		}else{
			DPrintf("等待提交完成进行快照")
			rf.SnapshotState =NeedSnapShot
		}
	}

}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.logs)
	rf.lastApplied=rf.logs[0].Index
	rf.commitIndex=rf.logs[0].Index

}
func (rf *Raft)GetSnapshotDat()[]byte  {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.ReadSnapshot()
}




type RequestVoteArgs struct {
	Term int
	CandidateId int
	LastLogIndex int
	LastLogTerm int
	// Your data here (2A, 2B).
}


type RequestVoteReply struct {
	// Your data here (2A).
	Term int
	VoteGranted bool
}

type AppendEntryArgs struct {
	Term int
	LeaderId int
	PreLogIndex int
	PreLogTerm int
	Entries []Entry
	LeaderCommit int
}
type AppendEntryReply struct {
	Term          int
	Success       bool
	ConflictIndex int
	ConflictTerm  int
}

func (rf *Raft)isLogUpToDate(requestLastIndex int,requestLastTerm int) bool {
	lastIndex:=rf.getLastLog().Index
	lastTerm:=rf.getLastLog().Term
	if requestLastTerm>lastTerm||(requestLastTerm==lastTerm&&requestLastIndex>=lastIndex){
		return true
	}else {
		return false
	}

}
func (rf *Raft)getRealIndex(index int) int {
	firstIndex:=rf.getFirstLog().Index
	return index-firstIndex
}
func (rf *Raft)updateTerm(term int )  {
	rf.currentTerm=term
	rf.votedFor=-1
	// 当Leader失去Leadership后通知kv层
	if rf.state==Leader &&rf.registerKVCommunication{
		DPrintf("server:%d KV 通信开始 LossLeaderShip",rf.me)
		rf.KVCommunication<-LossLeadership
		DPrintf("server:%d KV 通信结束 LossLeaderShip",rf.me)
	}
	rf.SetState(Follower)
	rf.persist(rf.encodeState())
}

func (rf *Raft)AppendEntry(args *AppendEntryArgs, reply *AppendEntryReply)  {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//DPrintf("term :%d server:%d state:%d 接收AppendEntry content:%v\n",rf.currentTerm,rf.me,rf.state,args)
 	firstIndex,lastIndex:=rf.getFirstLog().Index,rf.getLastLog().Index
	//1.检测请求Term与currentTerm的关系
	if args.Term<rf.currentTerm{
		reply.Term,reply.Success=rf.currentTerm,false
		return
	}

	if args.Term>rf.currentTerm{
		rf.updateTerm(args.Term)
	}
	rf.SetState(Follower)

	reply.Term=rf.currentTerm

	//2.检查是够存在下标为preLogTerm的日志是否存在
	//回溯冲突日志索引优化
	//case1:不存在下标为PreLogIndex的日志
	if args.PreLogIndex>lastIndex{
		reply.Success,reply.ConflictIndex,reply.ConflictTerm =false,lastIndex+1,-1
		return
	}
	if args.PreLogIndex < rf.getFirstLog().Index {
		reply.Term, reply.Success,reply.ConflictIndex = -1, false,rf.getFirstLog().Index+1
		DPrintf("{Node %v} receives unexpected AppendEntriesRequest %v from {Node %v} because prevLogIndex %v < firstLogIndex %v", rf.me, args, args.LeaderId, args.PreLogIndex, rf.getFirstLog().Index)
		return
	}

	//case2:存在下标为PreLogIndex的冲突日志
	if rf.logs[rf.getRealIndex(args.PreLogIndex)].Term!=args.PreLogTerm{
		conflictLogTerm:=rf.logs[rf.getRealIndex(args.PreLogIndex)].Term
		conflictLogIndex:=args.PreLogIndex-1
		//寻找第一个term为conflictLogTerm的日志下标
		for conflictLogIndex > firstIndex && rf.logs[rf.getRealIndex(conflictLogIndex)].Term ==conflictLogTerm{
			conflictLogIndex--
		}
		reply.Success,reply.ConflictIndex,reply.ConflictTerm =false,conflictLogIndex+1,conflictLogTerm
		return
	}

	//3.将日志添加到logs中
	index:=args.PreLogIndex+1
	for _,entry:=range args.Entries{
		//添加新元素
		if index>rf.getLastLog().Index{
			rf.logs=append(rf.logs,entry)
		}else {
			//已经存在的日志与新添加的日志有冲突则截断后面的日志
			if rf.logs[rf.getRealIndex(index)].Term!=entry.Term{
				rf.logs[rf.getRealIndex(index)]=entry
				rf.logs=rf.logs[0:rf.getRealIndex(index+1)]
			}
		}
		index++
	}

	//for index, entry := range args.Entries {
	//	if entry.Index-firstIndex >= len(rf.logs) || rf.logs[entry.Index-firstIndex].Term != entry.Term {
	//		rf.logs = shrinkEntriesArray(append(rf.logs[:entry.Index-firstIndex], args.Entries[index:]...))
	//		break
	//	}
	//}
	rf.persist(rf.encodeState())
	//DPrintf("term :%d server:%d logs:%v\n",rf.currentTerm,rf.me,rf.logs)
	//	进行第五步操作
	if args.LeaderCommit>rf.commitIndex{
		rf.commitIndex= Min(args.LeaderCommit,index-1)
		//如果需要进行快照或者正在进行快照则先不进行日志提交
		if rf.enableApply&&rf.SnapshotState==SnapSheet {
			rf.cond.Signal()
		}

	}

	reply.Success=true
}
//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//DPrintf("term:%d server:%d state:%d 接收RequestVote candidate:%d requestIndex:%d requestTerm:%d \n",rf.currentTerm,rf.me,rf.state,args.CandidateId,args.LastLogIndex,args.LastLogTerm)
	//1. 判定拒绝投票的情况
	if args.Term<rf.currentTerm||(args.Term==rf.currentTerm&&rf.votedFor!=-1&&rf.votedFor!=args.CandidateId){
		reply.Term,reply.VoteGranted=rf.currentTerm,false
		return
	}
	//请求的Term更大则更新Term;将状态转换为Follower
	if args.Term>rf.currentTerm{
		rf.updateTerm(args.Term)
	}

	//2.比较谁的日志最新
	if !rf.isLogUpToDate(args.LastLogIndex,args.LastLogTerm) {
		reply.Term,reply.VoteGranted=rf.currentTerm,false
		return
	}

	//重置选举定时器;并且将票投给请求方
	//DPrintf("term:%d server:%d state:%d 将票投给了 %d \n",rf.currentTerm,rf.me,rf.state,args.CandidateId)
	ReStartTimer(rf.electTimer,getRandomElectTime())
	rf.votedFor=args.CandidateId
	rf.persist(rf.encodeState())
	reply.Term,reply.VoteGranted=rf.currentTerm,true

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
	return ok
}

func (rf *Raft) sendAppendEntry(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntry", args, reply)
	return ok
}

func (rf *Raft)sendInstallSnapshot(server int,arg *InstallSnapshotArg,reply *InstallSnapshotReply) bool {
	ok :=rf.peers[server].Call("Raft.InstallSnapshot",arg,reply)
	return ok
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := rf.getLastLog().Index+1
	term := rf.currentTerm
	isLeader := rf.state==Leader


	if isLeader{
		entry:=Entry{
			Index: index,
			Term: term,
			Data: command,
		}
		rf.logs=append(rf.logs,entry)

		//DPrintf("term:%d server:%d state:%d 添加了日志:term:%d index:%d content:%v\n",rf.currentTerm,rf.me,rf.state,entry.Term,entry.Index,command)
		rf.persist(rf.encodeState())
	}
	// Your code here (2B).


	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	DPrintf("%d被杀死\n",rf.me)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}
//设置raft当前的状态
func (rf *Raft)SetState(state int)  {
	rf.state=state
	//不是leader重置选举计时器
	if state!=Leader{
		ReStartTimer(rf.electTimer,getRandomElectTime())
	}else{
		//停掉选举计时器
		for i:=0;i<len(rf.peers);i++{
			if i!=rf.me{
				rf.matchIndex[i]=rf.getFirstLog().Index
				rf.nextIndex[i]=rf.getLastLog().Index+1
			}
		}
		rf.electTimer.Stop()
	}

}
//返回最后一个日志的下标
func (rf *Raft)getLastLog()Entry {
	return rf.logs[len(rf.logs)-1]
}
func (rf *Raft)getFirstLog()Entry  {
	return rf.logs[0]
}
//开启选举逻辑
func (rf *Raft)startElection()  {
	voteCount:=1
	arg:=RequestVoteArgs{
		Term: rf.currentTerm,
		CandidateId: rf.me,
		LastLogIndex: rf.getLastLog().Index,
		LastLogTerm: rf.getLastLog().Term,
	}
	for i:=0;i<len(rf.peers);i++{
		if i!=rf.me{
			//向单个对等发个发送投票请求
			go func(index int) {
				reply:=RequestVoteReply{}
				if rf.sendRequestVote(index,&arg,&reply){
					//DPrintf("term:%d server:%d接到%d投票答复\n",rf.currentTerm,rf.me,index)
					rf.mu.Lock()
					defer rf.mu.Unlock()

					//只有currentTerm和state没有改变的情况下才处理请求的响应
					if rf.currentTerm==arg.Term&&rf.state==Candidate{

						if rf.currentTerm<reply.Term{
							rf.updateTerm(reply.Term)
							return
						}
						//DPrintf("term:%d server:%d接到%d进入了锁\n",rf.currentTerm,rf.me,index)
						if reply.VoteGranted{
							//DPrintf("term:%d server:%d接到%d成功投票\n",rf.currentTerm,rf.me,index)
							voteCount++
						}
						//超过半数成为Leader
						if voteCount >(len(rf.peers)/2){
							rf.SetState(Leader)
							DPrintf("term:%d server:%d state:%d 成为Leader \n",rf.currentTerm,rf.me,rf.state)
							rf.sendBroadCast()
						}
					//	如果响应的Term>currentTerm转变状态为Follower
					}
				}
			}(i)
		}
	}
}
func(rf *Raft)handleAppendEntryResponse(index int,arg *AppendEntryArgs) {
	reply:=AppendEntryReply{}

	//处理响应请求
	if rf.sendAppendEntry(index,arg,&reply){
		rf.mu.Lock()
		defer rf.mu.Unlock()

		//只有currentTerm好state没有改变的情况下才进行AppendEntry的逻辑处理
		if arg.Term==rf.currentTerm&&rf.state==Leader{
			if rf.currentTerm<reply.Term{
				rf.updateTerm(reply.Term)
				return
			}
			//成功则更新matchIndex和nextIndex;失败只更新nextIndex
			if reply.Success{
				rf.matchIndex[index]=Max(arg.PreLogIndex+len(arg.Entries),rf.matchIndex[index])
				rf.nextIndex[index]=rf.matchIndex[index]+1

				if rf.nextIndex[index]==0{
					fmt.Printf("更新成功时设置为0\n")
				}

			}else {
				if reply.Term>=0{
					targetIndex:=rf.getRealIndex(reply.ConflictIndex)
					for ;targetIndex>0;targetIndex--{
						if rf.logs[targetIndex].Term==reply.ConflictTerm {
							break
						}
					}
					//case1:能在logs中找到与conflictTerm相同的日志条目
					if targetIndex>0{
						rf.nextIndex[index]=rf.logs[targetIndex].Index+1
						//case2:不能再logs中找到与conflictTerm相同的日志条目
					}else{
						rf.nextIndex[index]=reply.ConflictIndex
					}
				}else{
					rf.nextIndex[index]=reply.ConflictIndex
				}

			}
		}
	}
}

func (rf *Raft)handleInstallSnapshotResponse(index int,arg *InstallSnapshotArg)  {
	DPrintf("server:%d install snapshot arg:%v",rf.me,arg)
	reply:=InstallSnapshotReply{}
	if rf.sendInstallSnapshot(index,arg,&reply){
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if reply.Term>rf.currentTerm{
			rf.updateTerm(reply.Term)
			return
		}
	//更行matchIndex和nextIndex
	rf.matchIndex[index]=arg.LastIncludeIndex
	rf.nextIndex[index]=arg.LastIncludeIndex+1
	}
}
//生成InstallSnapshot RPC调用的请求参数
func (rf *Raft)generateInstallSnapshotArg()* InstallSnapshotArg  {
	arg:=InstallSnapshotArg{
		Term: rf.currentTerm,
		LastIncludeTerm: rf.logs[0].Term,
		LastIncludeIndex: rf.logs[0].Index,
		Data: rf.persister.ReadSnapshot(),
	}
	return &arg
}

//生成AppendEntry RPC调用需要的参数
func (rf *Raft)generateAppendEntryArg(i int) *AppendEntryArgs {
	//构建请求参数
	arg:=AppendEntryArgs{
		Term: rf.currentTerm,
		LeaderId: rf.me,
		PreLogIndex: rf.nextIndex[i]-1,
		Entries: make([]Entry,0),
		LeaderCommit: rf.commitIndex,
	}
	arg.PreLogTerm=rf.logs[rf.getRealIndex(arg.PreLogIndex)].Term
	for j:=rf.nextIndex[i];j<=rf.getLastLog().Index;j++{
		arg.Entries=append(arg.Entries,rf.logs[rf.getRealIndex(j)])
	}

	return &arg
}
//leader向所有对等方发送AppendEntry广播
func (rf *Raft)sendBroadCast()  {

	for i:=0;i<len(rf.peers);i++{
		if i!=rf.me {
			preLogIndex:=rf.nextIndex[i]-1
			//如果需要发送的有部分在快照之内;则需要为Follower调用InstallSnapshot
			if preLogIndex<rf.getFirstLog().Index{

				arg:=rf.generateInstallSnapshotArg()
				go rf.handleInstallSnapshotResponse(i,arg)
			}else{
				arg:=rf.generateAppendEntryArg(i)
				go rf.handleAppendEntryResponse(i,arg)

			}
		}
	}
}

func (rf *Raft)upDateLeaderCommitIndex()  {
	firstLogIndex,lastLogIndex:=rf.getFirstLog().Index,rf.getLastLog().Index
	vec:=make([]int,0,len(rf.peers))
	for i:=0;i<len(rf.peers);i++{
		if i!=rf.me{
			//将符合的下标索引放入到容器中
			if rf.matchIndex[i]>firstLogIndex &&rf.matchIndex[i]<=lastLogIndex&&rf.logs[rf.getRealIndex(rf.matchIndex[i])].Term==rf.currentTerm{
				vec=append(vec,int(rf.matchIndex[i]))
			}
		}
	}
	sort.Sort(sort.Reverse(sort.IntSlice(vec)))
	size:=len(rf.peers)
	if len(vec)>=size/2 {
		v :=vec[size/2-1]

		//更新了commitIndex则唤醒applier
		if v>rf.commitIndex{

			rf.commitIndex=v
			//如果需要进行快照或者正在进行快照则先不进行日志提交
			if rf.enableApply&&rf.SnapshotState==SnapSheet {
				rf.cond.Signal()
			}
		}
	}
}
//循环监听两个定时器事件
func (rf *Raft)timer()  {
	for rf.killed()==false{

		select {
		//进行选举
		case <-rf.electTimer.C:
			rf.mu.Lock()
			rf.currentTerm++
			rf.votedFor=rf.me
			//DPrintf("term:%d server:%d state:%d 成为Candidate \n",rf.currentTerm,rf.me,rf.state)
			rf.SetState(Candidate)
			rf.persist(rf.encodeState())
			rf.startElection()
			rf.mu.Unlock()
		//	发送心跳包
		case <-rf.heartBeatTimer.C:
			rf.mu.Lock()

			if rf.state ==Leader{
				rf.sendBroadCast()
				rf.upDateLeaderCommitIndex()
			}
			rf.heartBeatTimer.Reset(heartBeatTime)

			rf.mu.Unlock()

		}

	}
}
//将可以提交的日志都提交了
func (rf *Raft)applier()  {
	for rf.killed()==false  {
		 rf.mu.Lock()
		 //判断是否可以提交日志
		 for rf.commitIndex<=rf.lastApplied||!rf.enableApply{
		 	rf.cond.Wait()
		 }

		 //复制提交所有可以提交的日志
		 firstIndex,commitIndex, lastApplied :=rf.getFirstLog().Index,rf.commitIndex, rf.lastApplied
		 entries := make([]Entry, commitIndex-lastApplied)

		 copy(entries, rf.logs[lastApplied+1-firstIndex:commitIndex+1-firstIndex])
		 rf.enableApply=false
		 rf.mu.Unlock()
		 //提交日志时是用来channel不能加锁
		 for _,entry:=range entries{
		 	applyMsg:=ApplyMsg{
		 		SnapshotValid: false,
		 		CommandTerm: entry.Term,
		 		CommandValid: true,
		 		CommandIndex: entry.Index,
		 		Command: entry.Data,
			}

			rf.applyCh<-applyMsg
			//DPrintf("term:%d server:%d state:%d 提交了日志:term:%d index:%d content:%v\n",rf.currentTerm,rf.me,rf.state,entry.Term,entry.Index,entry.Data)
		 }
		 rf.mu.Lock()
		 rf.enableApply=true
		 rf.lastApplied=Max(rf.lastApplied,rf.commitIndex)
		 rf.mu.Unlock()


	}
}
func (rf *Raft)SetMaxRaftState(size int )  {
	rf.maxRaftState=size
}
type InstallSnapshotArg struct {
	Term int
	LastIncludeIndex int
	LastIncludeTerm int
	Data []byte
}
type InstallSnapshotReply struct {
	Term int
}
func (rf *Raft) RegisterKVCommunication( ch chan int)  {
	rf.registerKVCommunication=true
	rf.KVCommunication=ch
}
//安装快照的RPC调用
func (rf *Raft) InstallSnapshot(arg *InstallSnapshotArg,reply *InstallSnapshotReply)  {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer DPrintf("{Node %v}'s state is {state %v,term %v,commitIndex %v,lastApplied %v,firstLog %v,lastLog %v} before processing InstallSnapshotRequest %v and reply InstallSnapshotResponse %v", rf.me, rf.state, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.getFirstLog(), rf.getLastLog(), arg, reply)
	reply.Term=rf.currentTerm
	//1.如果term<currentTerm 立即结束
	if rf.currentTerm<arg.Term{
		return
	}

	if arg.Term>rf.currentTerm{
		rf.updateTerm(arg.Term)
	}
	ReStartTimer(rf.electTimer,getRandomElectTime())

	//如果snapshot的lastIncludeIndex小于本地的commitIndex则说明log中已经含有快照中的信息只是没有apply而已;则没有必要安装快照
	if arg.LastIncludeIndex<=rf.commitIndex{
		return
	}
	//使用异步的方式将快照的方式提交
	go func() {
		rf.applyCh<-ApplyMsg{
			SnapshotValid: true,
			CommandValid: true,
			Command: arg.Data,
			CommandTerm: arg.LastIncludeTerm,
			CommandIndex: arg.LastIncludeIndex,
		}
	}()

}
func (rf *Raft)GetLastApplied() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.lastApplied
}
func (rf *Raft)CondInstallSnapshot(lastIncludedTerm int,lastIncludedIndex int ,snapshot []byte)bool  {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("{Node %v} service calls CondInstallSnapshot with lastIncludedTerm %v and lastIncludedIndex %v to check whether snapshot is still valid in term %v", rf.me, lastIncludedTerm, lastIncludedIndex, rf.currentTerm)

	// 快照过期了
	if lastIncludedIndex <= rf.commitIndex {
		DPrintf("{Node %v} rejects the snapshot which lastIncludedIndex is %v because commitIndex %v is larger", rf.me, lastIncludedIndex, rf.commitIndex)
		return false
	}
	if lastIncludedIndex > rf.getLastLog().Index {
		//丢弃所有的日志数据
		rf.logs = make([]Entry, 1)
	} else {
		//保留快照点之后的日志数据
		rf.logs = (rf.logs[lastIncludedIndex-rf.getFirstLog().Index:])
		rf.logs[0].Data= nil
	}
	rf.logs[0].Term, rf.logs[0].Index = lastIncludedTerm, lastIncludedIndex
	rf.lastApplied, rf.commitIndex = lastIncludedIndex, lastIncludedIndex
	rf.persister.SaveStateAndSnapshot(rf.encodeState(), snapshot)
	DPrintf("{Node %v}'s state is {state %v,term %v,commitIndex %v,lastApplied %v,firstLog %v,lastLog %v} after accepting the snapshot which lastIncludedTerm is %v, lastIncludedIndex is %v", rf.me, rf.state, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.getFirstLog(), rf.getLastLog(), lastIncludedTerm, lastIncludedIndex)
	return true


}
//rf层开始进行快照
func (rf *Raft)  StartSnapshot(index int, snapshot []byte)  {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.SnapshotState =SnapSheet
	snapshotIndex:=rf.getFirstLog().Index
	if index<snapshotIndex{
		DPrintf("{Node %v} rejects replacing log with snapshotIndex %v as current snapshotIndex %v is larger in term %v", rf.me, index, snapshotIndex, rf.currentTerm)
		return
	}
	rf.logs=shrinkEntriesArray(rf.logs[index-snapshotIndex:])
	rf.logs[0].Data=nil
	rf.persister.SaveStateAndSnapshot(rf.encodeState(), snapshot)

	DPrintf("{Node %v}'s state is {state %v,term %v,commitIndex %v,lastApplied %v,firstLog %v,lastLog %v} after replacing log with snapshotIndex %v as old snapshotIndex %v is smaller", rf.me, rf.state, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.getFirstLog(), rf.getLastLog(), index, snapshotIndex)

}

func shrinkEntriesArray(data []Entry) []Entry {
	res:=make([]Entry,0,len(data))
	res=append(res,data...)
	data=nil
	return  res
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
	rf := &Raft{
		mu:             sync.Mutex{},
		peers:          peers,
		persister:      persister,
		me:             me,
		dead:           0,
		state:          Follower,
		applyCh: applyCh,
		currentTerm:    0,
		votedFor:       -1,
		logs:                    make([]Entry,1),
		commitIndex:             0,
		lastApplied:             0,
		enableApply:             true,
		matchIndex:              make([]int,len(peers)),
		nextIndex:               make([]int,len(peers)),
		electTimer:              time.NewTimer(getRandomElectTime()),
		heartBeatTimer:          time.NewTimer(heartBeatTime),
		registerKVCommunication: false,
		maxRaftState:            -1,
		SnapshotState:           SnapSheet,
	}
	rf.cond=sync.NewCond(&rf.mu)
	rf.logs[0]=Entry{
		Term: 0,
		Index: 0,
	}
	//DPrintf("%d被启用\n",rf.me)
	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash

	rf.readPersist(persister.ReadRaftState())

	go rf.timer()
	go rf.applier()


	return rf
}
