package raft

import (
	"math/rand"
	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

const (
	Leader    = 1
	Follower  = 2
	Candidate = 3
	RoleNone  = -1
	None      = 0
)
const (
	HeartBeatLogType   int = 1
	AppendEntryLogType int = 2

	DetectMatchIndexLogType int = 3
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Type     int
	LogTerm  int64
	LogIndex int64
	data     []byte
}

type RaftLog struct {
	Entries     []*LogEntry
	NextIndexs  []int64
	MatchIndexs []int64
}

// A Go object implementing a single Raft peer.
type Raft struct {
	Mu        sync.Mutex          // Lock to protect shared access to this peer's state
	Peers     []*labrpc.ClientEnd // RPC end points of all peers
	Persister *Persister          // Object to hold this peer's persisted state
	Me        int                 // this peer's index into peers[]
	Dead      int32               // set by Kill()
	nPeers    int
	Role      string //角色，是竞选者还是领导还是追随者
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	Term             int       //这个节点的任期，也就是这个节点的版本
	VoteFor          int       //我选谁
	HeartDance       time.Time //发动心跳周期
	Candidate        time.Time //超市选举周期
	Candidatetimeout time.Duration
	Leader_id        int      //领导id
	log              *RaftLog //日志
}

func StateString(m int) string {
	switch m {
	case Leader:
		return "Leader"
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	}
	return "Unknown"
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.Mu.Lock()
	defer rf.Mu.Unlock()
	// Your code here (2A).
	return rf.Term, rf.Role == "Leader"
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	Term         int   //当前版本号
	CandidataId  int   //当前候选人的id
	LastLogIndex int64 //最后的日志来自的坐标
	LastLogTerm  int64 //最后的日志的版本
	// Your data here (2A, 2B).
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	Term  int  //版本号
	Voted bool //是否投票了
	// Your data here (2A).
}

// example RequestVote RPC handler.
/*
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	//这个是要用来投票的时候用的，就是说你当前是什么
	rf.Mu.Lock()
	defer rf.Mu.Unlock()
	reply.Voted = false
	reply.Term = rf.Term
	if rf.Term > args.Term { //版本不如投票者新
		return
	}
	if args.Term > rf.Term {
		rf.Term = args.Term
		rf.Role = StateString(Follower)
		rf.VoteFor = -1
		rf.Leader_id = -1
	}
	if rf.VoteFor == -1 || rf.VoteFor == args.CandidataId {
		LastLogIndex, LastLogTerm := rf.getLogIT()
		if LastLogIndex <= args.LastLogIndex && LastLogTerm <= args.LastLogTerm { //节点要保证更长也就是更新，要保证任期最长才能被投票
			rf.VoteFor = args.CandidataId
			rf.Candidate = time.Now() //一旦选举了就重置心跳，免得待会自己变成选举人了
			rf.Candidatetimeout = RandomCandidate()
			rf.Leader_id = args.CandidataId
			reply.Voted = true
		}
	}
	rf.persist()
}

*/
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	rf.Mu.Lock()
	defer rf.Mu.Unlock()
	reply.Term = rf.Term
	reply.Voted = false

	//不接收小于自己term的请求
	if rf.Term > args.Term {
		return
	}
	if args.Term > rf.Term {
		rf.Role = StateString(Follower) //leader转换为follower
		rf.Term = args.Term
		rf.VoteFor = -1
		rf.Leader_id = -1
		rf.persist()
	}
	//避免重复投票
	if rf.VoteFor == RoleNone || rf.VoteFor == args.CandidataId {
		lastLogTerm, lastLogIndex := rf.getLogIT()
		if args.LastLogTerm > lastLogTerm || (lastLogIndex <= args.LastLogIndex && lastLogTerm == args.LastLogTerm) {
			rf.VoteFor = args.CandidataId
			rf.Leader_id = args.CandidataId
			reply.Voted = true
			rf.Role = StateString(Follower)
			//为其他人投票，则重置自己的超时时间
			rf.Candidate = time.Now()
			rf.Candidatetimeout = RandomCandidate()
			rf.persist()
		}
	}

}
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.Mu.Lock()
	defer rf.Mu.Unlock()
	reply.Term = rf.Term
	reply.IsAccept = false
	if args.Term < reply.Term {
		return
	} //旧版本的不能作为leader心跳
	if args.Term > reply.Term {
		//如果更大的话直接转变成这个点的跟随者
		rf.Term = args.Term
		rf.Role = StateString(Follower)
		rf.Leader_id = -1
		rf.VoteFor = -1
		return
	}
	rf.Leader_id = args.LeaderId //谁发心跳谁是领导
	rf.Candidate = time.Now()    //重置选举时间
	rf.persist()
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.Peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	LeaderId   int
	Term       int //类似vote，但是现在是心跳要做的东西了，心跳要多传一个日志
	LogEntries []*LogEntry
}

type AppendEntriesReply struct {
	IsAccept     bool
	Term         int
	NextLogTerm  int64
	NextLogIndex int64
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.Peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.Dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.Dead)
	return z == 1
}
func RandomCandidate() time.Duration { //重新选举
	duration := time.Duration(150*time.Millisecond + time.Duration(rand.Int())%150*time.Millisecond)
	return duration
}
func RandomHeatDanceTime() time.Duration { //心跳
	duration := time.Duration(100 * time.Millisecond)
	return duration
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		time.Sleep(time.Millisecond * 1) //防止过快请求炸了
		func() {
			if rf.Role == StateString(Leader) {
				return
			}
			eps := time.Now().Sub(rf.Candidate)
			timeout := rf.Candidatetimeout
			if eps < timeout {
				return
			} //如果还没超时就继续
			//这边为什么不用timer呢？因为我怕timer的阻塞会在stop之后松了，为什么会stop呢？因为reset会先stop再重新设计
			rf.Mu.Lock()
			if rf.Role == StateString(Follower) {
				rf.Role = StateString(Candidate) //变成候选人
			}
			maxterm, votecount := rf.becomeCandidate()
			rf.Mu.Lock()
			defer rf.Mu.Unlock()
			rf.Candidate = time.Now()
			if rf.Role != StateString(Candidate) {
				return
			}
			if maxterm > rf.Term {
				rf.Role = StateString(Follower) //直接变成追随者，因为说明了自己的任期不如选举者的任期，应该让选举者来当日志才是最新的
				rf.Term = maxterm               //被ping了
				rf.VoteFor = -1
				rf.Leader_id = -1
			} else if votecount > rf.nPeers/2 {
				rf.Role = StateString(Leader)
				rf.HeartDance = time.Unix(0, 0) //定义一个很前面的时间，因为我们需要直接就进行心跳
				rf.Leader_id = rf.Me
			}
			rf.persist()
		}()
	}
}
func (rf *Raft) getLogIT() (int64, int64) {
	var LastLogIndex, LastLogTerm int64
	if len(rf.log.Entries) == 0 {
		LastLogTerm = 0
		LastLogIndex = 0
	} else {
		siz := len(rf.log.Entries)
		LastLogIndex = rf.log.Entries[siz-1].LogIndex
		LastLogTerm = rf.log.Entries[siz-1].LogTerm
	}
	return LastLogIndex, LastLogTerm
}
func (rf *Raft) becomeCandidate() (int, int) { //当前这个变成候选了
	/*
		rf.Role = StateString(Candidate)
		rf.VoteFor = rf.Me
		rf.Term++
		rf.persist()
		lastLogTerm, lastLogIndex := rf.getLogIT()
		rf.Mu.Unlock()

		type RequestVoteResult struct {
			peerId int
			resp   *RequestVoteReply
		}
		voteChan := make(chan *RequestVoteResult, rf.nPeers-1)
		args := &RequestVoteArgs{
			Term:         rf.Term,
			CandidataId:  rf.Me,
			LastLogIndex: lastLogIndex,
			LastLogTerm:  lastLogTerm,
		}
		for i := 0; i < rf.nPeers; i++ {
			if rf.Me == i {
				continue
			}
			go func(server int, args *RequestVoteArgs) {
				reply := &RequestVoteReply{}
				ok := rf.sendRequestVote(server, args, reply)
				if ok {
					voteChan <- &RequestVoteResult{
						peerId: server,
						resp:   reply,
					}
				} else {
					voteChan <- &RequestVoteResult{
						peerId: server,
						resp:   nil,
					}
				}
			}(i, args)
		}

		maxTerm := rf.Term
		voteGranted := 1
		totalVote := 1
		for i := 0; i < rf.nPeers-1; i++ {
			select {
			case vote := <-voteChan:
				totalVote++
				if vote.resp != nil {
					if vote.resp.Voted {
						voteGranted++
					}
					//出现更大term就退回follower
					if vote.resp.Term > maxTerm {
						maxTerm = vote.resp.Term
					}
				}
				if voteGranted > rf.nPeers/2 {
					return maxTerm, voteGranted
				}
			}
		}
		return maxTerm, voteGranted

	*/
	rf.Role = StateString(Candidate)
	rf.VoteFor = rf.Me
	rf.Term++                                             //让自己的版本+1，让自己有最新版本
	votechan := make(chan *RequestVoteReply, rf.nPeers-1) //除了自己以外都加进去
	LastLogIndex, LastLogTerm := rf.getLogIT()
	args := &RequestVoteArgs{
		Term:         rf.Term,
		CandidataId:  rf.Me,
		LastLogIndex: LastLogIndex,
		LastLogTerm:  LastLogTerm,
	}
	rf.Mu.Unlock()
	for i := 0; i < rf.nPeers; i++ {
		if rf.Me == i {
			continue
		}
		go func(peer int, args *RequestVoteArgs) {
			reply := &RequestVoteReply{}
			ok := rf.sendRequestVote(peer, args, reply)
			if ok {
				votechan <- reply
			} else {
				votechan <- reply
			}
		}(i, args)
	}
	maxterm := rf.Term
	votecount := 1
	for i := 0; i < rf.nPeers-1; i++ {
		select {
		case vote := <-votechan:
			if vote.Voted == false {
				continue
			}
			votecount++ //被投票数
			//一旦有更大的term就说明自己不是周期最大的，就退回成follower状态
			if vote.Term > maxterm {
				maxterm = vote.Term
			}
		}
		if votecount > rf.nPeers/2 {
			return maxterm, votecount
		}
	}
	return maxterm, votecount
}

func (rf *Raft) heartbeat() {
	for rf.killed() == false {
		time.Sleep(time.Millisecond)
		func() {
			if rf.Role != StateString(Leader) {
				return
			}
			if time.Now().Sub(rf.HeartDance) < RandomHeatDanceTime() {
				return
			}
			maxTerm := rf.sendHeartBeat()
			rf.Mu.Lock()
			if maxTerm > rf.Term {
				//先更新时间，避免leader一进入follower就超时
				rf.Term = maxTerm
				rf.Candidate = time.Now()
				rf.Role = StateString(Follower) //遇到term更高的了，说明已经经过一轮选举了
				rf.VoteFor = RoleNone
				rf.Leader_id = RoleNone
			} else {
				rf.Candidate = time.Now()
				//rf.timeoutInterval = heartBeatTimeout()
			}
			rf.Mu.Unlock()
		}()
	}
}
func Max(a int, b int) int {
	if a > b {
		return a
	} else {
		return b
	}
}
func (rf *Raft) sendHeartBeat() int {
	logentry := make([]*LogEntry, 0)
	logentry = append(logentry, &LogEntry{
		Type: HeartBeatLogType,
	})
	args := &AppendEntriesArgs{
		LeaderId:   rf.Me,
		Term:       rf.Term,
		LogEntries: logentry,
	}
	heartchan := make(chan *AppendEntriesReply, rf.nPeers-1)
	for i := 0; i < rf.nPeers; i++ {
		if i == rf.Me {
			continue
		}
		go func(peer int, args *AppendEntriesArgs) {
			reply := &AppendEntriesReply{}
			ok := rf.sendAppendEntries(peer, args, reply)
			if ok {
				heartchan <- reply
			} else {
				heartchan <- reply
			}
		}(i, args)
	}
	maxterm := rf.Term

	for {
		select {
		case ca := <-heartchan:
			if ca.IsAccept == false {
				continue
			}
			maxterm = Max(maxterm, ca.Term)
		}
	}
	return maxterm
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		Mu:        sync.Mutex{},
		Peers:     peers,
		Persister: persister,
		Me:        me,
		nPeers:    len(peers),
		Role:      StateString(Follower),
		Candidate: time.Now(),
		Term:      0,
		VoteFor:   -1,
		Leader_id: -1,
	}
	rf.log = &RaftLog{
		Entries:     make([]*LogEntry, 0),
		NextIndexs:  make([]int64, rf.nPeers),
		MatchIndexs: make([]int64, rf.nPeers),
	}
	rf.readPersist(persister.ReadRaftState())
	go rf.ticker()
	go rf.heartbeat()
	return rf
}
