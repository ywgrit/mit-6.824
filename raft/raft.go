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
	"sync"
	"sync/atomic"
    "math/rand"
    "fmt"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
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

type State int8

const (
    Follower State = iota
    Candidate
    Leader
)

type LogEntry struct {
    Term        int
    Index       int
    Command     interface{}
}

func (s State) String() string {
    if s == Follower {
        return "F"
    } else if s == Candidate {
        return "C"
    } else if s == Leader {
        return "L"
    } else {
        panic("invalid state " + string(rune(s)))
    }
}

func (e *LogEntry) String() string {
    return fmt.Sprintf("{%d t%d %v}", e.Index, e.Term, e.Command)
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

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
    applyCh         chan ApplyMsg
    applyCond       *sync.Cond
    replicatorCond  []*sync.Cond
    state           State

    currentTerm     int
    votedFor        int
    log       []*LogEntry // the first LogEntry don't contain command, it contains lastInludedTerm and lastIncludeIndex 

    commitIndex     int
    lastApplied      int

    // volatile state on leader
    nextIndex       []int // nextIndex[i] is the next index that leader should append log entry to server i 
    matchIndex      []int // matchIndex[i] is the highest log entry's Index that leader has been replicated on server i

    electionTimer *time.Timer
    heartbeatTimer *time.Timer
}

type InstallSnapshotArgs struct {
    Term                int
    LeaderId            int
    LastIncludedIndex   int
    LastIncludedTerm    int
    Data                []byte
}

type InstallSnapshotReply struct {
    Term            int
}

type AppendEntriesArgs struct {
    Term            int
    LeaderId        int
    PrevLogIndex    int
    PrevLogTerm     int
    Entries         []*LogEntry
    LeaderCommit    int
}

type AppendEntriesReply struct {
    Term        int
    Success     bool 
    XTerm       int // 0 or conflict Term
    XIndex      int // if XTerm == 0 XIndex is LogTail().Index, else is the first LogEntry's Index in XTerm
    // XLen        int // if XTerm == -1 XLen is (PrevLogIndex - XIndex), else XLen make no sense
}

func (rf *Raft) Majority() int {
    return len(rf.peers)/2 + 1
}

func (rf *Raft) LastLog() *LogEntry {
    return LastLog(rf.log)
}

func LastLog(Entries []*LogEntry) *LogEntry {
    return Entries[len(Entries) - 1]
}

func (rf *Raft) StateTransfer(state State) {
    rf.state = state
}

// generate a RequestVoteArgs passed to RequestVote 
func (rf *Raft) NewRequestVoteArgs() *RequestVoteArgs {
    return &RequestVoteArgs{
        Term:           rf.currentTerm,
        CandidateId:    rf.me,
        LastLogAtIndex: rf.LastLog().Index,
        LastLogTerm:    rf.LastLog().Term,
    }
}

func (rf *Raft) SubScript(Index int) int {
    return Index - rf.log[0].Index
}

func (rf *Raft) ResetTerm(term int) {
    rf.currentTerm = term
    rf.votedFor = -1
    rf.persist()
}

func Min(a int, b int) int {
    if a > b {
        return b
    } else {
        return a
    }
}

func Max(a int, b int) int {
    if a > b {
        return a
    } else {
        return b
    }
}

func (rf *Raft) LogAtIndex(Index int) *LogEntry {
    if Index < rf.log[0].Index {
        return nil
    }
    subscript := rf.SubScript(Index)
    if subscript < len(rf.log) {
        return rf.log[subscript]
    } else {
        return nil
    }
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
    rf.mu.Lock()
    defer rf.mu.Unlock()
    reply.Term = rf.currentTerm
    if args.Term < rf.currentTerm {
        reply.Success = false
        return
    }
    rf.electionTimer.Reset(ElectionTimeout())
    fmt.Printf("It's AppendEntries, rf.me = %d, 由peer %d发送而来, 重置peer %d的选举时间, 时间戳（毫秒）：%v\n\n\n", rf.me, args.LeaderId, rf.me, time.Now().UnixMilli())

    if rf.state == Candidate && args.Term == rf.currentTerm || args.Term > rf.currentTerm {
        rf.StateTransfer(Follower)
        rf.ResetTerm(args.Term)
    }

    // return conflicting LogEntries if exist
    if args.PrevLogIndex >= rf.log[0].Index { // only this case can there be conflicting LogEntries, but I don't understand how could this happen
        if prev := rf.LogAtIndex(args.PrevLogIndex); prev.Term != args.PrevLogTerm || prev == nil {
            if prev != nil {
                for _, entry := range rf.log {
                    if entry.Term == prev.Term {
                        reply.XIndex = entry.Index
                        break
                    }
                }
                reply.XTerm = prev.Term
            } else {
                reply.XIndex = rf.LastLog().Index
                reply.XTerm = 0
            }
            reply.Success = false
            return
        }
    }

    if len(args.Entries) > 0 { // really appendentries, not heartbeat
        if LastLog(args.Entries).Index > rf.log[0].Index { // if LastLog(arg.Entries).Index < rf.log[0].Index, then don't need append LogEntries, Note: although LastLog(args.Entries).Index > rf.Log[0].Index, args.PrevLogIndex maybe < rf.log[0].Index. So because this case, the only way to append entries to follower is: find the fisrt rf.LogEntry which Index equals to arg.LogEntry but Term is not. But I don't understand how could this happen

            nextsubscript := 0
            for i, entry := range args.Entries {
                if this := rf.LogAtIndex(entry.Index); this != nil {
                    if this.Term != entry.Term {
                        rf.log = rf.log[:rf.SubScript(entry.Index)]
                        nextsubscript = i
                        break
                    }
                    nextsubscript = i + 1
                }
            }
            
            for i := nextsubscript; i < len(args.Entries); i++ {
                entry := *args.Entries[i]
                rf.log = append(rf.log, &entry)
            }
        }
        rf.persist() // rf.log has changed, so rf needs be persisted
    }
  
    if args.LeaderCommit > rf.commitIndex {
        rf.commitIndex = Min(args.LeaderCommit, rf.LastLog().Index) // only after leader commited, can follower commit, leadercommit is maybe before the entries the leader sent to rf(for example, leader has committed loga, and server pass a new command b to leader, then logb will be append to follower). And args.leadercommit is maybe beyond the entries the leader sent to rf(for example, leader sent a log to majority of followers and received reply , but didn't sent this log to follower b yet because of network, and then leader heartbeat follower b, b.rf.LogTail < leader.commitIndex)

        rf.applyCond.Signal()
    }
    reply.Success = true // args.Term >= rf.currentTerm && args.Entries has no conflicting LogEntries with rf.log
}


func (rf *Raft) InstallSnapshot(peer int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) { // receive snapshot, and apply this snapshot
    rf.mu.Lock()
    reply.Term = rf.currentTerm
    if args.Term < rf.currentTerm {
        rf.mu.Unlock()
        return
    }
    rf.electionTimer.Reset(ElectionTimeout())
    
    if args.Term > rf.currentTerm || args.Term == rf.currentTerm && rf.state == Candidate {
        rf.StateTransfer(Follower)
        rf.ResetTerm(args.Term)
    }
    rf.mu.Unlock()

    go func() {
        rf.applyCh <- ApplyMsg{
            CommandValid:       false,
            SnapshotValid:      true,
            SnapshotTerm:       args.LastIncludedTerm,
            SnapshotIndex:      args.LastIncludedIndex,
            Snapshot:           args.Data,
        }
    }()
    return
}

func (rf *Raft) SendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
    return rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
}

func (rf *Raft) SendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
    return rf.peers[server].Call("Raft.AppendEntries", args, reply)
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	// Your code here (2A).
    var term int
    var isLeader bool
    // fmt.Printf("peer %d 进入了GetState()函数中了\n", rf.me)
    rf.mu.Lock()
    // fmt.Printf("peer %d 在GetState()中获得锁了\n\n\n\n", rf.me)
    defer rf.mu.Unlock()
    term = rf.currentTerm
    isLeader = rf.state == Leader
    // fmt.Printf("马上从Getstate函数中返回了 rf.me = %d\n\n\n\n", rf.me)
	return term, isLeader
}

func (rf *Raft) LogIsUpToDate(arg *RequestVoteArgs) bool {
    Term := rf.LastLog().Term
    Index := rf.LastLog().Index
    return arg.LastLogTerm > Term || arg.LastLogTerm == Term && arg.LastLogAtIndex >= Index
}

func (rf *Raft) serializeState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
    e.Encode(rf.log)
	return w.Bytes()
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	rf.persister.SaveRaftState(rf.serializeState())
}

func (rf *Raft) GetStateSize() int {
    return rf.persister.RaftStateSize()
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var term int
	var votedFor int
    var log []*LogEntry
    rf.mu.Lock()
    defer rf.mu.Unlock()
	if d.Decode(&term) != nil ||
	   d.Decode(&votedFor) != nil ||
       d.Decode(&log) != nil {
	    panic("error occur in readPersist")
	} else {
	  rf.currentTerm = term
	  rf.votedFor = votedFor
      rf.log = log
      rf.commitIndex = rf.log[0].Index
      rf.lastApplied = rf.log[0].Index
	}
}


//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).
    rf.mu.Lock()
    defer rf.mu.Unlock()
    if lastIncludedIndex <= rf.commitIndex {
        return false
    }

    if lastIncludedIndex > rf.LastLog().Index {
        rf.log = rf.log[0:1]
    } else {
        rf.log = rf.log[rf.SubScript(lastIncludedIndex):]
    }

    rf.log[0].Index = lastIncludedIndex
    rf.log[0].Term = lastIncludedTerm
    rf.log[0].Command = nil
    rf.commitIndex = lastIncludedIndex
    rf.lastApplied = lastIncludedIndex // because follower has installed this snapshot, so LogEntries in this snapshot is applied
    rf.persister.SaveStateAndSnapshot(rf.serializeState(), snapshot)
	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
// create a snapshot and persist
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
    rf.mu.Lock()
    defer rf.mu.Unlock()
    if index > rf.commitIndex || index <= rf.log[0].Index || rf.LogAtIndex(index) == nil {
        return
    }

    rf.log = rf.log[rf.SubScript(index):]
    rf.log[0].Command = nil
    rf.persister.SaveStateAndSnapshot(rf.serializeState(), snapshot)
}


//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
    Term            int
    CandidateId     int
    LastLogTerm     int // LastTerm and LastLog are used in election limits
    LastLogAtIndex  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
    Term            int // currentTerm, for candidate to update itself
    VoteGranted     bool
}

const (
    ElectionMaxTimeout = int64(500 * time.Millisecond)
    ElectionMinTimeout = int64(400 * time.Millisecond)
    HeartbeatTimeout = int64(100 * time.Millisecond)
)

func ElectionTimeout() time.Duration {
    rand.Seed(time.Now().UnixNano())
    return time.Duration(rand.Int63n(ElectionMaxTimeout - ElectionMinTimeout) + ElectionMinTimeout)
}

//
// example RequestVote RPC handler.
//
// If rf is follower, then rf will vote to args. If rf is candidate, then rf has been voted itself.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
    rf.mu.Lock()
    //fmt.Println("It's in requestvote, with lock")
    defer rf.mu.Unlock()
    reply.Term = rf.currentTerm

    if args.Term < rf.currentTerm || args.Term == rf.currentTerm && rf.votedFor != -1 && rf.votedFor != args.CandidateId {
        // reply.Term, reply.VoteGranted = rf.currentTerm, false
        reply.VoteGranted = false
        return
    }

    if args.Term > rf.currentTerm {// rf will become follower whatever rf's state is
        rf.StateTransfer(Follower)
        // rf.currentTerm, rf.votedFor = args.Term, -1
        rf.ResetTerm(args.Term)
    }

    if !rf.LogIsUpToDate(args) {
        // reply.Term, reply.VoteGranted = rf.currentTerm, false
        reply.VoteGranted = false
        return
    }

    rf.votedFor = args.CandidateId
    rf.persist()
    fmt.Printf("It's in requestvote, voteGranted = true, rf.me = %d, candidate = %d, 重置peer %d的选举时间, 时间戳（毫秒）：%v\n", rf.me, args.CandidateId, rf.me, time.Now().UnixMilli())
    //fmt.Println("It's in requestvote, after persist")
    rf.electionTimer.Reset(ElectionTimeout()) // only vote succeed can reset electiontimer, this ensure elections of servers with more up-to-date won't be interrupted by outdated servers' elections. It is very useful in unreliable network where it is likely that followers have different logs
    // reply.Term, reply.VoteGranted = rf.currentTerm, true
    reply.VoteGranted = true
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
    //fmt.Println("It's in sendrequestvote, after call requestvote")
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
	// Your code here (2B).
    // fmt.Printf("It's in start func\n")
    rf.mu.Lock()
    defer rf.mu.Unlock()
    if rf.state != Leader {
        return -1, -1, false
    }

    entry := LogEntry {
        Term:       rf.currentTerm,
        Index:      rf.LastLog().Index + 1,
        Command:    command,
    }

    rf.log = append(rf.log, &entry)
    rf.persist()
    rf.matchIndex[rf.me] += 1
    for peer := range rf.peers {
        if peer != rf.me {
            rf.replicatorCond[peer].Signal()
        }
    }
	return entry.Index, entry.Term, rf.state == Leader
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
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) StartElection() {
    //fmt.Println("It's in startelection")
    grantedvote := 1
    rf.votedFor = rf.me
    rf.persist()
    for peer := range rf.peers {
        if peer == rf.me {
            continue
        }
        go func(peer int) {
                reply := new(RequestVoteReply)
                args := rf.NewRequestVoteArgs()
                // fmt.Printf("It's startelection's go func's start, rf.me = %d, send to peer %d, 时间戳（毫秒）：%v\n", rf.me, peer, time.Now().UnixMilli())
                if !rf.sendRequestVote(peer, args, reply) {
                fmt.Printf("It's startelection's, after sendRequestVote is failed, rf.me = %d\n", rf.me)
                    return
                }
                fmt.Printf("It's startelection's, rf.me = %d received reply from send to peer %d, 时间戳（毫秒）：%v\n", rf.me, peer, time.Now().UnixMilli())
                rf.mu.Lock()
                fmt.Printf("It's startelection's go func, with lock, rf.me = %d\n", rf.me)
                defer rf.mu.Lock()
                if rf.currentTerm != args.Term || rf.state != Candidate { // because this function start with many goroutine, so rf maybe become leader or other server has become leader, so rf.state != Candidate or rf.currentTerm != args.Term is possible
                    return
                }
                if reply.VoteGranted {
                    grantedvote += 1
                    if grantedvote >= rf.Majority() {
                        for i := 0; i < len(rf.peers); i++ {
                            rf.nextIndex[i] = rf.LastLog().Index + 1
                            rf.matchIndex[i] = 0
                        }
                        rf.matchIndex[rf.me] = rf.LastLog().Index
                        rf.StateTransfer(Leader)
                        // if term, isLeader := rf.GetState(); isLeader {
                        fmt.Printf("peer %d state = %d, rf.term = %d, 时间戳（毫秒）： %v\n\n\n", rf.me, rf.state, rf.currentTerm, time.Now().UnixMilli())
                        // }
                        // fmt.Printf("startelection getstate() return\n\n\n\n")
                        rf.Heartbeat()
                        rf.heartbeatTimer.Reset(time.Duration(HeartbeatTimeout))
                    }
                } else if reply.Term > rf.currentTerm {
                    rf.StateTransfer(Follower)
                    rf.ResetTerm(reply.Term)
                    return
                }
        }(peer)
    }
}

func (rf *Raft) Applier() {
    // fmt.Println("It's in Applier, applyCond.L has not locked yet")
    rf.applyCond.L.Lock()
    //fmt.Println("It's in Applier, applyCond.L has locked")
    defer rf.applyCond.L.Unlock()
    for {
        for !rf.NeedApply() { // need judge constantly, because maybe exist fake awaken
            // fmt.Println("It's Applier, has judged")
            rf.applyCond.Wait()
            fmt.Println("It's Applier, need apply1")
            if rf.killed() { // It's usually be ignored, but this is necessary
                return
            }
            fmt.Println("It's Applier, need apply2")
        }

        rf.mu.Lock()

        rf.lastApplied += 1
        entry := rf.LogAtIndex(rf.lastApplied)
        rf.mu.Unlock()

        rf.applyCh <- ApplyMsg{
            Command:        entry.Command,
            CommandValid:   true,
            CommandIndex:   entry.Index,
        }
    }
}

func (rf *Raft) NeedApply() bool {
    rf.mu.Lock()
    //fmt.Println("It's in NeedApply")
    defer rf.mu.Unlock()
    return rf.NeedApplyWithOutLock()
}

func (rf *Raft) NeedApplyWithOutLock() bool {
    return rf.commitIndex > rf.lastApplied
}

func (rf *Raft) NeedReplicate(peer int) bool {
    rf.mu.Lock()
    defer rf.mu.Unlock()
    return rf.state == Leader && peer != rf.me && rf.matchIndex[peer] < rf.LastLog().Index
}


func (rf *Raft) Replicate(peer int) { // both invoked in heartbeat and replicator which uesd to appendentries
    fmt.Printf("rf.me = %d, It's in replicate, 时间戳（毫秒）：%v\n\n\n", rf.me, time.Now().UnixMilli())
    rf.mu.Lock()
    // fmt.Printf("rf.me = %d, It's in replicate, 时间戳（毫秒）：%v\n\n\n", rf.me, time.Now().UnixMilli())
    if rf.state != Leader {
        fmt.Println("rf isn't leader, but rf call replicate, it should be forbidden")
        rf.mu.Unlock()
        return
    }
    
    if rf.nextIndex[peer] <= rf.log[0].Index { // send snapshot, not LogEntries
        args := &InstallSnapshotArgs{
            Term:               rf.currentTerm,
            LeaderId:           rf.me,
            LastIncludedIndex:  rf.log[0].Index,
            LastIncludedTerm:   rf.log[0].Term,
            Data:               rf.persister.ReadSnapshot(),
        }
        reply := new(InstallSnapshotReply)
        
        rf.mu.Unlock()
        if !rf.SendInstallSnapshot(peer, args, reply) {
            return
        }
        rf.mu.Lock()

        if rf.currentTerm != args.Term { // rf.currentTerm > args.Term, the reply is for previous term, reply is outdated, then the reply should be ignored. Reference:https://thesquareplanet.com/blog/students-guide-to-raft/#term-confusion
            rf.mu.Unlock()
            return
        }
        if reply.Term > rf.currentTerm {
            rf.StateTransfer(Follower)
            rf.ResetTerm(reply.Term)
            rf.mu.Unlock()
            return
        }

        rf.nextIndex[peer] = args.LastIncludedIndex + 1
        rf.mu.Unlock()
        return
    }

    var entries []*LogEntry // if Replicate is invoked in heartbeat, then logentries is nil
    nextIndex := rf.nextIndex[peer]
    for j := nextIndex; j <= rf.LastLog().Index; j++ { // batch appendentries, Note: can't be j <len(rf.log), because log[0].index maybe is not zero
        EntryIndex := rf.LogAtIndex(j)
        if EntryIndex == nil {
            fmt.Printf("%d beyond log's bound", j)
        }
        entry := *EntryIndex // can't append rf.LogAtIndex(j) to follower, must create an new LogEntry
        entries = append(entries, &entry)
    }
    prev := rf.LogAtIndex(nextIndex - 1)
    args := &AppendEntriesArgs{
        Term:               rf.currentTerm,
        LeaderId:           rf.me,
        PrevLogIndex:       prev.Index,
        PrevLogTerm:        prev.Term,
        Entries:            entries,
        LeaderCommit:       rf.commitIndex,
    }
    rf.mu.Unlock()
    reply := new(AppendEntriesReply)
    fmt.Printf("rf.me = %d, I'm going to sendappendentries\n\n\n\n", rf.me)
    if rf.SendAppendEntries(peer, args, reply) {
        rf.mu.Lock()
        rf.HandleReplyWithOutLock(peer, args, reply)
        rf.mu.Unlock()
    }
}

func (rf *Raft) HandleReplyWithOutLock(peer int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
    if rf.currentTerm != args.Term { // rf.currentTerm > args.Term, the reply is for previous term, reply is outdated, then the reply should be ignored. Reference:https://thesquareplanet.com/blog/students-guide-to-raft/#term-confusion
        return
    }
    if reply.Success {
        if len(args.Entries) == 0 { // heartbeat, so leader didn't send any LogEntry to follower, so reply doesn't contain ConflictingTerm and ConflictingIndex
            return
        }
        LastLogAtIndex := LastLog(args.Entries).Index
        rf.matchIndex[peer] = LastLogAtIndex
        rf.nextIndex[peer] = LastLogAtIndex + 1

        //update commitIndex
        preCommitIndex := rf.commitIndex
        for i := rf.commitIndex + 1; i <= LastLogAtIndex; i++ {
            count := 0
            for p := range rf.peers {
                if rf.matchIndex[p] >= i {
                    count += 1
                }
            }
            if count >= rf.Majority() && rf.LogAtIndex(i).Term == rf.currentTerm {
                preCommitIndex = i
            }
        }
        rf.commitIndex = preCommitIndex
        
        if rf.commitIndex > rf.lastApplied {
            rf.applyCond.Broadcast()
        }
    } else { // appendentries failed, maybe reply.Term < rf.currentTerm or there be some conflicting LogEntries
        if reply.Term < rf.currentTerm {
            return
        }
        if reply.Term > rf.currentTerm {
            rf.StateTransfer(Follower)
            rf.ResetTerm(reply.Term)
            return
        }
        nextIndex := rf.nextIndex[peer]
        rf.matchIndex[peer] = 0

        if reply.XTerm > 0 { // args.Entries have a conflict LogEntries with rf.peers[peer]
            for i := len(rf.log) - 1; i >= 1; i-- {
                if rf.log[i].Term == reply.XTerm {
                    rf.nextIndex[peer] = Min(nextIndex, rf.log[i].Index + 1) // I don't understand
                    return
                }
            }
        }
        rf.nextIndex[peer] = Max(Min(nextIndex, reply.XIndex), 1) // I don't understand
    }
}

func (rf *Raft) Replicator(peer int) {
    //fmt.Println("It's in Replicator")
    rf.replicatorCond[peer].L.Lock()
    defer rf.replicatorCond[peer].L.Unlock()
    for {
        for !rf.NeedReplicate(peer) {
            rf.replicatorCond[peer].Wait()
            if rf.killed() { // rf may end, so need to check
                return
            }
        }

        //fmt.Println("It's in Replicator, replicatorCond is wakeup")
        rf.Replicate(peer)
    }
}

func (rf *Raft) Heartbeat() {
    fmt.Printf("It's in heartbeat, 时间戳（毫秒）：%v\n", time.Now().UnixMilli())
    for peer := range rf.peers {
        if peer != rf.me {
            go rf.Replicate(peer)
        }
    }
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
    fmt.Println("It's in ticker")
	for rf.killed() == false {
        select {
        case <- rf.electionTimer.C:
            fmt.Printf("It's ticker, rf.me = %d, rf.electionTimer.C, rf.state = %v, 时间戳（毫秒）：%v\n", rf.me, rf.state, time.Now().UnixMilli())
            rf.mu.Lock()
            // fmt.Println("It's ticker, rf.electionTimer.C, with lock")
            // fmt.Printf("It's ticker, rf.me = %d, rf.electionTimer.C, rf.state = %v, 时间戳（毫秒）：%v\n", rf.me, rf.state, time.Now().UnixMilli())
            if rf.state != Leader {
                rf.StateTransfer(Candidate)
                rf.currentTerm += 1
                rf.StartElection()
                fmt.Printf("选举函数立刻返回, rf.me = %d, 重置peer %d的选举时间, 时间戳（毫秒）：%v\n", rf.me, rf.me, time.Now().UnixMilli())
            }
                rf.electionTimer.Reset(ElectionTimeout()) // if network is bad, appendentries rpc or apply may be discarded or delayed, so candidate need startelection again
                // fmt.Printf("选举函数立刻返回, rf.me = %d, 重置peer %d的选举时间, 时间戳（毫秒）：%v\n", rf.me, rf.me, time.Now().UnixMilli())
                rf.mu.Unlock()
        case <- rf.heartbeatTimer.C:
            // fmt.Printf("It's ticker, rf.me = %d, rf.heartbeatTimer.C, 时间戳（毫秒）：%v\n", rf.me, time.Now().UnixMilli())
            rf.mu.Lock()
            // fmt.Println("It's ticker, rf.heartbeatTimer.C, with lock")
            if rf.state == Leader {
                rf.Heartbeat()
                rf.heartbeatTimer.Reset(time.Duration(HeartbeatTimeout))
            }
            rf.mu.Unlock()
        }
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start 
// goroutines for any long-running work.
// just one Raft server, the system consists of multiple Raft server
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
        rf := &Raft{
            peers:          peers,
            persister:      persister,
            me:             me,
            dead:           0,
            applyCh:        applyCh,
            replicatorCond: make([]*sync.Cond, len(peers)),
            state:          Follower,
            currentTerm:    0,
            votedFor:       -1,
            nextIndex:      make([]int, len(peers)),
            matchIndex:     make([]int, len(peers)),
            heartbeatTimer: time.NewTimer(time.Duration(HeartbeatTimeout)),
            electionTimer:  time.NewTimer(ElectionTimeout()),
        }
	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash

	// rf.readPersist(persister.ReadRaftState())
    rf.log = append(rf.log, &LogEntry{ // the first LogEntry contain nil command 
        Term:       0,
        Index:      0,
        Command:    nil,
    })
    rf.applyCond = sync.NewCond(&sync.Mutex{})
    Index := rf.LastLog().Index
    for i := 0; i < len(peers); i++ {
        rf.matchIndex[i], rf.nextIndex[i] = 0, Index + 1
        if i != rf.me {
            rf.replicatorCond[i] = sync.NewCond(&sync.Mutex{})
            // start replicator goroutine to replicate LogEntries in batch
            go rf.Replicator(i)
        }
    }
    
    if len(rf.log) != 1 {
        panic("len(rf.log) != 1")
    }

	// start ticker goroutine to start elections
	go rf.ticker()

    // start applier goroutine to pass commited log to server or tester
    go rf.Applier()

	rf.readPersist(persister.ReadRaftState()) // I don't understand why don't put readPersist before rf.log = append()

	return rf
}
