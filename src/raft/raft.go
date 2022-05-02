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
	"math/rand"
	"runtime"
	"sync"
	"sync/atomic"
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
	SnapshotTerm  int // the lastIncludedTerm
	SnapshotIndex int // the lastIncludedIndex
}

type State int8

const (
	Follower State = iota
	Candidate
	Leader
)

type LogEntry struct {
	Term    int
	Index   int
	Command interface{}
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
	mu              sync.Mutex          // Lock to protect shared access to this peer's state
	peers           []*labrpc.ClientEnd // RPC end points of all peers
	persister       *Persister          // Object to hold this peer's persisted state
	me              int                 // this peer's index into peers[]
	dead            int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	applyCh         chan ApplyMsg
	applyCond       *sync.Cond
	replicatorCond  []*sync.Cond
	state           State

	currentTerm     int
	votedFor        int
	log             []*LogEntry

	commitIndex     int
	lastApplied     int

	nextIndex       []int
	matchIndex      []int

    electionTimer   *time.Timer
    heartbeatTimer  *time.Timer
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []*LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term             int
	Success          bool
	XTerm            int
	XIndex           int
}

func (rf *Raft) Majority() int {
	return len(rf.peers)/2 + 1
}

func (rf *Raft) LastLog() *LogEntry {
	return LastLog(rf.log)
}

func LastLog(Entries []*LogEntry) *LogEntry {
	return Entries[len(Entries)-1]
}

func (rf *Raft) StateTransfer(state State) {
    rf.state = state
}

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
	if len(rf.log) > subscript {
		return rf.log[subscript]
	} else {
		return nil
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// now := time.Now()
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}
    rf.electionTimer.Reset(ElectionTimeout())

	if rf.state == Candidate || args.Term > rf.currentTerm {// rf is candidate, so rf want become leader, but now rf received appendentries news which comes from leader, so rf will know there is a leader, then rf will become follower and reset rf.term
        rf.StateTransfer(Follower)
		rf.ResetTerm(args.Term) // args is leader, so reset rf.term according to args.term
    }
	if args.PrevLogIndex >= rf.log[0].Index { // only in this case can there be conflict logentries
		if prev := rf.LogAtIndex(args.PrevLogIndex); prev == nil || prev.Term != args.PrevLogTerm {
			if prev != nil {
				for _, entry := range rf.log { // find the first LogEntry in prev.Term
					if entry.Term == prev.Term {
						reply.XIndex = entry.Index
						break
					}
				}
				reply.XTerm = prev.Term
			} else { // args.PrevLogIndex is beyond rf's LogEntry's bound
				reply.XIndex = rf.LastLog().Index
				reply.XTerm = 0
			}
			reply.Success = false
			return
		}
	}
	if len(args.Entries) > 0 { // not heartbeat, leader does append LogEntries here
		// if pass log consistency check, do merge
        if LastLog(args.Entries).Index > rf.log[0].Index { // if LastLog(arg.Entries).Index < rf.log[0].Index, then don't need append LogEntries, Note: although LastLog(args.Entries).Index > rf.Log[0].Index, args.PrevLogIndex maybe < rf.log[0].Index. So because this case, the only way to append entries to follower is: find the fisrt rf.LogEntry which Index equals to arg.LogEntry but Term is not
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
		rf.persist()
	}
	// trigger apply
	if args.LeaderCommit > rf.commitIndex {
        rf.commitIndex = Min(args.LeaderCommit, rf.LastLog().Index) // only after leader commited, can follower commit, leadercommit is maybe before the entries the leader sent to rf(for example, leader has committed loga, and server pass a new command b to leader, then logb will be append to follower). And args.leadercommit is maybe beyond the entries the leader sent to rf(for example, leader sent a log to majority of followers and received reply , but didn't sent this log to follower b yet because of network, and then leader heartbeat follower b, b.rf.LastLog < leader.commitIndex)
		// rf.applyCond.Broadcast()
		rf.applyCond.Signal()
	}
	reply.Success = true
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) { // rf not really install, just receive snapshot, and apply this 
    rf.mu.Lock()
    reply.Term = rf.currentTerm
    if args.Term < rf.currentTerm {
        rf.mu.Unlock()
        return
    }

    rf.electionTimer.Reset(ElectionTimeout())
    if args.Term > rf.currentTerm || rf.state == Candidate {
        rf.StateTransfer(Follower)
        rf.ResetTerm(args.Term)
    }
    rf.mu.Unlock()
    
    go func() {
        rf.applyCh <- ApplyMsg{
            CommandValid:   false,
            SnapshotValid:  true,
            Snapshot:       args.Data,
            SnapshotTerm:   args.LastIncludedTerm,
            SnapshotIndex:  args.LastIncludedIndex,
        }
    }()

    return
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
    return rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	return rf.peers[server].Call("Raft.AppendEntries", args, reply)
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.state == Leader
	return term, isleader
}

func (rf *Raft) LogIsUpToDate(args *RequestVoteArgs) bool {
    Term := rf.LastLog().Term
    Index := rf.LastLog().Index
    return args.LastLogTerm > Term || args.LastLogTerm == Term && args.LastLogAtIndex >= Index
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) serializeState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	return w.Bytes()
}

func (rf *Raft) persist() {
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
// leader sent some snapshot to this follower, now follower want install snapshot
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
    rf.mu.Lock()
    defer rf.mu.Unlock()
    if lastIncludedIndex <= rf.commitIndex { // snapshot is outdated, discard this snapshot
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
    rf.lastApplied = lastIncludedIndex // because follower has installed this snapshot, so logentries in this snapshot is applied
    rf.persister.SaveStateAndSnapshot(rf.serializeState(), snapshot)
    return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
// service find log is too big, so service wants to compaction log, snapshot is database, and snapshot should be persisted in persister
func (rf *Raft) Snapshot(index int, snapshot []byte) {
    rf.mu.Lock()
    defer rf.mu.Unlock()
    if index > rf.commitIndex || index <= rf.log[0].Index || rf.LogAtIndex(index) == nil { // It's wrong, only can take snapshot of LogEntries which rf.log[0].Index < index <= commitIndex
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
	Term            int
	CandidateId     int
	LastLogTerm     int
	LastLogAtIndex  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

const (
	ElectionTimeoutMax = int64(600 * time.Millisecond)
	ElectionTimeoutMin = int64(500 * time.Millisecond)
	HeartbeatTimeout  = int64(100 * time.Millisecond) // the tester requires thst the leader send heartbeat RPCs no more than ten times per second
)

func ElectionTimeout() time.Duration {
	rand.Seed(time.Now().UnixNano())
	return time.Duration(rand.Int63n(ElectionTimeoutMax-ElectionTimeoutMin) + ElectionTimeoutMin) // already Millisecond
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// now := time.Now()
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
    if args.Term < rf.currentTerm || args.Term == rf.currentTerm && rf.votedFor != args.CandidateId && rf.votedFor != -1 {
        reply.VoteGranted = false
        return
    }

    if args.Term > rf.currentTerm {
        rf.StateTransfer(Follower)
        rf.ResetTerm(args.Term)
    }

    if !rf.LogIsUpToDate(args) {
        reply.VoteGranted = false
        return
    }

    rf.votedFor = args.CandidateId
    rf.persist()
    // rf.lastHeartbeat = now
    rf.electionTimer.Reset(ElectionTimeout())
    // fmt.Printf("It's in requestvote, rf.me = %d, 同意投票给 peer %d, 重置peer %d的选举时间， 时间戳（毫秒）: %v\n", rf.me, args.CandidateId, rf.me, time.Now().UnixMilli())
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
	return rf.peers[server].Call("Raft.RequestVote", args, reply)
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
	if rf.state != Leader {
		return -1, -1, false
	}
	entry := LogEntry{
		Term:    rf.currentTerm,
		Index:   rf.LastLog().Index + 1,
		Command: command,
	}
	rf.log = append(rf.log, &entry)
	rf.persist()
	rf.matchIndex[rf.me] += 1
	for peer := range rf.peers {
		if peer != rf.me {
		    // rf.replicatorCond[peer].Signal()
            go rf.Replicate(peer)
		}
	}
	// use replicatorCond[i] (replicate by Sync) could significantly reduce RPC call times since
	// multiple Start will be batched into a single round of replication. but doing so will introduce
	// longer latency because replication is not triggered immediately, and higher CPU time due to
	// serialization is more costly when dealing with longer AppendEntries RPC calls.
	// personally I think the commented Sync design is better, but that wont make us pass speed tests in Lab 3.
	// TODO: how could the actual system find the sweet point between network overhead and replication latency?
	// rf.BroadcastHeartbeat()// go replicate()
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
    grantedvote := 1
    rf.votedFor = rf.me
    rf.persist()
    // rf.Debug(dElection, "electionTimeout %dms elapsed, turning to Candidate", rf.electionTimeout/time.Millisecond)

    for peer := range rf.peers {
        if peer == rf.me {
            continue
        }
        go func(peer int) {
            rf.mu.Lock()
            reply := new(RequestVoteReply)
            args := rf.NewRequestVoteArgs()
            rf.mu.Unlock()
            if !rf.sendRequestVote(peer, args, reply) {
                return
            }
            rf.mu.Lock()
            defer rf.mu.Unlock()
            if rf.state != Candidate || rf.currentTerm != args.Term {
                return
            }
            if reply.VoteGranted {
                grantedvote += 1

                if grantedvote >= rf.Majority() {
                    // fmt.Printf("It's in startelection, 成功选举出leader %d, 这是在peer %d, 时间戳（毫秒）: %v\n", rf.me, peer, time.Now().UnixMilli())
                    for i := 0; i < len(rf.peers); i++ {
                        rf.nextIndex[i] = rf.LastLog().Index + 1
                        rf.matchIndex[i] = 0 // when generate a new leader, nextIndex[] and matchIndex[] should be reset
                    }
                    rf.matchIndex[rf.me] = rf.LastLog().Index
                    rf.StateTransfer(Leader)
                    // fmt.Printf("刚当上leader, rf.me = %d (leaderid), 这是在peer %d, 时间戳（毫秒）: %v\n", rf.me, peer, time.Now().UnixMilli())
                    rf.Heartbeat()
                    rf.heartbeatTimer.Reset(time.Duration(HeartbeatTimeout)) // if not reset heartbeatTimer, leader can't sent heartbeat to follower after this heartbeat, and then follower will start new election, that's not correct
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
	rf.applyCond.L.Lock()
	defer rf.applyCond.L.Unlock()
	for {
		for !rf.NeedApply() {
			rf.applyCond.Wait()
            // fmt.Printf("It's in DoApply, need apply\n")
			if rf.killed() {
				return
			}
		}

		rf.mu.Lock()
		// if !rf.NeedApplyL() {
		// 	rf.mu.Unlock()
		// 	continue
		// }
		rf.lastApplied += 1
		entry := rf.LogAtIndex(rf.lastApplied)
		// toCommit := *entry
		// rf.Debug(dCommit, "apply rf[%d]=%+v", rf.lastApplied, toCommit)
		rf.mu.Unlock()

		rf.applyCh <- ApplyMsg{
			Command:      entry.Command,
			CommandValid: true,
			CommandIndex: entry.Index,
		}
    }
}

func (rf *Raft) NeedApply() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.NeedApplyWithOutLock()
}

func (rf *Raft) NeedApplyWithOutLock() bool {
	return rf.commitIndex > rf.lastApplied
}

func (rf *Raft) NeedReplicate(peer int) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.state == Leader && peer != rf.me && rf.LastLog().Index > rf.matchIndex[peer]
}

// Replicate must be called W/O rf.mu held.
func (rf *Raft) Replicate(peer int) {
	rf.mu.Lock()
    // fmt.Printf("rf.me = %d, It's in replicate, 时间戳（毫秒）: %v\n\n\n\n", rf.me, time.Now().UnixMilli())
	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}
	if rf.nextIndex[peer] <= rf.log[0].Index { // only sanpshot can catch up, rf.nextIndex[peer] <= already existed snapshot's lastIncludedIndex, so need to sent snapshot to peer
        args := &InstallSnapshotArgs{
            Term:               rf.currentTerm,
            LeaderId:           rf.me,
            LastIncludedIndex:  rf.log[0].Index,
            LastIncludedTerm:   rf.log[0].Term,
            Data:               rf.persister.ReadSnapshot(),
        }
        reply := new(InstallSnapshotReply)
        rf.mu.Unlock()

        if !rf.sendInstallSnapshot(peer, args, reply) { // can't use rpc with lock, because it will cause dead lock
            return 
        }
        rf.mu.Lock()

        if rf.currentTerm != args.Term {
            rf.mu.Unlock()
            return
        }

        if reply.Term > rf.currentTerm {
            rf.StateTransfer(Follower)
            rf.ResetTerm(reply.Term)
            rf.mu.Unlock()
            fmt.Printf("It's in Replicate, reply.Term > rf.term\n\n\n\n\n")
            return
        }

        rf.nextIndex[peer] = args.LastIncludedIndex + 1
        // rf.matchIndex[peer] = args.LastIncludedIndex
        rf.mu.Unlock()
        return
	}

	var entries []*LogEntry
	nextIndex := rf.nextIndex[peer]
	for j := nextIndex; j <= rf.LastLog().Index; j++ {
		atIndex := rf.LogAtIndex(j)
		if atIndex == nil {
            fmt.Println("rf.LogAtIndex(j) is nil")
            runtime.Goexit()
			panic(rf.Sdebug(dFatal, "atIndex == nil  %s", rf.FormatState()))
		}
		entry := *atIndex
		entries = append(entries, &entry)
	}
	prev := rf.LogAtIndex(nextIndex - 1)
	rf.Debug(dWarn, "replicate S%d nextIndex=%v matchIndex=%v prevLog: %v", peer, rf.nextIndex, rf.matchIndex, prev)
	args := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prev.Index,
		PrevLogTerm:  prev.Term,
		LeaderCommit: rf.commitIndex,
		Entries:      entries,
	}
	// rf.Debug(dReplicate, "replication triggered for S%d with args %+v", peer, args)
    rf.mu.Unlock()
    reply := new(AppendEntriesReply)
	if rf.sendAppendEntries(peer, args, reply) {
        rf.mu.Lock()
        rf.HandleReplyWithOutLock(peer, args, reply)
	    rf.mu.Unlock()
		return
	}
}

// Sync must be called W/O rf.mu held.
func (rf *Raft) HandleReplyWithOutLock(peer int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if rf.currentTerm != args.Term {
		return
	}
    if reply.Success {
        if len(args.Entries) == 0 { // heartbeat, so leader didn't send any logentry to follower, so reply doesn't contain ConflictiingTerm and XIndex 
            return
        }
        LastLogAtIndex := LastLog(args.Entries).Index
        rf.matchIndex[peer] = LastLogAtIndex // can't set rf.matchIndex[peer] to be len(rf.log), because rf.log may changed when sent appendentries rpc 
        rf.nextIndex[peer] = LastLogAtIndex + 1

        // update commitIndex
        preCommitIndex := rf.commitIndex
        for i := rf.commitIndex + 1; i <= LastLogAtIndex; i++ {
            count := 0
            for p := range rf.peers {
                if rf.matchIndex[p] >= i {
                    count += 1
                }
            }
            if count >= rf.Majority() && rf.LogAtIndex(i).Term == rf.currentTerm { // rf.LogAtIndex(i).Term maybe less than rf.Term, in this case, we can't commit these LogEntries, we just can commit LogEntries which's Term equal to rf.Term, i.e currentTerm
                preCommitIndex = i
            }
        }
        rf.commitIndex = preCommitIndex

        // trigger DoApply
        if rf.commitIndex > rf.lastApplied {
            // rf.applyCond.Broadcast()
            rf.applyCond.Signal()
        }
    } else { // appendentries failed, maybe reply.Term < rf.Term or conflict logentries exist
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

        if reply.XTerm > 0 {
            for i := len(rf.log) - 1; i >= 1; i-- {
                if rf.log[i].Term == reply.XTerm {
                    rf.nextIndex[peer] = Min(nextIndex, rf.log[i].Index+1)
                    return
                }
            }
        }
        rf.nextIndex[peer] = Max(Min(nextIndex, reply.XIndex), 1)
    }
}

// actually replicatorCond nerver be wakeup, so DoReplicate are blocked all the time
func (rf *Raft) Replicator(peer int) {
	rf.replicatorCond[peer].L.Lock()
	defer rf.replicatorCond[peer].L.Unlock()
	for {
		// rf.Debug(dWarn, "Replicator: try to acquire replicatorCond[%d].L", peer)
		// rf.Debug(dWarn, "Replicator: acquired replicatorCond[%d].L", peer)
		for !rf.NeedReplicate(peer) {
			rf.replicatorCond[peer].Wait()
			if rf.killed() {
				return
			}
            // fmt.Printf("replicatorCond 被唤醒了")
		}

		rf.Replicate(peer)
	}
}

func (rf *Raft) Heartbeat() {
    // fmt.Printf("It's in Heartbeat, rf.me = %d (leaderid), 时间戳（毫秒）: %v\n", rf.me, time.Now().UnixMilli())
	for peer := range rf.peers {
		if peer != rf.me {
		    go rf.Replicate(peer)
		}
	}
}

func (rf *Raft) ticker() {
    for rf.killed() == false {
        select {
        case <- rf.electionTimer.C:
            
            rf.mu.Lock()
            if rf.state != Leader { // if rf.state is leader, then rf shouldn't satrt election, only older leader failed can start a new election
                rf.StateTransfer(Candidate)
                rf.currentTerm += 1
                rf.StartElection()
            }
            rf.electionTimer.Reset(ElectionTimeout())
            rf.mu.Unlock()
        case <- rf.heartbeatTimer.C:
            // fmt.Printf("It's rf.heartbeatTimer.C without holding lock, rf.me = %d\n\n", rf.me)
            rf.mu.Lock()
            // fmt.Printf("It's rf.heartbeatTimer.C with holding lock, rf.me = %d\n\n", rf.me)
            if rf.state == Leader {
                // fmt.Printf("It's rf.heartbeatTimer.C, rf.me = %d, 时间戳（毫秒）: %v\n\n", rf.me, time.Now().UnixMilli())
                rf.Heartbeat()
                rf.heartbeatTimer.Reset(time.Duration(HeartbeatTimeout))
            }
                // rf.heartbeatTimer.Reset(time.Duration(HeartbeatTimeout))
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
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
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

	rf.readPersist(persister.ReadRaftState())


	return rf
}
