package kvraft

import (
	"bytes"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

    kv      map[string]string
    dup     map[int64] interface{} // every request has a clientid and a requestid, if dup[args.clientid].requestid == args.requestid, then clientid has sent same request to kv several times in succssion, i.e request has repeated

    notify  map[int]chan string
    
    lastApplied int

	// Your definitions here.
}

func (kv *KVServer) readSnapshot(snapshot []byte) {
    var dup map[int64]interface{}
    var kvmap  map[string]string
    r := bytes.NewBuffer(snapshot)
    d := labgob.NewDecoder(r)
    if e := d.Decode(&dup); e != nil {
        dup = make(map[int64]interface{})
    }
    if e := d.Decode(&kvmap); e != nil {
        kvmap = make(map[string]string)
    }
    kv.dup = dup
    kv.kv = kvmap
}

func (kv *KVServer) DoApply() { // wait command to be committed and be executed
    for v := range kv.applyCh { // messages applyCh received come from two places:1、kv received operation from clerk, and kv invoke kv.rf.start(operation), so after kv.rf committed this operation. kv.rf will apply this operation(i.e send this operation to kv according to applyCh). 2、kv.rf received operation from other rafts, and apply operation to kv
        if kv.killed() { // kv maybe killed when wait data from applyCh
            return
        }

        if v.CommandValid {
            kv.apply(v) // executed commands if needs
            if _, isLeader := kv.rf.GetState(); !isLeader { // whether kv.rf is leader or not, kv.DoApply will be executed, but obly leader would handle clerk's request. If kv.rf isn't leader, command is just received from leader and apply to kv, so don't need to handle clerk's request
                continue
            }
            kv.mu.Lock()
            switch args := v.Command.(type) {
            case GetArgs:
                ch := kv.notify[v.CommandIndex]
                val := ""
                if s, ok := kv.kv[args.Key]; ok {
                    val = s
                }
                kv.mu.Unlock()
                go func() { // can't block reading from applyCh, so start a goroutine to send message. Note: any channel has a possibility to be blocked
                    ch <- val
                }()
                break
            case PutAppendArgs:
                ch := kv.notify[v.CommandIndex]
                kv.mu.Unlock()
                go func() {
                    ch <- "" // send nothing, just notify kv's handler that operation has applied, now handler can continue to handle next things
                }()
            }
        } else { // leader sent snapshot to kv.rf, and kv.rf sent this to kv, so kv should install this snapshot(change rf's log and persist this snapshot to persister), and read data from snapshot to memory
            if kv.rf.CondInstallSnapshot(v.SnapshotTerm, v.SnapshotIndex, v.Snapshot) {
                kv.lastApplied = v.SnapshotIndex
                kv.readSnapshot(v.Snapshot)
            }
            
        }
    }
}

func (kv *KVServer) apply(v raft.ApplyMsg) {
    if v.CommandIndex <= kv.lastApplied { // I don't understand
        return
    }
    repeat := false
    switch args := v.Command.(type) {
    case GetArgs:
        kv.lastApplied = v.CommandIndex
        break
    case PutAppendArgs:
        // judge request is repeated or not
        if dup, ok := kv.dup[args.ClientId]; ok {
            if putDup, ok := dup.(PutAppendArgs); ok && putDup.RequestId == args.RequestId { // args.ClientId sent the same request continuously because args.clientid received ErrTimeout before
                repeat = true
                break
            }
        }
        if args.Type == PutOp {
            kv.kv[args.Key] = args.Value
        } else {
            kv.kv[args.Key] += args.Value
        }
        kv.dup[args.ClientId] = v.Command
        kv.lastApplied = v.CommandIndex
    }

    if kv.rf.GetStateSize() >= kv.maxraftstate && kv.maxraftstate != -1 {
        w := new(bytes.Buffer)
        e := labgob.NewEncoder(w)
        if err := e.Encode(kv.dup); err != nil {
            panic(err)
        }
        if err := e.Encode(kv.kv); err != nil {
            panic(err)
        }
        if v.CommandIndex != kv.lastApplied {
            fmt.Printf("v.CommandIndex != kv.lastApplied, v.CommandIndex = %v, kv.lastApplied = %v, repeat = %v\n\n\n", v.CommandIndex, kv.lastApplied, repeat)
        }
        kv.rf.Snapshot(v.CommandIndex, w.Bytes()) //I don't understand, v.CommandIndex different with kv.lastApplied
    }
}

const TimeoutInterval = 500 * time.Millisecond

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) { // only operation is executed, can handler return
	// Your code here.
    if _,isLeader := kv.rf.GetState(); !isLeader {
        reply.Err = ErrWrongLeader
        return
    }
    op := *args
    beforestart := time.Now().UnixMilli()
    i, _, isLeader := kv.rf.Start(op) // Here is a trap, it maybe take much time for Start(op) to get kv.rf.mu.lock, so kv.rf's state maybe changed
    if !isLeader {
        afterstart := time.Now().UnixMilli()
        fmt.Printf("It's in Get, before Start(op):%+v    after Start(op):%+v\n\n\n", beforestart, afterstart)
        reply.Err = ErrWrongLeader
        return
    }
    ch := make(chan string, 1)
    kv.mu.Lock()
    kv.notify[i] = ch
    kv.mu.Unlock()
    select {
    case v := <- ch:
        reply.Value = v
        reply.Err = OK
        return
    case <- time.After(TimeoutInterval): // leader maybe delay sent operation to followers because of bad network
        reply.Err = ErrTimeout
        return
    }

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
    if _,isLeader := kv.rf.GetState(); !isLeader {
        reply.Err = ErrWrongLeader
        return
    }
    op := *args
    beforestart := time.Now().UnixMilli()
    i, _, isLeader := kv.rf.Start(op)
    if !isLeader {
        afterstart := time.Now().UnixMilli()
        fmt.Printf("It's in PutAppend, before Start(op):%+v    after Start(op):%+v\n\n\n", beforestart, afterstart)
        reply.Err = ErrWrongLeader
        return
    }
    ch := make(chan string, 1)
    kv.mu.Lock()
    kv.notify[i] = ch
    kv.mu.Unlock()
    select {
    case <- ch:
        reply.Err = OK
        return
    case <- time.After(TimeoutInterval): // leader maybe delay sent operation to followers because of bad network
        reply.Err = ErrTimeout
        return
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
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
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
	labgob.Register(GetArgs{})
    labgob.Register(PutAppendArgs{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
    kv.notify = make(map[int]chan string)
    kv.readSnapshot(persister.ReadSnapshot())
    go kv.DoApply() // execute operation which is sent to kv or other KVServers and sent to kv then

	// You may need initialization code here.

	return kv
}
