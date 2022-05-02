package kvraft

import (
	"crypto/rand"
	"math/big"
	"sync/atomic"

	"6.824/labrpc"
)


type Clerk struct {
	servers []*labrpc.ClientEnd
    clientid    int64
    leaderid    int64 // store leader avoid taking much time to find leader every sent rpc
	// You will have to modify this struct.
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
    ck.clientid = nrand()
    ck.leaderid = 0
	// You'll have to add code here.
	return ck
}

func (ck *Clerk) args() Args {
    return Args{ClientId: ck.clientid, RequestId: nrand()}
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

	// You will have to modify this function.
    args := GetArgs{Key: key}
    leaderid := atomic.LoadInt64(&ck.leaderid)
    reply := new(GetReply)
    for { // keep cycling util succeed
        ok := ck.servers[leaderid].Call("KVServer.Get", &args, reply)
        if ok {
            if reply.Err == OK {
                return reply.Value
            } else if reply.Err == ErrTimeout { // ck.leaderid is real leader, but leader maybe delay apply command to server because of bad network, so ck needs to re-send rpc to ck.leader
                continue
            }
        }
        leaderid = ck.nextleaderid(leaderid)
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
	// You will have to modify this function.
    args := PutAppendArgs{Key: key, Value: value, Args: ck.args()}
    if op == "Put" {
        args.Type = PutOp
    } else {
        args.Type = AppendOp
    }

    leaderid := atomic.LoadInt64(&ck.leaderid)
    reply := new(PutAppendReply)
    for {
        ok := ck.servers[leaderid].Call("KVServer.PutAppend", &args, reply)
        if ok {
            if reply.Err == OK {
                return
            } else if reply.Err == ErrTimeout {
                continue
            }
        }
        leaderid = ck.nextleaderid(leaderid)
    }
    
}

func (ck *Clerk) nextleaderid(currentleaderid int64) int64 {
    next := (currentleaderid + 1) % int64(len(ck.servers))
    atomic.StoreInt64(&ck.leaderid, next)
    return next
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
