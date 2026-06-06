package kvraft

import "6.5840/labrpc"
import "crypto/rand"
import "math/big"
import "time"




// seqNum倒是不用并发保护毕竟RPC是串行打的... 

type Clerk struct {
	servers []*labrpc.ClientEnd
	clientId int64  // MakeClerk 时 nrand() 生成，终身不变
    seqNum   int64  // 从 1 开始，成功后 +1 // actually it's "next avail seqNum"

	leader int
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
	ck.clientId = nrand()
    ck.seqNum = 1
	return ck
}

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
func (ck *Clerk) Get(key string) string {
    for {
        args := &GetArgs{
            Key:      key,
            ClientId: ck.clientId,
            SeqNum:   ck.seqNum,
        }
        reply := &GetReply{}
        ok := ck.servers[ck.leader].Call("KVServer.Get", args, reply)

        if !ok || reply.Err == ErrWrongLeader {
            ck.leader = (ck.leader + 1) % len(ck.servers)
            time.Sleep(10 * time.Millisecond)
            continue
        }

        ck.seqNum++
        switch reply.Err {
        case ErrNoKey:
            return ""
        case OK:
            return reply.Value
        }
    }
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	for {
		args := &PutAppendArgs{
			Key:      key,
			Value:    value,
			Op:       op,
			ClientId: ck.clientId,
			SeqNum:   ck.seqNum,
		}
		reply := &PutAppendReply{}
		ok := ck.servers[ck.leader].Call("KVServer.PutAppend", args, reply)

		if !ok || reply.Err == ErrWrongLeader {
			ck.leader = (ck.leader + 1) % len(ck.servers)
			time.Sleep(10 * time.Millisecond)
			continue
		}

		if reply.Err == OK {
			ck.seqNum++
			return
		}
	}
}


func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
