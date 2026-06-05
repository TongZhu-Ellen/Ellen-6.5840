package kvraft

import "6.5840/labrpc"
import "crypto/rand"
import "math/big"

import (
	"time"
)


type Clerk struct {
	servers []*labrpc.ClientEnd
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
	// You'll have to add code here.
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

		for i := range ck.servers {
			args := &GetArgs{
				Key: key,
			}
			reply := &GetReply{}
			ok := ck.servers[i].Call("KVServer.Get", args, reply)

			// ---------- server处理中 ------------

			if !ok || reply.Err == ErrWrongLeader {
				continue
			}

			if reply.Err == ErrNoKey {
				return ""
			}

			return reply.Value
		}
		

		// 都试过了没找到
		// 事已至此先睡觉
		time.Sleep(100 * time.Millisecond)
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

		for i := range ck.servers {
			args := &PutAppendArgs{
				Key: key,
				Value: value,
				Op: op,
			}
			reply := &PutAppendReply{}
			ok := ck.servers[i].Call("KVServer.PutAppend", args, reply)

			// ---------- server处理中 ------------

			if !ok || reply.Err == ErrWrongLeader {
				continue
			}

			if reply.Err == OK {
				return 
			}

			

			
		}
		

		// 都试过了没找到
		// 事已至此先睡觉
		time.Sleep(100 * time.Millisecond)
	}
}


func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
