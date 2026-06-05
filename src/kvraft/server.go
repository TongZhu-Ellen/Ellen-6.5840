package kvraft

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type OpType string
const (
    OpGet    OpType = "Get"
    OpPut    OpType = "Put"
    OpAppend OpType = "Append"
)

type Op struct {
	
	Type OpType
	Key string 
	Value string	
}

type result struct {
	value string
	err Err
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft

	applyCh  chan raft.ApplyMsg
	chanMap  map[int]chan result
	kvMap    map[string]string 


	dead    int32 // set by Kill()
	maxraftstate int // snapshot if log grows this big

	
}


func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{}) // 看这里！

	kv := new(KVServer)
	kv.me = me
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.chanMap = make(map[int]chan result)
	kv.kvMap = make(map[string]string)

	kv.maxraftstate = maxraftstate

	go kv.guardApply()

	
	return kv
}

func (kv *KVServer) guardApply() {
    for msg := range kv.applyCh {
		res := result{}
        op := msg.Command.(Op)
		kv.mu.Lock()
	    switch op.Type {
		case OpGet:
			val, ok := kv.kvMap[op.Key]
			if !ok { 
				res.err = ErrNoKey
			} else { 
				res.err = OK
				res.value = val
			}
		case OpPut:
			kv.kvMap[op.Key] = op.Value
		case OpAppend:
			kv.kvMap[op.Key] += op.Value
	    }

		waitChan, ok := kv.chanMap[msg.CommandIndex]
		kv.mu.Unlock()

		if ok {
			select {
			case waitChan <- res:
			default: // 沉默忽略
			}
			
		}

		
	}
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {

	op := Op{
        Type: OpGet,
        Key: args.Key,
    } // 这里不能传pointer进去！

	idx, _, ok := kv.rf.Start(op)
	if !ok {
		reply.Err = ErrWrongLeader
		return
	}

	waitChan := make(chan result, 1)

	kv.mu.Lock()
	kv.chanMap[idx] = waitChan
	kv.mu.Unlock()

	defer func() {
		kv.mu.Lock()
		delete(kv.chanMap, idx)
		kv.mu.Unlock()
	}()

	select {
	case res := <-waitChan:
		reply.Err = res.err
		reply.Value = res.value



	case <-time.After(500 * time.Millisecond):
		reply.Err = ErrWrongLeader
	
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	op := Op{
        Type: OpType(args.Op),
        Key: args.Key,
		Value: args.Value,
    }

	idx, _, ok := kv.rf.Start(op)
	if !ok {
		reply.Err = ErrWrongLeader
		return
	}

	waitChan := make(chan result, 1)

	kv.mu.Lock()
	kv.chanMap[idx] = waitChan
	kv.mu.Unlock()

	defer func() {
		kv.mu.Lock()
		delete(kv.chanMap, idx)
		kv.mu.Unlock()
	}()

	select {
	case <-waitChan:
		reply.Err = OK

	case <-time.After(500 * time.Millisecond):
		reply.Err = ErrWrongLeader
	}

}































// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}


