package kvraft

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"log"
	"sync"
	"sync/atomic"
	"time"
	"fmt"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func helpKey(clientId, seqNum int64) string {
	return fmt.Sprintf("%d@%d", clientId, seqNum)
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

	ClientId int64 // 新增
	SeqNum   int64 // 新增
}



type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft

	applyCh  chan raft.ApplyMsg
	chanMap  map[string]chan struct{} // key == clientId@seqNum
	lastSeq  map[int64]int64 // clientId → 最后成功执行的 seqNum
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
	kv.chanMap = make(map[string]chan struct{})
	kv.lastSeq = make(map[int64]int64)
	kv.kvMap = make(map[string]string)

	kv.maxraftstate = maxraftstate

	go kv.guardApply()

	
	return kv
}









func (kv *KVServer) guardApply() {
    for msg := range kv.applyCh {
		
        op := msg.Command.(Op)
		key := helpKey(op.ClientId, op.SeqNum)
		kv.mu.Lock()
		if op.SeqNum <= kv.lastSeq[op.ClientId] {
			kv.helpNotice(key)
			continue
		}

		kv.lastSeq[op.ClientId] = op.SeqNum // 更新seqNum!
	    switch op.Type {
		case OpPut:
			kv.kvMap[op.Key] = op.Value
		case OpAppend:
			kv.kvMap[op.Key] += op.Value
	    }

		kv.helpNotice(key)
		
		

		
	}
}


func (kv *KVServer) helpNotice(key string) {
	
	waitChan, ok := kv.chanMap[key]
	kv.mu.Unlock()
	if ok {
        select {
        case waitChan <- struct{}{}:
        default:
        }
    }
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {

	op := Op{
        Type: OpGet,
        Key: args.Key,

		ClientId: args.ClientId,
		SeqNum:   args.SeqNum,
    } // 这里不能传pointer进去！
	key := helpKey(args.ClientId, args.SeqNum)

	waitChan := make(chan struct{}, 1)
	kv.mu.Lock()
	kv.chanMap[key] = waitChan
	kv.mu.Unlock()
	defer func() {
		kv.mu.Lock()
		delete(kv.chanMap, key)
		kv.mu.Unlock()
	}()
	

	_, _, ok := kv.rf.Start(op)
	if !ok {
		reply.Err = ErrWrongLeader
		return
	}


	select {
	case <-waitChan:
		kv.mu.Lock()
		val, ok := kv.kvMap[args.Key]
		kv.mu.Unlock()
		if !ok {
			reply.Err = ErrNoKey
			return
		}
		reply.Err = OK
		reply.Value = val

	case <-time.After(500 * time.Millisecond):
		reply.Err = ErrWrongLeader
	
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	op := Op{
        Type: OpType(args.Op),
        Key: args.Key,
		Value: args.Value,

		ClientId: args.ClientId, // 漏了
    	SeqNum:   args.SeqNum,   // 漏了
    }
	key := helpKey(args.ClientId, args.SeqNum)

	// 准备好waitChan以及释放！
	waitChan := make(chan struct{}, 1)
	kv.mu.Lock()
	kv.chanMap[key] = waitChan
	kv.mu.Unlock()
	defer func() {
		kv.mu.Lock()
		delete(kv.chanMap, key)
		kv.mu.Unlock()
	}()

    // 进入raft集群！
	_, _, ok := kv.rf.Start(op)
	if !ok {
		reply.Err = ErrWrongLeader
		return
	}

	// 得到结果或者放弃等待！
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


