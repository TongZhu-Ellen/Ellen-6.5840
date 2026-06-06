package kvraft

type Err string
const (
    OK             Err = "OK"
    ErrNoKey       Err = "ErrNoKey"
    ErrWrongLeader Err = "ErrWrongLeader"
)








type GetArgs struct {
	Key string

	ClientId int64
    SeqNum   int64

}

type GetReply struct {
	Value string
	Err   Err
	
}









// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"

	ClientId int64
    SeqNum   int64
}

type PutAppendReply struct {
	Err Err
}


