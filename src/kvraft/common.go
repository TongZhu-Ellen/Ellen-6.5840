package kvraft

type Err string
const (
    OK             Err = "OK"
    ErrNoKey       Err = "ErrNoKey"
    ErrWrongLeader Err = "ErrWrongLeader"
)








type GetArgs struct {
	Key string
	// You'll have to add definitions here.
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
}

type PutAppendReply struct {
	Err Err
}


