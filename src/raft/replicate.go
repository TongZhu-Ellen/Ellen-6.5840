package raft 

import "time"



type AppendEntriesArgs struct {

	Term int // leader's term
	LeaderId int

	// 2B:
	PrevLogIndex int // 上次的最后一条，
	PrevLogTerm int 
	Entries []Entry
	LeaderCommit int

}

// example AppendEntries RPC reply structure.
// field names must start with capital letters!
type AppendEntriesReply struct {
	
	Term int // my term / follower's term
	
	// 2B:
	Success bool
	ConflictStart int


}

// helper func; can only be called by leader!
func (rf *Raft) replicator(i int) {


	for {

		if _, leads := rf.GetState(); !leads {
			return 
		}

		retry := true

		for retry {
		    retry = rf.singleAppend(i)
		}

		time.Sleep(HEATBEAT_INTERVAL)
	}
	

}

func (rf *Raft) singleAppend(i int) (retry bool) {
	
	rf.mu.Lock() // ----------- 锁 --------------
    prevLogIndex := rf.nextIndex[i] - 1
    args := &AppendEntriesArgs{
		Term: rf.currentTerm,
		LeaderId: rf.me,
		
		PrevLogIndex: prevLogIndex,
		PrevLogTerm: rf.get(prevLogIndex).Term,
		Entries: rf.entriesFrom(prevLogIndex+1), 
		LeaderCommit: rf.commitIndex,
	}
    reply := &AppendEntriesReply{}
    rf.mu.Unlock() // ----------- 锁 --------------

    ok := rf.sendAppendEntries(i, args, reply) 

    // ----------- Server 处理中！ --------------

	if !ok { // 这是没发出去...  
		return true
	}


	rf.mu.Lock() // ----------- 锁 --------------
    defer rf.mu.Unlock()

    // 更改自身term的逻辑永远先行！
    if reply.Term > rf.currentTerm { 
		rf.newGen(reply.Term)
		return false
	}

	// 朝代已然改变！
    if rf.currentTerm != args.Term || rf.state != Leader {
		return false
	}


	// "If AppendEntries fails because of log inconsistency: decrement nextIndex and retry"
    // "If successful: update nextIndex and matchIndex for follower"
    if !reply.Success {
		rf.nextIndex[i] = reply.ConflictStart
		return true
    }


	rf.matchIndex[i] = prevLogIndex + len(args.Entries)
	rf.nextIndex[i]  = rf.matchIndex[i] + 1
	rf.updateCommitIndex()
	return false
}

 

func (rf *Raft) sendAppendEntries(i int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[i].Call("Raft.AppendEntries", args, reply)
	return ok
}


























// example AppendEntries RPC handler.
// 这是follower方的处理函数！
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).


	rf.mu.Lock()
	defer rf.mu.Unlock()

	//  --------------  全局条 --------------
	if args.Term > rf.currentTerm { 
		// "If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower"
		rf.newGen(args.Term)
	} else if rf.state == Candidate && args.Term == rf.currentTerm {
		// "If AppendEntries RPC received from new leader: convert to follower"
		rf.toFollower()
	}

	

	//  ---------------- 专属条们 -----------------

	// 1. "Reply false if term < currentTerm"
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return 
	}

	// 2. "Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm"
    if args.PrevLogIndex >= len(rf.log) ||
	rf.log[args.PrevLogIndex].Term != args.PrevLogTerm  {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}



	// 3. "If an existing entry conflicts with a new one (same index but different terms), 
	// delete the existing entry and all that follow it"
	myIdx := args.PrevLogIndex + 1
	yourIdx := 0 

	for myIdx < len(rf.log) && yourIdx < len(args.Entries) {
		if rf.log[myIdx].Term != args.Entries[yourIdx].Term {
			rf.log = rf.log[ : myIdx] // 连同这个也不要了。
			break
		} 
		myIdx++
		yourIdx++
	}
	

	// 4. "Append any new entries not already in the log"
	for yourIdx < len(args.Entries) {
		rf.append(args.Entries[yourIdx])
		myIdx++
		yourIdx++
	}

	// 5. "If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)"
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, len(rf.log) - 1)

		if rf.commitIndex > rf.lastApplied {
			rf.bEffortKick()
		}

	}


	

	
    rf.touched()

	reply.Term = rf.currentTerm
	reply.Success = true

	

} 

