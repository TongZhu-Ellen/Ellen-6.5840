package raft 

type RequestVoteArgs struct {
	// Your data here (2A, 2B).

	Term int // candidate's term!
	CandidateID int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).

	Term int
	VoteGranted bool

}


func (rf *Raft) election() {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.currentTerm++
	supporter := 1 
	rf.votedFor = rf.me // I support myself!
	rf.lastTouchedAt = time.Now()

	args := &RequestVoteArgs{
		Term: rf.currentTerm,
		CandidateID: rf.me,
	}

	for i := 0; i < len(rf.peers); i++ {

		if i == rf.me {
			continue // I voted for myself already! 
		}

		go func(server int) {

			reply := &RequestVoteReply{}
			ok := rf.sendRequestVote(server, args, reply)

			// 咳咳，此处其他人处理中... 

			if !ok {
				return // 该server 死了
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.state = Follower
				rf.votedFor = -1
				return // 后续不需要了你已经不是candidate了别操这心了！
			}

			if rf.state == Candidate && rf.currentTerm == args.Term && reply.VoteGranted {
				supporter++
				
				if supporter > len(rf.peers) / 2 {
					// 立刻当选！
					rf.state = Leader 
					go rf.leaderTicker() // 会立刻发心跳的。
				}
			}

			







		} (i)
	}




}




func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}



// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		// 那你还不如我呢！
		reply.Term = rf.currentTerm
		reply.VoteGranted = false 
		return 
	}

	if args.Term > rf.currentTerm {
		// 需要重新变回follower
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = -1 
	}

	reply.Term = rf.currentTerm

	reply.VoteGranted = rf.votedFor == -1 || rf.votedFor == args.CandidateID // TODO: 2B 需要加上比较Log的逻辑！

	if reply.VoteGranted {
		rf.votedFor = args.CandidateID
		rf.lastTouchedAt = time.Now()
	}




}

