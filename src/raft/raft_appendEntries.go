package raft 



type AppendEntriesArgs struct {

	Term int // leader's term
	LeaderId int
}

// example AppendEntries RPC reply structure.
// field names must start with capital letters!
type AppendEntriesReply struct {
	
	Term int // my term / follower's term

}

// helper func; can only be called by leader!
func (rf *Raft)  appendYourEntries() {
	
	for i := 0; i < len(rf.peers); i++ {

		if i == rf.me {
			continue
		}

		go func(server int) {
			rf.mu.Lock() // ----------- 锁 --------------
			args := &AppendEntriesArgs{
				Term: rf.currentTerm,
				LeaderId: rf.me,
			}
			reply := &AppendEntriesReply{}
			rf.mu.Unlock() // ----------- 锁 --------------

			ok := rf.sendAppendEntries(server, args, reply) 


			// servers处理中！

			if !ok {
				return 
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			if reply.Term > rf.currentTerm {
				rf.turnPage(reply.Term)
			}



		}(i)

	}


}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// example AppendEntries RPC handler.
// 这是follower方的处理函数！
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).


	rf.mu.Lock()
	defer rf.mu.Unlock()

	oldTerm := rf.currentTerm

	if args.Term > rf.currentTerm { 
		rf.turnPage(args.Term)
	} else if rf.state == Candidate && args.Term == rf.currentTerm {
		rf.ripPage()
	}


	if args.Term >= oldTerm {
		// this is valid touch!
		rf.touched()

		// TODO: 2B
	}

	


	reply.Term = rf.currentTerm
	

} 

