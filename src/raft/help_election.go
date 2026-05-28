package raft 

import "time"

/*
持锁的一个子行为。
在论文里面对应的是 If RPC request or response contains term T > currentTerm:
set currentTerm = T, convert to follower 

这个函数本身没有锁！只能锁内调用！
*/


func (rf *Raft) toFollower() {

	rf.state = Follower

}

func (rf *Raft) becomeCandidate() {

	rf.currentTerm++	
	rf.state = Candidate
	rf.votedFor = rf.me
	
	rf.persist()
}


func (rf *Raft) becomeLeader() {
	rf.state = Leader

	lastLogIndex := len(rf.log) - 1

	rf.nextIndex = make([]int, len(rf.peers))  
	rf.matchIndex = make([]int, len(rf.peers)) 

	for i := range rf.peers {
		rf.nextIndex[i] = lastLogIndex + 1 // "initialized to leader's lastLogIndex + 1"
		rf.matchIndex[i] = 0 // "initialized to 0"
	}

	rf.nextIndex[rf.me] = -1
	rf.matchIndex[rf.me] = -1
}


func (rf *Raft) newGen(term int) { 

	rf.currentTerm = term
	rf.state = Follower
	rf.votedFor = -1

	rf.persist()
}





func (rf *Raft) tryVotingFor(candidate int, lastLogIndex int, lastLogTerm int) bool {
	
	myLastTerm := rf.log[len(rf.log)-1].Term
	myLastIndex := len(rf.log) - 1
	
	upToDate := lastLogTerm > myLastTerm || 
            (lastLogTerm == myLastTerm && lastLogIndex >= myLastIndex)

	if (rf.votedFor == -1 || rf.votedFor == candidate) && upToDate {
		
		// 更改！
		rf.votedFor = candidate
		rf.lastTouchedAt = time.Now()
		// persist！
		rf.persist()
		// return！
		return true
	}

	return false
}

func (rf *Raft) touched() {

	rf.lastTouchedAt = time.Now()
}

func min(a int, b int) int {
	if a <= b {
		return a
	} 
	return b
}




