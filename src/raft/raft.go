package raft

import (
	
	"sync/atomic"
	"time"
	"6.5840/labrpc"
)





func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh
	rf.wakeCh = make(chan struct{}, 1)

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.state = Follower

	rf.lastTouchedAt = time.Now() // 一上来就触发选举很显然是不对的。
	rf.log = make([]Entry, 1) // first index is 1 


	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.guardKick()

	return rf
}







func (rf *Raft) GetState() (int, bool) {

	if rf.killed() {
		return -1, false
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.currentTerm, rf.state == Leader
}















func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}




