package raft

import (
	"math/rand"
	"time"
)



func (rf *Raft) leaderTicker() {
    for {
    
        if _, leads := rf.GetState(); !leads {
			return 
		}
        
        rf.appendYourEntries()
        time.Sleep(HEATBEAT_INTERVAL)
    }
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (2A)
		// Check if a leader election should be started.

		rf.mu.Lock() // ------- 锁! -------
		if time.Since(rf.lastTouchedAt) > SELECTION_TIMEOUT && rf.state != Leader {

			
			rf.becomeCandidate()
			go rf.collectOpinion()
		}
		rf.mu.Unlock() // ------- 锁! -------


	
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)

		

	}
}





