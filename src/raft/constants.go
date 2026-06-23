package raft 

import "time"



const (
	SELECTION_TIMEOUT = 300 * time.Millisecond
	HEATBEAT_INTERVAL =  50 * time.Millisecond
	
)
















type RaftState int
const (
    Follower  RaftState = iota // 0
    Candidate                   // 1
    Leader                      // 2
)