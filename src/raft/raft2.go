package raft

import (
    "6.5840/labgob"
    "bytes"
)

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}


func (rf *Raft) persist() {
    w := new(bytes.Buffer)
    e := labgob.NewEncoder(w)
    e.Encode(rf.currentTerm)
    e.Encode(rf.votedFor)
    e.Encode(rf.log)
    rf.persister.Save(w.Bytes(), nil)
}

func (rf *Raft) readPersist(data []byte) {
    if data == nil || len(data) == 0 {
        return
    }
    r := bytes.NewBuffer(data)
    d := labgob.NewDecoder(r)
    var currentTerm int
    var votedFor int
    var log []Entry
    if d.Decode(&currentTerm) != nil ||
       d.Decode(&votedFor) != nil ||
       d.Decode(&log) != nil {
        panic("readPersist failed")
    }
    rf.currentTerm = currentTerm
    rf.votedFor = votedFor
    rf.log = log
}


// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}