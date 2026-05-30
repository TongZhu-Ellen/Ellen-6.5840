package raft  







func (rf *Raft) get(i int) Entry {
    return rf.log[i - rf.snapshotIndex]
}

func (rf *Raft) entriesFrom(start int) []Entry {
    return append([]Entry{}, rf.log[start - rf.snapshotIndex:]...)
}

func (rf *Raft) logLength() int {
    return len(rf.log) + rf.snapshotIndex
}
















func (rf *Raft) append(entry Entry) {

	rf.log = append(rf.log, entry)
	rf.bEffortKick()
	rf.persist()
}

func (rf *Raft) batchAppend(entries []Entry) {
    rf.log = append(rf.log, entries...)
    rf.bEffortKick()
    rf.persist()
}