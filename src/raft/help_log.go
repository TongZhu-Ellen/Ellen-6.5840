package raft  




func (rf *Raft) append(entry Entry) {
	rf.log = append(rf.log, entry)
	rf.kick()
	rf.persist()
}



func (rf *Raft) get(i int) Entry {
	return rf.log[i]
}

func (rf *Raft) set(i int, entry Entry) {
	rf.log[i] = entry
	rf.persist()
}