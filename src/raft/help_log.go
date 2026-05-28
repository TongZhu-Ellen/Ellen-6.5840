package raft  




func (rf *Raft) append(entry Entry) {
	rf.log = append(rf.log, entry)
	rf.bEffortKick()
	rf.persist()
}



func (rf *Raft) get(i int) Entry {
	return rf.log[i]
}

// 并非直接引用！！！
// 这里直接引用，因为是本机上 --- 会直接跑出data-race！
func (rf *Raft) entriesFrom(start int) []Entry {
	return append([]Entry{}, rf.log[start:]...)
}

func (rf *Raft) set(i int, entry Entry) {
	rf.log[i] = entry
	rf.persist()
}