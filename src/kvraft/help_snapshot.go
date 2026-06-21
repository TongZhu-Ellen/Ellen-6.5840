package kvraft


import (
    "bytes"
    "6.5840/labgob"
)

// 编码：把 kvMap 和 lastSeq 打包成 []byte
func (kv *KVServer) encodeSnapshot() []byte {
    w := new(bytes.Buffer)
    e := labgob.NewEncoder(w)
    e.Encode(kv.kvMap)
    e.Encode(kv.lastSeq)
   
    return w.Bytes()
}

// 解码：从 []byte 恢复 kvMap 和 lastSeq
func (kv *KVServer) decodeSnapshot(data []byte) {
    if data == nil || len(data) == 0 {
        return
    }
    
    r := bytes.NewBuffer(data)
    d := labgob.NewDecoder(r)

    var kvMap map[string]string
    var lastSeq map[int64]int64
   

    if d.Decode(&kvMap) != nil ||
       d.Decode(&lastSeq) != nil {
        return
    }

    kv.kvMap = kvMap
    kv.lastSeq = lastSeq
    
}