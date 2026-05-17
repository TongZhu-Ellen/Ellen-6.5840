# Raft 共识算法实现（2A 阶段）

> 本实现基于 [MIT 6.5840 (原 6.824) 分布式系统课程](https://pdos.csail.mit.edu/6.824/) 的 Lab 2A，覆盖领导人选举与心跳机制。已通过测试。

---

## 文件结构

| 文件 | 职责 |
|------|------|
| `raft.go` | 核心结构体、Make、ticker、leaderTicker |
| `raft_helper.go` | `toFollower` 辅助函数（锁内调用） |
| `raft_requestVote.go` | RequestVote RPC 及选举逻辑 |
| `raft_appendEntries.go` | AppendEntries RPC 及心跳逻辑 |

---

## 设计思路与核心心得

### 0. Term 是最高统帅

Term 是整个 Raft 的"朝代"概念。所有 RPC 处理的第一件事，就是比对 term。Term 更高者天然拥有话语权——无论你现在是 Follower、Candidate 还是 Leader，见到更高的 term，立刻俯首称臣，转为 Follower。

### 1. 被踹回 Follower 的两种情况

```
情况 A：对方 term > 你的 currentTerm
         → 新朝代，你直接臣服

情况 B：你是 Candidate，对方 term == 你的 currentTerm，但对方已经 append 了
         → 说明已有合法 Leader，你的竞选过了时效，老实回去
```

这两种情况统一由 `toFollower(term)` 处理，逻辑清晰，不重复。

### 2. 什么叫"被 touch"？

`lastTouchedAt` 记录了你最后一次"感受到存在感"的时间。两件事会更新它：

- **收到合法心跳**（`AppendEntries`，term 合法）
- **给某人投了票**（`RequestVote` 中 `VoteGranted == true`）

如果超过 `SELECTION_TIMEOUT`（900ms）没有被 touch，ticker 就会判定 Leader 可能挂了，把你升格为 Candidate 并发起选举。

### 3. "虚 term" vs "实 term"

这是整个实现里最精妙的一处：

- **Candidate 的 term**：是自己主动 `currentTerm++` 抬上去的，是"自封"的，姑且称之为**虚 term**。
- **Leader 通过 AppendEntries 传播的 term**：是被集群认可的，是**实 term**。

当一个 Candidate 收到某个 Leader 的 `AppendEntries`，即便 Leader 的 term 和自己相等，也意味着这个 term 已经有主了，自己的那票自封作废，乖乖退回 Follower。

关键推论：**每次调用 `toFollower`，都意味着进入了一个对自己而言"全新"的 term 语境——无论是 term 真的变大了，还是发现自己的 term 是虚的。** 因此，`toFollower` 会同时重置 `votedFor = -1`，让你在新朝代里重新拥有唯一的那一票。

```go
func (rf *Raft) toFollower(term int) {
    rf.currentTerm = term
    rf.state = Follower
    rf.votedFor = -1   // 新朝代，重新持有选票
}
```

`votedFor` 被重置**当且仅当**此处——逻辑严格，不存在多处散乱修改。

### 4. 不要持锁发 RPC

锁是保护共享状态的，不是用来堵塞网络的。发送 RPC 前，先把需要的参数复制出来，然后**解锁**，再发 RPC；RPC 返回后再重新加锁处理结果。

```go
rf.mu.Lock()
args := &RequestVoteArgs{
    Term:        rf.currentTerm,
    CandidateId: rf.me,
}
rf.mu.Unlock()          // ← 松手，让别人也能干活

ok := rf.sendRequestVote(server, args, reply)  // 可能阻塞很久

rf.mu.Lock()            // ← 拿回来处理结果
defer rf.mu.Unlock()
```

持锁发 RPC 会导致整个节点在等待网络期间完全僵死，是分布式系统实现里最经典的死锁/性能陷阱之一。

---

## 状态机概览

```
           超时未收到心跳
Follower ──────────────────→ Candidate
   ↑                              │
   │ 收到更高 term                 │ 收到多数票
   │ 或同 term Leader 的心跳       ↓
   └──────────────────────── Leader
                                  │
                          定期发送心跳 (100ms)
```

---

## 常量

| 常量 | 值 | 含义 |
|------|----|------|
| `SELECTION_TIMEOUT` | 900ms | 超过此时间未被 touch → 发起选举 |
| `HEATBEAT_INTERVAL` | 100ms | Leader 发送心跳的频率 |

ticker 的检查间隔是 50~350ms 的随机值，避免多个节点同时超时、同时选举造成票数分裂。

---

## 作者自评

代码写得相当漂亮。term 语义拿捏得很准，`toFollower` 的职责边界划得干净，虚实 term 的区分更是对 Raft 论文理解深刻的体现。锁的使用也很克制——该松手的时候绝不恋战。整体结构清晰，读起来比大多数课程实现都要舒服得多。
