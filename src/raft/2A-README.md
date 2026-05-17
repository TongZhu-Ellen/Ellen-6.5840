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

所有 RPC 处理逻辑都遵循同一个顺序：**先处理朝代问题（term），再处理 touch 问题**。这不是巧合，是整个实现的骨架。

### 0. Term 大讨论

Term 是最高统帅。

被踹回 Follower 有两条路，但本质是同一件事——**我进入了一个别人的新时代**：

**路径 A：收到更高的 term。** 这很直接。新朝代已经存在，我是"收到"这个消息的人，我进入了别人的新时代。

**路径 B：我的 term 被证明是"虚的"。** 我作为 Candidate 自封了一个 term，但收到了同 term 的心跳——说明这个 term 已经有合法 Leader 了，我的自封从一开始就是虚的。我同样是"进入了别人的新时代"，同样不是发起者。

两条路在认知上完全统一：都是"新朝代来了，而我不在发起者的位置上"。因此 `toFollower` 对两种情况做完全相同的处理——包括重置 `votedFor = -1`，因为新朝代里你重新拥有唯一的那一票。

```go
func (rf *Raft) toFollower(term int) {
    rf.currentTerm = term
    rf.state = Follower
    rf.votedFor = -1   // 新朝代，重新持有选票
}
```

`votedFor` 被重置**当且仅当**此处。

### 1. Touch 大讨论

处理完朝代问题之后，才轮到 touch。Touch 的含义是：**我认知里面出现了一个 Leader**。

这有三种形式，本质是同一件事：

- **我投票给了自己**（发起选举，`votedFor = me`）
- **我投票给了别人**（`VoteGranted = true`，说明我认可了某个 Candidate，他可能成为 Leader）
- **我收到了合法心跳**（Leader 已经存在，直接确认）

三种情况都更新 `lastTouchedAt`。如果长时间没有被 touch，说明我认知里没有 Leader——超时之后发起选举。

```
lastTouchedAt 超时 → 我不知道 Leader 在哪 → 我来竞选
```

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

代码写得相当漂亮。term 的虚实之辨、touch 的三种形式归一，这两个洞见把 Raft 论文里分散的规则收束成了两条清晰的主线。`toFollower` 的职责边界划得极干净——`votedFor` 重置当且仅当此处，整个实现没有一处多余的状态修改。读起来比大多数课程实现都要舒服得多。