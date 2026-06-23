# Ellen-6.5840 — Raft 共识协议完整实现

基于 [MIT 6.5840 (2023)](https://pdos.csail.mit.edu/6.824/) 框架，从零实现完整的 Raft 共识协议及其上层线性一致分布式 KV 服务。

**Lab 2（Raft）+ Lab 3（kvraft）全部测试通过，含高频随机故障注入与网络分区场景。**

---

## 项目结构

```
raft/
  raft.go              # Make()、ticker()、GetState()
  types.go             # Raft struct、Entry、ApplyMsg
  election.go          # RequestVote RPC 及 handler
  replicate.go         # AppendEntries RPC 及 handler、replicator goroutine
  install.go           # InstallSnapshot RPC 及 handler
  log_commit.go        # Start()、guardKick()、bEffortKick()
  persister.go         # persist()、readPersist()、Snapshot()
  help_election.go     # becomeCandidate/Leader/newGen/tryVotingFor（不持锁）
  help_log.go          # get/append/entriesFrom/logLength（不持锁）
  help_replicate.go    # updateCommitIndex/stepBack/reconcileEntries（不持锁）
  constants.go         # 超时与心跳间隔

kvraft/
  server.go            # KVServer、guardApply()、Get/PutAppend handler
  client.go            # Clerk、Get/PutAppend
  help_snapshot.go     # encodeSnapshot/decodeSnapshot
  common.go            # RPC 结构体定义
```

---

## 设计决策与踩坑记录

### 1. 锁纪律：`help_*.go` 一律不持锁

所有 `help_*.go` 里的函数不持有锁，由调用方负责加锁保证原子性。这个约定来自一次真实的死锁：

`GetState()` 是 public 函数，内部会拿锁。早期实现里在 `ticker` 的循环中调用它检查状态——但 `ticker` 本身已经持锁，于是测试直接冻住，terminal 没有任何输出，一动不动。这是经典的同线程重入死锁。

更微妙的变体是"中途开锁"：早期把 `becomeCandidate` 拆成两步——`currentTerm++` 一个函数，`votedFor = me` 一个函数，各自持锁。两次调用之间有一个窗口期，另一个 goroutine 趁机修改了 `votedFor`，跑出了 candidate 身份但票投给别人的偶发 bug。

**教训：锁的粒度是语义，不是操作。** `term++` 和 `votedFor = me` 是同一件原子事务（成为 candidate），中间不能有任何窗口暴露出去。

---

### 2. replicator 模式替代 leaderTicker

早期用单一 `leaderTicker` goroutine 定期广播给所有 peer，在 2C 的持久化压力测试下出现偶发提交错误。

根本原因：单一 ticker 广播时，多个 peer 的 AppendEntries 在时间上并发，互相之间没有顺序保证，RPC 响应乱序回来时 `matchIndex` 的更新会产生竞争。

改成 **per-peer `replicator` goroutine** 后：每个 peer 对应一个独立 goroutine，串行处理发往该 peer 的所有消息，失败 retry，成功 sleep。同一 peer 的消息天然有序，bug 消失。

```go
func (rf *Raft) replicator(i int) {
    for !rf.killed() {
        retry := true
        for retry {
            retry = rf.singleAppend(i)
        }
        time.Sleep(HEATBEAT_INTERVAL)
    }
}
```

选举天然需要并发（要同时问所有人），所以 `collectOpinion` 里 per-peer 起 goroutine 是对的。但同一个 peer 的 AppendEntries 之间没有必要并发——**能串行的地方不要并发，并发不是免费的。**

---

### 3. Fast Backup：O(term数) 次 RPC 完成冲突回退

论文原文的 nextIndex 逐条 decrement 在网络不稳定场景下极慢。实现了 XTerm/XIndex/XLen 三字段的快速回退：

```
follower reply:
  XTerm  = 冲突位置的 term（-1 表示日志太短）
  XIndex = 该 term 第一条日志的 index
  XLen   = follower 日志长度（仅 XTerm==-1 时有效）
```

leader 收到后的 `stepBack` 逻辑：

- `XTerm == -1`：follower 日志太短，`nextIndex = min(XLen, logLength)`
- leader 日志中存在 XTerm：冲突在这个 term 内部，`nextIndex = lastIndexOfTerm(XTerm) + 1`
- leader 日志中不存在 XTerm：follower 该 term 的日志全部无效，`nextIndex = XIndex`

每个 term 最多一次 RPC 即可跳过，回退轮数从 O(log条目数) 降至 O(term数)。

---

### 4. apply 流水线：guardKick + wakeCh

apply 不在持锁路径上完成，而是通过一个容量为 1 的 `wakeCh` channel 触发独立的 `guardKick` goroutine：

```go
func (rf *Raft) bEffortKick() {
    select {
    case rf.wakeCh <- struct{}{}:
    default:  // 已有通知在途，不重复发
    }
}
```

`guardKick` 持锁批量收集 `[lastApplied+1, commitIndex]` 范围内的条目，释放锁后再逐条写入 `applyCh`。**持锁期间不阻塞在 channel 发送上**，避免了 applyCh 满时的死锁。

跳过 `i <= snapIndex` 的条目处理了 snapshot 和 apply 进度之间的重叠窗口。

---

### 5. 快照边界处理：AppendEntries 与 snapIndex 的交叉

当 leader 发来的 `PrevLogIndex` 落在 follower 的快照范围内时，一致性由快照本身保证，不需要再比对日志。实现上计算出 entries 中第一个不在快照内的位置，从那里开始 `reconcileEntries`：

```go
if args.PrevLogIndex <= rf.snapIndex {
    appendStart := rf.snapIndex - args.PrevLogIndex
    if appendStart >= len(args.Entries) {
        rf.tryUpdateCommit(args.LeaderCommit)
        return
    }
    rf.reconcileEntries(rf.snapIndex+1, appendStart, args.Entries)
    rf.tryUpdateCommit(args.LeaderCommit)
    return
}
```

`InstallSnapshot` handler 同样处理了幂等性：`args.LastIncludedIndex <= rf.snapIndex` 时直接返回，避免用旧快照覆盖更新的状态。收到快照时如果 `LastIncludedIndex` 落在现有日志范围内，保留后续条目而不是无脑清空。

---

### 6. kvraft：共识层与状态机完全解耦

`raft.Raft` 不知道 `Op` 里装的是什么，只负责让所有节点对 log 顺序达成一致。`kvMap` 和 `lastSeq` 完全本地、完全确定性，靠"喂进同一份顺序一致的 log"自动收敛。

---

### 7. 去重：O(n) → O(1)

每个 Clerk 持有全局唯一 `clientId`（启动时 `nrand()` 生成）和单调递增 `seqNum`（成功后才 +1）。

关键约束：**同一个 client 的 RPC 是串行打出去的**——下一个请求只可能在上一个成功之后发出。因此 server 端不需要记录"所有见过的请求"，只需要 per-client 记一个 `lastSeq`：

```go
if op.SeqNum <= kv.lastSeq[op.ClientId] {
    // 见过，跳过 apply，但仍然 notify
}
```

UUID 去重方案的去重表随时间 O(n) 增长；利用串行约束，把一个看起来需要历史记录的问题变成了不需要历史记录的问题。

---

### 8. apply 与 notify 是两个独立维度

这是实现中最容易犯的设计错误：把"要不要 apply"和"要不要 notify"绑在一起，写成"只有真的 apply 了才 notify"。

- **要不要 apply**：正确性问题，看 `seqNum` 是否见过。所有副本都得跑，跟本地有没有 handler 在等无关——follower 上 99% 的 apply 根本没人等，它也得照样执行。
- **要不要 notify**：纯本地调度问题，看 `chanMap` 里有没有人在等这个 key，跟这条 entry 是不是第一次出现无关。

两件事分开判断，重复请求也会 notify，handler 才能正确返回。leader 切换导致的 RPC 孤儿（500ms 超时）由 client 重试处理，不需要 server 端特殊逻辑。

handler 侧用 `chan struct{}` 传递纯信号，不在 channel 里携带 value。`Get` handler 被唤醒后重新加锁读 `kvMap`——如果把 value 塞进 channel 就得发明一个能同时装"成功/失败 + 可能有 value"的通用返回体，代码变丑，收益为零。

---

### 9. 线性一致性保证：一句话

**rpc1 已返回 ⟹ rpc2 的 log entry 只能排在它后面。**

rpc1 返回意味着对应 entry 已 commit；Leader Completeness Property 保证已提交的 entry 在之后所有 leader 的 log 里都存在；rpc2 在 rpc1 返回后才发出，无论落在哪个 leader，rpc1 的 entry 已经先一步占好位置。Q.E.D.

注意这里的保证是**实时顺序**，不是发出顺序——rpc1 先发出不代表 rpc1 先执行，这也不是线性一致性的要求。

---

## 测试覆盖

| Lab | 测试场景 |
|-----|---------|
| 2A  | 基本选举、网络分区后重新选举 |
| 2B  | 日志复制、并发 Start()、leader 崩溃恢复 |
| 2C  | 持久化、崩溃重启、Figure 8 不稳定网络 |
| 2D  | 日志压缩、InstallSnapshot、快照后追赶 |
| 3A  | 线性一致性、并发 clerk、leader 切换、网络分区 |
| 3B  | KV 层快照、日志大小上限、崩溃恢复后状态一致 |

全部通过，含高频随机故障注入（unreliable network、server crash/restart、network partition）。
