# Raft 实现笔记

MIT 6.5840 Lab 2 (2A/2B) — Raft 共识算法实现。所有测试通过，包括 TestBackup2B。

---

## 设计哲学：忠实翻译论文，不自作聪明

把论文 Figure 2 当作规格说明书，一行一行翻译成 Go 代码，不做任何"创新"。坑全在细节里。

---

## 1. General Rules 的处理顺序

论文 §5.1 的全局规则（term 更新 + 退回 Follower）必须**最先处理**，后续各 RPC 的 specific 逻辑放在后面。

需要注意的是：先执行全局规则会影响后续判断所依赖的状态（比如 currentTerm），写 specific 逻辑时要把这一点考虑进去。

（吐槽：论文在这里写得相当不清楚，完全没有说明全局规则与各 RPC specific 规则之间的处理顺序关系，需要自己想。）

---

## 2. 死锁陷阱：helper 函数统一不加锁

实现中把一些核心状态变更封装成 helper 函数，比如 `turnPage()`、`ripPage()`、`tryVotingFor()` 等，集中管理状态变更逻辑，避免到处散写。

**这是个好习惯，但有一个必须遵守的约定：helper 函数里统一不加锁。**

原因很简单：helper 只在持锁的上下文里被调用。如果 helper 内部再去抢锁，就会死锁。

具体踩过的坑：`GetState()` 是一个对外暴露的函数，内部加了锁。`leaderTicker` 调用它来判断自己是否还是 leader：

```go
func (rf *Raft) leaderTicker() {
    for {
        if _, leads := rf.GetState(); !leads {  // GetState() 内部加锁
            return
        }
        rf.appendYourEntries()
        ...
    }
}
```

如果在持锁的情况下调用 `GetState()`，或者在 helper 里调用任何内部有锁的函数，必死锁。

**结论：helper 函数是"锁内专用工具"，进入 helper 之前你已经持锁，helper 里不再加锁。对外暴露的函数（如 GetState）有自己的锁，绝不在持锁上下文里调用它们。**

---

## 3. TestBackup2B 的通过：快速日志回退（Fast Backup）

这是论文里没有详细描述、但 Lab 强制要求的优化，也是 2B 里最复杂的部分。

### 问题背景

朴素实现里，AppendEntries 失败后，leader 每次把 `nextIndex[server]` 减 1 再重试。如果 follower 的日志落后很多，这个过程要来回几百次 RPC，TestBackup2B 会超时。

### 解决方案：让 follower 在拒绝时多说一点信息

Reply 结构里增加三个字段：

```go
XTerm  int  // 冲突位置的 term
XIndex int  // 该 term 第一条 log 的 index
XLen   int  // follower 日志长度（用于 prevLogIndex 超出范围的情况）
```

**Follower 拒绝时有两种情况：**

**情况 A：prevLogIndex 超出了 follower 的日志长度**

follower 根本没有那么长的日志，直接告诉 leader：我一共只有 XLen 条。

```go
reply.XTerm = -1
reply.XLen  = len(rf.log)
```

leader 收到后直接跳到：`nextIndex[server] = reply.XLen`，一步到位。

**情况 B：prevLogIndex 存在，但 term 对不上**

follower 找到冲突 term 的第一条 log 的 index，告诉 leader：

```go
reply.XTerm  = rf.log[args.PrevLogIndex].Term
reply.XIndex = // 该 term 第一条 log 的 index（往前找）
```

leader 收到后在自己的日志里找 XTerm：

- **找到了**：说明 leader 和 follower 在这个 term 上都有日志，冲突在这个 term 的结尾之后，从 `found + 1` 开始发。
- **没找到**：leader 根本没有这个 term，follower 这个 term 的所有日志都是错的，从 `XIndex` 开始覆盖。

这样，原本需要几百次 RPC 才能收敛的日志回退，最多几次就能定位到正确的 `nextIndex`，TestBackup2B 的时间限制就能满足。

---

## 4. 不要持锁往 channel 发消息

`applyTicker` 负责把已提交的日志应用到状态机，向 `applyCh` 发送 `ApplyMsg`。

**向 channel 发消息可能阻塞**（如果接收方还没准备好）。如果此时持着 `rf.mu`，整个 Raft 实例就会卡死——没有任何其他 goroutine 能拿到锁，包括可能在等待接收 `applyCh` 的上层代码，经典死锁。

解决方式：发消息前先解锁，发完再重新加锁：

```go
for rf.commitIndex > rf.lastApplied {
    rf.lastApplied++
    msg := ApplyMsg{ ... }
    rf.mu.Unlock()   // 先放锁
    rf.applyCh <- msg  // 再发消息
    rf.mu.Lock()     // 发完重新拿锁
}
```

这个模式略显笨拙，但逻辑清晰，不容易出错。