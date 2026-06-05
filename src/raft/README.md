# Raft 实现踩坑记录

凡是叫做 `help_xxx.go` 里面的函数 —— 都是本身不持有锁的。只是作为常用动作的组合，调用方自己负责加锁。

---

## 1. 玩锁自焚

### 1.1 GetState 死锁

`GetState` 是 public 函数，框架会在外部调用它。一开始没注意，在持锁的地方内部又调用了它，结果死锁，测试一动不动。

排查方式：加 `DPrintf` 没用，因为根本跑不到那里。最后问 AI 才意识到是死锁。

fix：`help_xxx.go` 里的函数全部不持锁，调用方加锁；`GetState` 这类 public 函数自己加锁，内部绝不再调用任何会拿锁的东西。

### 1.2 中途开锁 = 开门 have sex

把 `becomeCandidate` 拆成两个函数分开管理（`currentTerm++` 和 `votedFor = me`），自以为很 canonical，实际上两次调用之间解锁了一下。

结果跑出了一个偶发 bug，极难复现。

教训：**锁的粒度不是"每个操作一把锁"，而是"一个原子语义单元一把锁"。** `term++` 和 `votedFor = me` 在语义上是同一件事（成为 candidate），中间不能有任何窗口暴露出去。对锁失去知觉，就好像开着门 have sex。

---

## 2. leader ticker → replicator 模式

2C 测试有偶发 bug。最初用的是 leader 在 ticker 里定期广播的模式（leader ticker）。后来参考了 [这篇博客](https://www.inlighting.org/archives/mit-6-824-notes)，改成每个 follower 各有一个 `replicator` goroutine 的模式，bug 消失。

两者的核心区别：

- **leader ticker**：一个 goroutine 广播给所有 peer，多个 peer 的 append 在时间上并发，互相之间没有顺序保证；
- **replicator**：每个 peer 对应一个独立 goroutine，串行处理发往该 peer 的所有消息，失败就 retry，成功就 sleep。

教训：**尽量减少并发，尊重 canonical 的交互模式。** 选举天然是并发的（要同时问所有人），所以 `collectOpinion` 里 per-peer 起 goroutine 是对的。但同一个 peer 的 append 消息之间，没有必要并发，串行反而更简单、更正确。能串行的地方就不要并发，并发不是免费的。

就算当时改了 replicator 之后还有 bug，也不会改回去——因为 replicator 是更正确的模型，bug 只会出在别处。

---

## 3. index 偏移：snapIndex 的心智模型

引入快照（2D）之后，log 的物理下标和逻辑下标不再一致。所有 `rf.log[i]` 都要换成 `rf.get(i)`，后者内部做 `i - rf.snapIndex` 的偏移。

容易犯的错：直接用物理下标切片，比如 `rf.log[i:]`，在有快照的情况下会越界或者语义错误。统一用 `entriesFrom`、`get`、`logLength` 这几个 helper，不要绕过它们直接操作 `rf.log`。

`rf.log[0]` 是哨兵位，存的是最后一条被 snapshot 的 entry 的 term（`snapTerm`），方便 `PrevLogTerm` 的查询跨过快照边界。

---

## 4. bEffortKick：apply 不要在锁内做

apply 到 `applyCh` 是阻塞操作（上层 KV 服务在读），如果在持锁的情况下直接 `rf.applyCh <- msg`，就会在锁内等待，死锁风险极高。

做法：用一个带缓冲的 `wakeCh`（容量 1）通知 `guardKick` goroutine，`guardKick` 在锁外做实际的 apply。`bEffortKick` 用 select + default 保证"已经有人通知过了就不重复通知"，不会阻塞。

```go
func (rf *Raft) bEffortKick() {
    select {
    case rf.wakeCh <- struct{}{}:
    default:
    }
}
```

这个模式的代价是 apply 可能有轻微延迟，但换来了锁内逻辑的简洁性，值得。

---

## 5. 朝代检查：RPC 回来之后状态可能已变

所有发 RPC 的地方，回来之后都要先检查：

1. `reply.Term > rf.currentTerm` → 立刻 `newGen`，放弃本次操作；
2. `rf.currentTerm != args.Term || rf.state != Leader`/ `rf.state != Candidate` → 朝代已换，也放弃。

这两个检查缺一不可。第一个处理"我已经过时了"，第二个处理"我虽然 term 没变，但角色变了（比如变成 follower 了）"。顺序也很重要：永远先处理 term 更新，再处理业务逻辑。