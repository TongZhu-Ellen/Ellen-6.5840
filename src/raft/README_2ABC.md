# Raft 实现踩坑记录

凡是叫做 `help_xxx.go` 里面的函数 —— 都是本身不持有锁的。只是作为常用动作的组合，调用方自己负责加锁。

---

## 0. 我对这个 lab 的理解

Raft 想解决的问题说白了非常实际：**怎么用一群会掉线、会重启的普通机器，模拟出一台永远不会挂的机器？** 人类从来不缺硬件，缺的是稳定的物理环境——机器总会坏，机房总会断电，用数量来对冲个体的偶然性，是非常实在的技术。
 
但要解决这个问题，首先得定义清楚：这台"虚拟意义上永不掉线的机器"，什么时候算是真正接受了一个操作？这不是个工程问题，是个语义问题。Raft 的答案是：由 leader 把这个操作写进 log，复制到超过半数的机器。这个"过半"不是魔法，换成 2/3 也能自洽；"过半"只是作者的一个选择。

然后随之而来的工程问题是：这套规则必须在信息量有限的条件下跑起来。比如投票的时候不可能把两边的整个 log 都传过去比。所以论文定义了"at least as up-to-date"——用 `(lastLogTerm, lastLogIndex)` 这两个数来定义了什么叫”更新“。然后最重要的其实是：作者给出了证明（leader completeness），说明这个简化是正确的。

---

## 1. 玩锁自焚

### 1.1 GetState 死锁

`GetState` 是框架要调用的 public 函数，它自己会拿锁。当时觉得写了没用到有点亏，就顺手在 `ticker` 的 for-loop 里拿它来检查状态——但 `ticker` 本身已经持锁了，于是死锁，测试直接冻住，不报错，terminal里面不print任何东西，一动不动。

我当时直接愣在当场，问了 AI 才意识到这就是是死锁... 

fix：从此 `help_xxx.go` 里的所有函数一律不持锁，由调用方负责。这个现在是我的新习惯了。

### 1.2 中途开锁 = 开门更衣

当时想把 `becomeCandidate` 拆得"更 canonical"一点：`currentTerm++` 一个函数，`votedFor = me` 一个函数，各自持锁。感觉自己不要太canonical了（手动滑稽）。

结果两次调用之间有一个窗口期，锁是开着的。另一个 goroutine 趁这个空隙进来，把 `votedFor` 改掉了——于是跑出了一个 candidate 身份但票投给别人的偶发bug。 

教训：**锁的粒度是语义，不是操作。** `term++` 和 `votedFor = me` 是同一件事（成为 candidate），从外部看必须是原子的，中间不能有任何窗口暴露出去。把它们拆开加锁好比开着大门换衣服，完全没有”隐私“概念（完全没有锁的概念）。


---



## 2. leader ticker → replicator 模式

2C 测试有偶发 bug。最初用的是 leaderTicker 定期广播的模式。后来参考了 [这篇博客](https://www.inlighting.org/archives/mit-6-824-notes)，改成每个 follower 各有一个 `replicator` goroutine 的模式，bug 消失。

两者的核心区别：

- **leader ticker**：一个 goroutine 广播给所有 peer，多个 peer 的 append 在时间上并发，互相之间没有顺序保证；
- **replicator**：每个 peer 对应一个独立 goroutine，串行处理发往该 peer 的所有消息，失败就 retry，成功就 sleep。

教训：**尽量减少并发，尊重 canonical 的交互模式。** 选举天然是并发的（要同时问所有人），所以 `collectOpinion` 里 per-peer 起 goroutine 是对的。但同一个 peer 的 append 消息之间，没有必要并发，串行反而更简单、更正确。能串行的地方就不要并发，并发不是免费的。

我当时脑子里的想法是：就算当时改了 replicator 之后还有 bug，也不会改回去—— replicator 是更正确的模型。这不会是更错误的选择。

---

## 3. 2C == persist (term + vote + log)，以及为什么。
 
2C在我看来是纯体力活儿：弄清楚那些字段必须持久化，然后在任何改动它们的地方调 `persist()`

因为我自己改动这些参数绝大多数都是在help_xxx.go里面做的，比较集中，所以改起来也相对比较简单。

重点就一个：不要遗漏！

 
