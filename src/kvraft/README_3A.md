# 6.5840 (2023) Lab 3A —— kvraft 实现笔记



## 1. 全局共识层和本地状态机解耦！

`raft.Raft` 不知道、也不需要知道 `Op` 里装的是什么——它只负责一件事：让所有节点对一份顺序一致的 log 达成一致。`kvMap` 和 `lastSeq` 则相反：完全本地、完全确定性、每个 server 各跑一份，彼此之间不说一句话，靠"喂进同一份顺序一样的 log"就能让状态自动收敛。

State Machine Replication的模式我认为很妙。现实中一个共识算法和一个状态机，一边是raft本身的复杂性，另一边则是一整个游戏的参数系统之类的业务逻辑。不解耦简直没法弄... 



## 2. apply 循环和 RPC 应答的解耦

`guardApply()` 是负责apply的，而RPC handler 没有改动数据库的权限，它只能把 `Op` 丢进 log，然后在自己的 `chan struct{}`（key 是 `clientId@seqNum`）上等通知。

这个 channel 设计成空的，负责传递信号。`Get` 的RPC handler 被叫醒之后，需要自己重新加锁去读一遍 `kvMap`。设计的取舍是：这里如果硬要把 value 塞进 channel 里带出去，就得发明一个能同时装下"成功/失败 + 可能有 value"的通用返回体。我觉得是变丑了的。

是否要 apply：看 `clientId@seqNum` 是不是新的。是否要通知：看有没有人在 `chanMap` 里等这个 key。这两件事互不依赖。

"要不要 apply"和"要不要 notify"看着像一件事的两半，其实问的是完全不同维度的问题。前者是正确性问题：这条 log entry 有没有见过，所有副本都得算，跟本地有没有人等无关——follower 上 99% 的 apply 根本没人在等，它也得照样跑。后者是纯本地的、瞬时的调度问题：`chanMap` 里有没有人举手，跟这条 entry 是不是第一次出现无关。

如果用一种超级惯性的思维把这俩绑一起，写成"只有真的 apply 了才 notify" --- 我当时非常快就发现了一个大问题：leader半路挂掉之后这个rpc的返回要怎么办呢？？？



## 3. O(n) 到 O(1)的去重储存！

每个 Clerk 一个终身不变的 `clientId`，一个从 1 开始、成功才自增的 `seqNum`。关键的观察只有一句话：**同一个 client 的 RPC 是串行打出去的**——下一个请求，只可能在上一个成功之后才会被发出。

正因为这一点，server 端不需要记住"见过的每一个请求"，只需要给每个 `clientId` 记一个数：最后一个成功执行的 `seqNum`。

```
op.SeqNum <= kv.lastSeq[op.ClientId]  →  见过，跳过
```

如果换成给每个请求发一个独立 UUID 去重，去重表会随时间无限增长，是 `O(n)`。用上数据本身自带的结构，把一个看起来需要历史记录的问题，变成了不需要历史记录的问题。

## 4. 线性一致性：一句话证明

**rpc1 已经返回 ⟹ rpc2 提供的logEntry 只能排在它后面。**

理由：rpc1 返回意味着它对应的 entry 已经被 commit，而 Leader Completeness Property 保证：一条已提交的 entry，会在之后所有的 leader 的 log 里拥有一席之地；rpc2 是在 rpc1 返回之后才发起的，无论它最终落在哪个 leader 上，rpc1 的 entry 已经先一步占好了位置，rpc2 的 entry 只能排在后面。Q.E.D.

这里我没有说rpc1先发出则一定rpc1一定会先被执行——这也不是线性一致性的要求。
---

3B（snapshot）部分另起一节。
