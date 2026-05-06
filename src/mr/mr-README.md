# MapReduce — 6.824 Lab 1

全过了。

## 设计亮点

### 函数抽象清晰，一眼就懂
 
整个控制流被切成了几个职责单一的函数，读起来几乎不需要注释：
 
- `Worker` — 主循环，只管"要不要退出、要不要等"
- `eachCall` — 单次 RPC 交互，拿任务、干活、汇报，逻辑完整自洽
- `mapper` / `reducer` — 纯粹的计算逻辑，不掺杂任何调度细节
- `fetchMap` / `fetchReduce` — coordinator 侧的任务调度，phase 推进也藏在这里，不外漏
coordinator 和 worker 之间的边界非常干净：worker 只通过 `RequestTask` 和 `ResponseTask` 两个 RPC 和外界交互，coordinator 内部状态完全封装。
 
### 矩阵建模，下标即语义
 
中间文件命名为 `mr-x-y`，x 是 map 任务编号，y 是 reduce 任务编号，构成一个逻辑上的 X×Y 矩阵。x 和 y **均从 1 开始**，这不是随意的选择——它让 0 成为一个干净的哨兵值：
 
```go
mapTask  = make([]*MapTask,    fileNum+1)  // 下标 1..X
reduceTask = make([]*ReduceTask, nReduce+1) // 下标 1..Y
```
 
`fetchMap` / `fetchReduce` 返回 0 就天然意味着"没有可分配的任务"，不需要额外的 bool flag 或者错误码，调用方直接 `if x > 0` 即可。如果从 0 开始建模，0 既是第一个合法任务又是哨兵，语义混乱，所有边界判断都要多绕一层。从 1 开始，模型干净，代码自然。
 
### 原子写文件

mapper 先写 `.tmp`，完成后 rename，利用 Unix rename 的原子性保证 reducer 不会读到写了一半的中间文件。

---

## TODO

### 1. 去掉"10s内完成或失败"的隐性假设

目前的容错逻辑是：超过 10s 没收到 `ResponseTask`，就认为 worker 已经寄了，把任务重新分配给别人。

这里有个隐患：**并不能保证原来的 worker 真的不会再写文件了**。它可能只是很慢，10s 后还在写，而新 worker 也在写同一个 `mr-x-y`，两边互相覆盖。

**正确做法：给每个 worker 分配唯一 ID（比如 PID），`.tmp` 文件名带上 worker ID：**

```
mr-1-1.tmp.{workerID}
```

只有拿到该任务的 worker 才能 rename 自己的 `.tmp` 文件。coordinator 在重新分配任务时，旧 worker 即使最终写完了，rename 的也是自己的 `.tmp`，不会污染新 worker 的结果。

这样就彻底去掉了"10s内一定完成"的假设。

### 2. 用 heap 替换线性扫描的 `fetchMap` / `fetchReduce`

目前 `fetchMap` 和 `fetchReduce` 每次都从头线性扫描整个任务列表，找第一个 `Todo` 或超时的 `Doing`。

更 canonical 的做法是维护一个**最小堆**，按 `assignedAt` 排序：

- `Todo` 的任务 `assignedAt` 设为零值，永远在堆顶
- `Doing` 的任务按分配时间排序，超时检查只需看堆顶

这样 `fetch` 的复杂度从 O(N) 降到 O(log N)，而且语义更自然——堆顶就是"最需要被处理的任务"。