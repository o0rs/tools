# 分布式任务调度器 Mini 版 — 架构设计文档

## 1. 系统总览

### 1.1 定位

纯 Go 标准库（零外部依赖）实现的分布式任务调度系统，采用 **Pull 模型**，涵盖任务注册、优先级调度、自动重试、超时检测、失败回调、Worker 心跳六大核心能力。

### 1.2 架构图

```
                          ┌─────────────────────────────────────┐
                          │           Scheduler (调度中心)        │
  ┌──────────┐            │                                     │            ┌──────────┐
  │  Client   │── POST ──▶│  ┌─────────┐     ┌──────────────┐  │◀── POST ──│  Worker 1 │
  │ (提交任务) │  /tasks   │  │ TaskQueue│     │ tasks map    │  │  /pull    │  Worker 2 │
  └──────────┘            │  │ (优先级堆)│     │ (全量任务表)  │  │           │  Worker N │
                          │  └─────────┘     └──────────────┘  │            └──────────┘
                          │                                     │                  │
                          │  ┌──────────────┐  ┌─────────────┐ │     heartbeat     │
                          │  │timeoutChecker│  │workerHealth- │ │◀── POST ─────────┘
                          │  │ (超时检测)    │  │Checker(掉线) │ │   每 5s 一次
                          │  └──────────────┘  └─────────────┘ │
                          └─────────────────────────────────────┘
```

### 1.3 三个进程

| 进程 | 入口 | 角色 | 生命周期 |
|------|------|------|---------|
| **Scheduler** | `cmd/scheduler/main.go` | HTTP 服务，调度中心 | 常驻 |
| **Worker** | `cmd/worker/main.go` | 无状态执行节点，可启多个 | 常驻 |
| **Client** | `cmd/client/main.go` | 提交任务 + 监控的演示脚本 | 一次性 |

---

## 2. 核心设计决策

### 2.1 Pull 模型 vs Push 模型

本系统采用 **Pull 模型**：Worker 主动轮询 Scheduler 领取任务。

| 对比项 | Pull（本项目） | Push |
|--------|--------------|------|
| Scheduler 需要知道 Worker 地址 | ❌ 不需要 | ✅ 需要 |
| Worker 加入/退出成本 | 零，启动即可用 | 高，需注册回调地址 |
| 负载均衡 | 天然均衡（谁空闲谁拿） | 需自行实现分配策略 |
| 实时性 | 有 pollInterval 延迟（默认 2s） | 任务到达即推送 |
| 弹性伸缩 | 天然支持 | 需要额外的服务发现机制 |

**选择理由：** Pull 模型在实现简单性和弹性伸缩方面有显著优势，2s 延迟对于非实时场景可接受。

### 2.2 单锁设计

Scheduler 使用一把 `sync.RWMutex` 保护所有共享状态：

```
写锁（互斥）: SubmitTask, PullTask, CompleteTask, FailTask, Heartbeat,
             checkTimeouts, checkWorkerHealth
读锁（共享）: GetTask, GetAllTasks, GetAllWorkers, Stats
```

**为什么不用细粒度锁：** 核心操作（如 PullTask）需要同时修改 task 和 worker 的状态，细粒度锁会引入死锁风险和中间状态不一致问题。单锁对于 mini 版足够。

### 2.3 Owner 校验

CompleteTask / FailTask 执行三重校验：

```go
if !ok || task.Status != model.TaskRunning || task.WorkerID != workerID {
    return false
}
```

| 条件 | 防御场景 |
|------|---------|
| `!ok` | 不存在的 task ID（bug 或恶意请求） |
| `status != running` | 任务已被超时检测器改状态，Worker 上报过期 |
| `workerID != workerID` | 任务已重分配给其他 Worker，旧 Worker 延迟上报不应覆盖 |

### 2.4 双重超时保障

```
层级 1 — Worker 端：context.WithTimeout(timeout)
    handler 内部通过 select + ctx.Done() 响应超时

层级 2 — Scheduler 端：timeoutChecker 每 5s 扫描
    即使 Worker 端超时机制失效，Scheduler 也会兜底处理
```

---

## 3. 数据模型

### 3.1 Task 状态机

```
                   PullTask                CompleteTask
  ┌─────────┐  ──────────▶  ┌─────────┐  ──────────▶  ┌───────────┐
  │ pending  │               │ running  │               │ completed │
  └─────────┘  ◀──────────  └─────────┘               └───────────┘
       ▲        FailTask       │    │
       │     (retries 剩余)     │    │ FailTask (retries 用完)
       │                       │    ▼
       │                       │  ┌─────────┐
       │    timeout+re-enqueue │  │ failed   │
       └───────────────────────┘  └─────────┘
                                    │
                  timeout (retries 用完)
                                    ▼
                               ┌─────────┐
                               │ timeout  │
                               └─────────┘
```

### 3.2 Task 字段说明

| 字段 | 类型 | 说明 |
|------|------|------|
| `ID` | string | 全局唯一，格式 `task-<16位hex>` |
| `Name` | string | 任务类型名，用于在 Worker 端匹配 handler |
| `Payload` | string | JSON 编码的业务参数 |
| `Priority` | int | 值越大优先级越高 |
| `MaxRetries` | int | 最大重试次数（不含首次） |
| `RetryCount` | int | 当前已重试次数 |
| `TimeoutSeconds` | int | 单次执行超时秒数 |
| `Status` | TaskStatus | pending / running / completed / failed / timeout |
| `WorkerID` | string | 当前执行此任务的 Worker ID |
| `FailCallback` | string | 彻底失败时 POST 回调的 URL |
| `StartedAt` | *time.Time | 指针类型，仅 running 时有值，用于超时计算 |

### 3.3 Worker 状态

```
  ┌──────┐  PullTask   ┌──────┐
  │ idle │ ──────────▶ │ busy │
  └──────┘ ◀────────── └──────┘
             Complete/
             Fail/Timeout
                │
                │ heartbeat 超时
                ▼
           ┌─────────┐
           │ offline  │
           └─────────┘
```

### 3.4 优先级队列

基于 `container/heap` 实现的最大堆：

- `Less(i, j)` 返回 `h[i].Priority > h[j].Priority`，高优先级先出队
- `Pop()` 时尾部元素置 nil，防止内存泄漏
- 外层包 `sync.Mutex`，保证并发安全
- 入队/出队时间复杂度：O(log n)

---

## 4. Scheduler 详解

### 4.1 存储结构

```
Scheduler
├── queue   *TaskQueue              ← 仅存 pending 任务，堆排序按优先级
├── tasks   map[string]*Task        ← 全量任务索引（含历史已完成/失败）
└── workers map[string]*WorkerInfo  ← 全量 Worker 索引
```

`tasks` 和 `queue` 中存的是同一个 Task 指针。修改 Task 字段两边都能看到。

### 4.2 核心方法

| 方法 | 调用者 | 作用 |
|------|--------|------|
| `SubmitTask` | Client (API) | 生成 ID → pending → 入队列 + 入 map |
| `PullTask` | Worker (API) | 校验 Worker idle → 出队最高优先级 → 双向绑定 task↔worker |
| `CompleteTask` | Worker (API) | 三重校验 → completed → 释放 Worker |
| `FailTask` | Worker (API) | 三重校验 → 释放 Worker → 判断重试 or 永久失败 |
| `Heartbeat` | Worker (API) | 刷新 `LastHeartbeat` 时间戳 |

### 4.3 后台 goroutine

#### timeoutChecker（超时检测）

```
每 checkInterval (5s) 执行一次：
  遍历 tasks map
    ↓
  status == running && startedAt != nil && timeoutSeconds > 0
    ↓
  now - startedAt > timeoutSeconds ?
    ├── YES + retries 剩余 → re-enqueue
    └── YES + retries 用完 → status=timeout + failCallback
```

检测精度：0~5s 延迟。30s 超时的任务最迟 35s 被发现。

#### workerHealthChecker（掉线检测）

```
每 checkInterval (5s) 执行一次：
  遍历 workers map
    ↓
  status != offline && now - lastHeartbeat > heartbeatTimeout (30s)
    ↓
  有在途任务？→ re-enqueue
  worker.status = offline
```

#### 退出机制

两个 goroutine 共享 `stopCh`（无缓冲 channel）。`Scheduler.Stop()` 调用 `close(stopCh)` 时，所有 goroutine 的 `select case <-stopCh` 同时触发，全部退出。这是 Go 中常见的广播退出模式。

### 4.4 失败回调

任务彻底失败时，通过 `go invokeFailCallback(task)` 异步 POST 到 `FailCallback` URL：

```json
{
  "task_id": "task-abc123",
  "name": "send_email",
  "error": "SMTP connection refused",
  "status": "failed",
  "retry_count": 4
}
```

使用单独 goroutine 调用，避免 HTTP 请求阻塞 Scheduler 的写锁。

---

## 5. Worker 详解

### 5.1 生命周期

```
New() → RegisterHandler() × N → Start() → [运行中] → Stop()
                                   │
                                   ├── POST /api/workers/register → 获取 ID
                                   ├── go heartbeatLoop()  (每 5s)
                                   └── go pollLoop()       (每 2s)
```

### 5.2 Handler 注册模式

```go
type TaskHandler func(ctx context.Context, payload string) (string, error)
```

Worker 是通用执行框架，具体做什么由注册的 handler 决定。Task 的 `Name` 字段匹配 handler：

```go
w.RegisterHandler("send_email", handleSendEmail)
w.RegisterHandler("process_image", handleProcessImage)
```

找不到 handler 时上报失败（不是 panic），Scheduler 正常走重试逻辑。

### 5.3 任务执行的三层保护

```go
ctx, cancel := context.WithTimeout(context.Background(), timeout)

ch := make(chan execResult, 1)  // buffer=1 关键

go func() {
    defer func() {
        if r := recover(); r != nil {       // 保护层 2：panic 恢复
            ch <- execResult{"", fmt.Errorf("handler panicked: %v", r)}
        }
    }()
    res, err := handler(ctx, task.Payload)  // 保护层 1：context 超时
    ch <- execResult{res, err}
}()

select {
case <-ctx.Done():                          // 保护层 3：外层超时兜底
    w.reportFailure(...)
case res := <-ch:
    ...
}
```

| 保护层 | 机制 | 防御场景 |
|--------|------|---------|
| 1 | `context.WithTimeout` | handler 内部检查 `ctx.Done()` 主动退出 |
| 2 | `defer recover()` | handler panic 不崩溃 Worker 进程 |
| 3 | `select` 外层超时 | handler 卡死不检查 ctx 时，主流程仍能超时退出 |

### 5.4 channel buffer=1 的必要性

```
超时场景：ctx.Done() 先触发 → pullAndExecute return → 没人再读 ch
              ↓
handler goroutine 之后完成 → ch <- result
              ↓
buffer=0：永远阻塞 → goroutine 泄漏 ❌
buffer=1：写入 buffer 成功 → goroutine 正常退出 ✅
```

整个生命周期中最多写入 1 次，所以 buffer=1 是最小充分条件。

### 5.5 select 的行为

- 两个 case 都没就绪：goroutine 被 runtime 挂到两个 channel 的等待队列上，休眠（不消耗 CPU）
- 某个 channel 就绪：runtime 唤醒 goroutine，从所有等待队列摘除，执行对应 case
- 两个同时就绪：**随机选一个**（Go 规范要求，防止饥饿）
- 因为有 context 超时，select 一定在有限时间内返回

### 5.6 弹性伸缩

**扩容：** 直接启动新 Worker 实例，自动注册 + 开始拉任务，Scheduler 无需任何配置变更。

**缩容（优雅）：** `SIGINT/SIGTERM` → `close(stopCh)` → `wg.Wait()` 等在途任务完成 → 退出。之后 Scheduler 在 30s 心跳超时后标记 offline。

**缩容（崩溃）：** Worker 不再发心跳 → Scheduler 30s 后检测到 → 标记 offline → 在途任务 re-enqueue → 其他 Worker 接手。

---

## 6. HTTP API

### 6.1 路由总表

| Method | Endpoint | Handler | 说明 |
|--------|----------|---------|------|
| POST | `/api/tasks` | `handleTasks` | 提交新任务 |
| GET | `/api/tasks` | `handleTasks` | 列出所有任务 |
| POST | `/api/tasks/pull?worker_id=` | `handlePullTask` | Worker 拉取任务 |
| GET | `/api/tasks/{id}` | `handleTaskByID` | 查看任务详情 |
| POST | `/api/tasks/{id}/complete` | `handleTaskByID` | 上报完成 |
| POST | `/api/tasks/{id}/fail` | `handleTaskByID` | 上报失败 |
| POST | `/api/workers/register` | `handleRegisterWorker` | Worker 注册 |
| POST | `/api/workers/{id}/heartbeat` | `handleWorkerByID` | 心跳上报 |
| GET | `/api/workers` | `handleListWorkers` | 列出所有 Worker |
| GET | `/api/stats` | `handleStats` | 调度器统计 |

### 6.2 路由匹配

使用标准库 `http.ServeMux`，按最长前缀匹配：

- `/api/tasks/pull` 比 `/api/tasks/` 更具体，优先匹配
- `/api/workers/register` 比 `/api/workers/` 更具体，优先匹配
- 路径参数通过 `strings.TrimPrefix` + `strings.Split` 手动解析

---

## 7. 完整任务生命周期（含一次重试）

```
1. Client POST /api/tasks
   → Scheduler.SubmitTask()
   → 生成 "task-abc123", status=pending, 入队列

2. Worker POST /api/tasks/pull?worker_id=worker-xyz
   → Scheduler.PullTask()
   → 出队最高优先级任务, status=running, workerID=worker-xyz

3. Worker 查找 handlers["send_email"] → 找到 handleSendEmail
   → context.WithTimeout(30s) 下执行
   → 30% 概率失败: "SMTP connection refused"

4. Worker POST /api/tasks/task-abc123/fail
   → Scheduler.FailTask()
   → retryCount: 0→1, maxRetries=3, 还有重试次数
   → status=pending, re-enqueue

5. 下一个 poll 周期, Worker 再次 PullTask
   → 第二次尝试... 成功

6. Worker POST /api/tasks/task-abc123/complete
   → Scheduler.CompleteTask()
   → status=completed, result="email sent: ..."
```

---

## 8. 配置参数

### Scheduler

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `-addr` | `:8080` | HTTP 监听地址 |
| `-heartbeat-timeout` | `30s` | Worker 心跳超时阈值 |
| `-check-interval` | `5s` | 后台检查器扫描间隔 |

### Worker

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `-scheduler` | `http://localhost:8080` | Scheduler 地址 |
| `-poll-interval` | `2s` | 任务拉取轮询间隔 |
| `-heartbeat-interval` | `5s` | 心跳发送间隔 |

---

## 9. Scheduler 职责边界

```
Scheduler 做什么：                    Scheduler 不做什么：
  ✅ 存储任务和 Worker 状态（内存）      ❌ 不执行任务（Worker 做）
  ✅ 按优先级分配任务                   ❌ 不主动推送任务（Worker 来拉）
  ✅ 检测超时、检测掉线、自动重试         ❌ 不持久化（纯内存，重启丢失）
  ✅ 触发失败回调                       ❌ 不做认证鉴权（mini 版省略）
  ✅ 暴露 HTTP API
```

---

## 10. 可扩展方向

| 方向 | 说明 |
|------|------|
| 持久化存储 | Redis / MySQL 替代内存 map，支持 Scheduler 重启恢复 |
| 任务去重 | 基于 Name+Payload 哈希的幂等提交 |
| 延迟任务 | 支持 `execute_after` 字段，到时间才入队 |
| gRPC 通信 | 替代 HTTP，降低序列化开销和连接管理成本 |
| 多 Scheduler HA | Raft 选主，避免单点故障 |
| Web Dashboard | 实时展示任务流转和 Worker 状态 |
| Prometheus metrics | 接入监控体系，暴露 /metrics 端点 |
| Worker 并发槽位 | 单 Worker 同时处理多个任务，提升吞吐 |
