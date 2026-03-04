# 分布式任务调度器 Mini 版

一个用纯 Go 标准库（零外部依赖）实现的分布式任务调度系统，涵盖后端开发中最核心的分布式调度能力。

## ✨ 核心特性

| 特性 | 说明 |
|------|------|
| **任务注册** | 通过 HTTP API 提交任务，支持指定任务类型、优先级、超时、重试次数 |
| **优先级调度** | 基于堆（heap）的优先级队列，高优先级任务优先分配 |
| **自动重试** | 任务失败后自动重入队列，直到达到最大重试次数 |
| **超时检测** | 后台 goroutine 定期检查运行中的任务，超时自动重试或标记失败 |
| **失败回调** | 任务最终失败后，向指定 URL 发送 POST 回调通知 |
| **Worker 心跳** | Worker 定期上报心跳；调度器检测掉线 Worker 并重新分配其任务 |
| **并发安全** | 全局 `sync.RWMutex` + 优先级队列自带锁，保证线程安全 |
| **Panic 恢复** | Worker 执行 handler 时捕获 panic，不会导致进程崩溃 |

## 🏗️ 架构设计

```
┌──────────────┐          HTTP API          ┌──────────────┐
│   Client     │ ──── POST /api/tasks ────▶ │              │
│  (提交任务)   │                            │   Scheduler  │
└──────────────┘                            │   (调度器)    │
                                            │              │
┌──────────────┐  register / heartbeat      │  - 优先级队列  │
│   Worker 1   │ ◀──────────────────────▶   │  - 超时检测   │
│   Worker 2   │   pull task / report       │  - 重试管理   │
│   Worker N   │                            │  - 心跳监控   │
└──────────────┘                            └──────────────┘
```

**工作流程：**

1. **Client** 通过 HTTP API 提交任务到 Scheduler
2. **Worker** 启动时向 Scheduler 注册，获取唯一 ID
3. **Worker** 周期性拉取（Pull）待执行任务
4. **Worker** 在 context 超时控制下执行任务，上报结果
5. **Worker** 持续发送心跳，Scheduler 检测掉线并重分配任务
6. 任务最终失败时，Scheduler 向 `fail_callback` URL 发送通知

## 📂 项目结构

```
task-scheduler/
├── cmd/
│   ├── scheduler/main.go     # 调度器入口
│   ├── worker/main.go        # Worker 入口（含 demo handler）
│   └── client/main.go        # 演示客户端（提交任务 + 监控）
├── pkg/
│   ├── model/task.go          # 数据模型：Task, Worker, 优先级队列
│   ├── scheduler/
│   │   ├── scheduler.go       # 核心调度逻辑
│   │   ├── api.go             # HTTP API 层
│   │   └── scheduler_test.go  # 单元测试
│   └── worker/worker.go       # Worker 实现
├── go.mod
├── Makefile
└── README.md
```

## 🚀 快速开始

### 编译

```bash
cd task-scheduler
make build
```

### 运行

打开三个终端窗口：

```bash
# 终端 1：启动调度器
make run-scheduler
# 或: go run ./cmd/scheduler -addr :8080

# 终端 2：启动 Worker（可以启动多个）
make run-worker
# 或: go run ./cmd/worker -scheduler http://localhost:8080

# 终端 3：提交任务并观察
make run-client
# 或: go run ./cmd/client -scheduler http://localhost:8080
```

## 📡 HTTP API

### 任务管理

| Method | Endpoint | 说明 |
|--------|----------|------|
| `POST` | `/api/tasks` | 提交新任务 |
| `GET` | `/api/tasks` | 列出所有任务 |
| `GET` | `/api/tasks/{id}` | 查看任务详情 |
| `POST` | `/api/tasks/pull?worker_id=xxx` | Worker 拉取任务 |
| `POST` | `/api/tasks/{id}/complete` | 上报任务完成 |
| `POST` | `/api/tasks/{id}/fail` | 上报任务失败 |

### Worker 管理

| Method | Endpoint | 说明 |
|--------|----------|------|
| `POST` | `/api/workers/register` | 注册 Worker |
| `POST` | `/api/workers/{id}/heartbeat` | 心跳上报 |
| `GET` | `/api/workers` | 列出所有 Worker |

### 监控

| Method | Endpoint | 说明 |
|--------|----------|------|
| `GET` | `/api/stats` | 调度器统计信息 |

### 提交任务示例

```bash
curl -X POST http://localhost:8080/api/tasks \
  -H "Content-Type: application/json" \
  -d '{
    "name": "send_email",
    "payload": "{\"to\":\"user@example.com\"}",
    "priority": 10,
    "max_retries": 3,
    "timeout_seconds": 30,
    "fail_callback": "http://localhost:9090/on-fail"
  }'
```

### 查看统计信息

```bash
curl http://localhost:8080/api/stats | jq
```

## 🧪 测试

```bash
make test
```

测试覆盖：
- 任务提交与拉取
- 优先级排序
- 忙碌 Worker 不可拉取
- 重试机制（含 max_retries = 0）
- 超时检测与重试
- Worker 掉线检测与任务重分配
- 心跳机制
- 统计信息

## ⚙️ 命令行参数

### Scheduler

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `-addr` | `:8080` | 监听地址 |
| `-heartbeat-timeout` | `30s` | Worker 心跳超时 |
| `-check-interval` | `5s` | 后台检查间隔 |

### Worker

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `-scheduler` | `http://localhost:8080` | 调度器地址 |
| `-poll-interval` | `2s` | 任务拉取间隔 |
| `-heartbeat-interval` | `5s` | 心跳发送间隔 |

## 🔑 设计要点（面试/简历亮点）

1. **Pull 模型 vs Push 模型**：采用 Pull 模型，Worker 主动拉取任务，避免 Scheduler 需要维护 Worker 地址和连接状态
2. **Owner 校验**：`CompleteTask` / `FailTask` 校验 `workerID`，防止离线 Worker 的延迟响应覆盖新分配
3. **双重超时**：Scheduler 端超时检测 + Worker 端 `context.WithTimeout`，双保险
4. **Panic Recovery**：Worker handler 执行时 `defer recover()`，单个任务 panic 不影响 Worker 进程
5. **优雅关停**：`signal.Notify` + `sync.WaitGroup`，确保在途任务处理完毕
6. **零依赖**：仅使用 Go 标准库，无第三方依赖

## 📋 可扩展方向

- [ ] 持久化存储（Redis / MySQL）替代内存 map
- [ ] 任务去重 / 幂等执行
- [ ] 任务延迟执行（Delayed Task）
- [ ] 基于 gRPC 通信替代 HTTP
- [ ] 多调度器 HA（Raft 选主）
- [ ] Web Dashboard 可视化
- [ ] Prometheus metrics 接入
