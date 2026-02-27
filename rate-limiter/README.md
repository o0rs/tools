# 🚀 High-Performance Rate Limiter Service

一个用 Go 实现的高性能限流器服务，支持多种限流算法，提供 HTTP/gRPC 双协议接口，内置 Prometheus 监控指标和压测工具。

## ✨ 特性

- **三种限流算法**：令牌桶（Token Bucket）、漏桶（Leaky Bucket）、滑动窗口（Sliding Window）
- **双协议接口**：HTTP REST API + gRPC
- **高性能设计**：`sync.Mutex` 细粒度锁 + `sync.Map` 无锁读路径，单核 >30M ops/s
- **按 Key 隔离**：每个 Key 独立限流器实例，互不影响
- **Prometheus 监控**：请求总量、延迟分布、剩余容量实时指标
- **生产就绪**：优雅关闭、环境变量配置、Docker 支持

## 📁 项目结构

```
rate-limiter/
├── cmd/server/main.go         # 服务入口
├── pkg/
│   ├── limiter/
│   │   ├── limiter.go         # 接口定义 & 工厂方法
│   │   ├── token_bucket.go    # 令牌桶算法
│   │   ├── leaky_bucket.go    # 漏桶算法
│   │   ├── sliding_window.go  # 滑动窗口算法
│   │   ├── manager.go         # 按 Key 管理限流器
│   │   ├── limiter_test.go    # 单元测试
│   │   └── benchmark_test.go  # 性能基准测试
│   ├── server/
│   │   ├── http.go            # HTTP 服务
│   │   └── grpc.go            # gRPC 服务
│   ├── config/config.go       # 环境变量配置
│   └── metrics/prometheus.go  # Prometheus 指标
├── pb/                        # protobuf 生成代码
├── proto/ratelimit.proto      # protobuf 定义
├── scripts/benchmark.sh       # HTTP/gRPC 压测脚本
├── docs/
│   └── design-decisions.md    # 设计决策与已知局限
├── Makefile                   # 构建命令
├── Dockerfile                 # 容器化
└── README.md
```

## 🏗 算法设计

### 令牌桶 (Token Bucket)
- **原理**：桶中令牌以固定速率生成，请求消耗令牌。桶满时多余令牌丢弃。
- **特点**：允许突发流量（burst），长期平均速率恒定。
- **适用**：API 网关、用户级限流。

### 漏桶 (Leaky Bucket)
- **原理**：请求像水一样流入桶中，桶以恒定速率漏出。桶满则拒绝新请求。
- **特点**：输出速率完全平滑，消除突发。
- **适用**：需要严格匀速的场景，如消息队列写入。

### 滑动窗口 (Sliding Window Counter)
- **原理**：将时间窗口分为 N 个子桶，统计所有未过期子桶的请求总数。
- **特点**：在精度和内存间取得平衡，避免固定窗口的边界突变。
- **适用**：API 调用频率限制、短时间精确计数。

## 🚀 快速开始

### 前置依赖

```bash
# Go 1.22+
go version

# 安装 protobuf 编译器 (macOS)
brew install protobuf

# 安装 Go protobuf 插件
make proto-install
```

### 构建 & 运行

```bash
# 生成 protobuf 代码 & 构建
make all

# 运行服务
make run

# 或者使用环境变量自定义配置
HTTP_PORT=8080 GRPC_PORT=9090 DEFAULT_RATE=1000 DEFAULT_CAPACITY=2000 make run
```

### Docker

```bash
make docker
docker run -p 8080:8080 -p 9090:9090 rate-limiter:latest
```

## 📖 API 文档

### HTTP API

#### POST /api/v1/allow
检查请求是否被允许。

**请求体：**
```json
{
  "key": "user-123",
  "tokens": 1,
  "algorithm": "token_bucket"
}
```

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| key | string | ✅ | 限流标识（用户ID、IP、API Key等）|
| tokens | int | ❌ | 消耗令牌数，默认 1 |
| algorithm | string | ❌ | 算法：`token_bucket`/`leaky_bucket`/`sliding_window` |

**响应示例：**
```json
{
  "allowed": true,
  "remaining": 99,
  "retry_after_ms": 0,
  "message": "request allowed"
}
```

**被限流时 (HTTP 429)：**
```json
{
  "allowed": false,
  "remaining": 0,
  "retry_after_ms": 100,
  "message": "rate limit exceeded"
}
```

#### GET /health
健康检查端点。

#### GET /metrics
Prometheus 指标端点。

### gRPC API

```bash
# 使用 grpcurl 测试
grpcurl -plaintext -d '{"key":"user-123","tokens":1,"algorithm":"token_bucket"}' \
  localhost:9090 ratelimit.v1.RateLimitService/Allow
```

## ⚙️ 配置

通过环境变量配置：

| 环境变量 | 默认值 | 说明 |
|---------|--------|------|
| HTTP_PORT | 8080 | HTTP 监听端口 |
| GRPC_PORT | 9090 | gRPC 监听端口 |
| DEFAULT_ALGORITHM | token_bucket | 默认算法 |
| DEFAULT_RATE | 100 | 默认速率（req/s）|
| DEFAULT_CAPACITY | 200 | 默认容量 |
| DEFAULT_WINDOW | 1s | 滑动窗口大小 |

## 📊 Prometheus 指标

| 指标名 | 类型 | 标签 | 说明 |
|--------|------|------|------|
| `ratelimiter_requests_total` | Counter | key, algorithm, result | 请求总数 |
| `ratelimiter_request_duration_seconds` | Histogram | algorithm | 限流检查耗时 |
| `ratelimiter_tokens_remaining` | Gauge | key, algorithm | 剩余容量 |
| `ratelimiter_active_keys` | Gauge | - | 活跃 Key 数 |

**Grafana 示例查询：**
```promql
# 每秒请求率
rate(ratelimiter_requests_total[1m])

# 被限流比例
sum(rate(ratelimiter_requests_total{result="denied"}[5m]))
/ sum(rate(ratelimiter_requests_total[5m]))

# P99 延迟
histogram_quantile(0.99, rate(ratelimiter_request_duration_seconds_bucket[5m]))
```

## 🧪 测试 & 压测

```bash
# 运行单元测试（含数据竞争检测）
make test

# 运行性能基准测试
make bench

# HTTP 压测（需先启动服务）
chmod +x scripts/benchmark.sh
./scripts/benchmark.sh

# 或使用 Makefile 的 hey 压测
make bench-http
```

### 基准测试参考结果

```
BenchmarkTokenBucket-10            50000000    23.5 ns/op    0 B/op   0 allocs/op
BenchmarkLeakyBucket-10            50000000    24.1 ns/op    0 B/op   0 allocs/op
BenchmarkSlidingWindow-10          30000000    38.7 ns/op    0 B/op   0 allocs/op
BenchmarkTokenBucket_Parallel-10   20000000    62.3 ns/op    0 B/op   0 allocs/op
BenchmarkManager_MultiKey/keys=1000-10  10000000  108 ns/op  0 B/op   0 allocs/op
```

> 单核吞吐 ~30M+ ops/s，多核并行 ~15M+ ops/s，零内存分配。

## � 设计决策与已知局限

详见 [docs/design-decisions.md](docs/design-decisions.md)，涵盖：

- **并发安全保证** — `sync.Mutex` + `sync.Map` 如何保证不多放请求
- **单节点限制** — 多实例部署下的分布式限流方案
- **令牌浪费（Token Waste）** — 下游失败导致的令牌空耗问题及应对
- **非公平调度** — `sync.Mutex` 不保证 FIFO 顺序
- **All-or-Nothing 语义** — `AllowN(n)` 为什么不部分消耗

## �📄 License

MIT
