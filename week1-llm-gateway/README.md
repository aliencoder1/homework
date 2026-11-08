# 作业一：LLM 统一模型调用服务（Mini LLM Gateway）

用一套统一接口调用两种协议的模型，调用方只认**逻辑模型名**，不需要知道背后是哪种协议：

| 逻辑模型名（调用方传的） | 协议 | 真实模型（发给 DeepSeek 的） | 上游地址 |
| --- | --- | --- | --- |
| `deepseek-v4-pro` | OpenAI **Responses** API | `deepseek-v4-pro` | `POST https://api.deepseek.com/responses` |
| `deepseek-v4-flash` | **Anthropic Messages** API | `deepseek-flash` | `POST https://api.deepseek.com/anthropic/v1/messages` |

纯 Python 标准库实现，**不需要安装任何依赖**（Python 3.9+，开发时用的是 3.13）。

## 功能一览

| 要求 | 怎么实现 | 代码 |
| --- | --- | --- |
| 统一抽象层，按 model 路由 | 路由表 → 协议 Adapter；Adapter 负责“统一请求 ⇄ 协议请求”两次翻译 | `gateway/config.py`、`gateway/adapters/` |
| `stream=true` SSE | 两种协议的上游事件 → 统一事件 → 对外只有 `delta` / `done` / `error` 三种 SSE 事件 | `gateway/sse.py`、各 Adapter 的 `stream()` |
| `response_format` 合法 JSON | Responses 用原生 `text.format`；Anthropic 不支持，把 Schema 写进 system。**两者都由 Gateway 本地 `json.loads` + Schema 校验**，不合法自动重试 | `gateway/structured.py` |
| 提示词模板存储 / 变量替换 / 版本引用 | `prompts/<name>/<version>.json`；请求里引用 `name + version`；变量缺失/多余/类型错在调模型前就返回 400；每个版本有 `content_hash` | `gateway/prompts.py`、`prompts/` |
| Token 分类统计 | 4 类：未缓存输入 / 缓存输入 / 思考 / 可见输出；`GET /v1/stats` 按模型汇总 | `gateway/observability.py` |
| 延迟（含 TTFT） | `latency_ms`（到最后一个字）+ `ttft_ms`（到第一个字，流式）；P50 / P95 | 同上 |
| 统一错误码 | 所有上游异常都翻译成固定的 `code`，不泄露上游原始报错 | `gateway/errors.py` |
| 指数退避重试，最多 3 次 | Full Jitter：等待 = random(0, min(4s, 0.5s × 2^(n-1)))；只重试可重试错误；流式只在第一个字之前重试 | `gateway/resilience.py` |
| 按模型限流，超限 429 | 每个模型一个令牌桶（速率）+ 并发上限；超了直接 `429 RATE_LIMITED` + `Retry-After` | 同上 |

## 快速开始

```bash
cd homework/week1-llm-gateway

# 1. 配置 API Key（.env 已被 .gitignore 忽略）
cp .env.example .env
#    然后编辑 .env，填入自己的 DEEPSEEK_API_KEY

# 2. 启动（默认 http://127.0.0.1:8000，端口可用 PORT 改）
python main.py

# 3. 另开一个终端，跑全部验证（离线 + 真实端到端，约 40 秒）
python verify.py
python verify.py --offline     # 只跑离线部分：不需要 Key，不花钱
```

> ⚠️ 如果本机设置了 `HTTP_PROXY`，curl 调本机要加 `--noproxy '*'`，否则请求会被发到代理上。下面的例子都带了。

## 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `POST` | `/v1/chat/completions` | 统一调用；`stream: true` 时返回 SSE |
| `GET` | `/v1/stats` | 按模型汇总 Token、延迟、错误 |
| `GET` | `/v1/prompts` | 已注册的提示词模板和版本 |
| `GET` | `/healthz` | 健康检查 |

### 请求体

```jsonc
{
  "model": "deepseek-v4-pro",               // 必填，逻辑模型名
  "messages": [                              // 必填，非空；role 只能是 system / user / assistant
    {"role": "user", "content": "你好"}
  ],
  "stream": false,                           // 可选
  "max_tokens": 1000,                        // 可选，正整数
  "response_format": {"type": "json_object"},// 可选：text / json_object / json_schema
  "prompt": {                                // 可选：引用模板（和 system 消息二选一）
    "name": "translator", "version": "v1",
    "variables": {"target_language": "English"}
  }
}
```

### 非流式响应

```json
{
  "id": "gw_1a2b3c4d5e6f",
  "model": "deepseek-v4-pro",
  "content": "1+1等于2。",
  "finish_reason": "stop",
  "usage": {"input_tokens": 20, "cached_input_tokens": 0, "output_tokens": 35, "reasoning_tokens": 28},
  "latency_ms": 1524,
  "attempts": 1,
  "parsed": {},
  "prompt": {"name": "translator", "version": "v1", "content_hash": "sha256:1ec5f151091d3a58"}
}
```

`parsed` 只在带 `response_format` 时出现，`prompt` 只在引用模板时出现。`reasoning_tokens` 为 `null` 表示该协议不单独报告思考 Token（Anthropic 协议把它算在 `output_tokens` 里），**不是 0**。

### 流式响应（SSE）

```text
event: delta
data: {"text": "杭州"}

event: delta
data: {"text": "是浙江省省会……"}

event: done
data: {"id": "gw_…", "model": "deepseek-v4-flash", "finish_reason": "stop", "usage": {…}, "ttft_ms": 895, "latency_ms": 968, "attempts": 1}
```

流开始后（已经回了 200）如果出错，最后一帧是 `event: error`，`data` 里是统一错误体。**没有 `done` 就不算成功**。

## curl 示例

```bash
B=http://127.0.0.1:8000

# 1. 两种协议，同一种写法：只换 model
curl -s --noproxy '*' $B/v1/chat/completions -H 'Content-Type: application/json' -d '{
  "model": "deepseek-v4-pro",
  "messages": [{"role": "system", "content": "只用一句中文回答。"}, {"role": "user", "content": "1+1 等于几？"}]
}'
curl -s --noproxy '*' $B/v1/chat/completions -H 'Content-Type: application/json' -d '{
  "model": "deepseek-v4-flash",
  "messages": [{"role": "user", "content": "1+1 等于几？"}]
}'

# 2. 流式（-N 关掉 curl 自己的缓冲，才能看到一段段出来）
curl -N -s --noproxy '*' $B/v1/chat/completions -H 'Content-Type: application/json' -d '{
  "model": "deepseek-v4-flash", "stream": true,
  "messages": [{"role": "user", "content": "用一句话介绍杭州"}]
}'

# 3. 结构化输出：json_schema（两个模型都支持，返回里看 parsed）
curl -s --noproxy '*' $B/v1/chat/completions -H 'Content-Type: application/json' -d '{
  "model": "deepseek-v4-flash",
  "messages": [{"role": "user", "content": "我叫李雷，今年 28 岁，住在北京。"}],
  "response_format": {"type": "json_schema", "json_schema": {"name": "person", "schema": {
    "type": "object", "required": ["name", "age", "city"],
    "properties": {"name": {"type": "string"}, "age": {"type": "integer"}, "city": {"type": "string"}}}}}
}'

# 4. 结构化输出：json_object
curl -s --noproxy '*' $B/v1/chat/completions -H 'Content-Type: application/json' -d '{
  "model": "deepseek-v4-pro",
  "messages": [{"role": "user", "content": "给出北京和上海的人口（万人），键名用拼音"}],
  "response_format": {"type": "json_object"}
}'

# 5. 提示词模板：先看有哪些，再按 name + version 引用
curl -s --noproxy '*' $B/v1/prompts
curl -s --noproxy '*' $B/v1/chat/completions -H 'Content-Type: application/json' -d '{
  "model": "deepseek-v4-flash",
  "messages": [{"role": "user", "content": "今天天气真好"}],
  "prompt": {"name": "translator", "version": "v2", "variables": {"target_language": "Japanese", "tone": "polite"}}
}'

# 6. 错误码：缺变量 → 400 PROMPT_VARIABLES_INVALID（不会调模型）
curl -s --noproxy '*' $B/v1/chat/completions -H 'Content-Type: application/json' -d '{
  "model": "deepseek-v4-pro", "messages": [{"role": "user", "content": "hi"}],
  "prompt": {"name": "translator", "version": "v2", "variables": {"target_language": "en"}}
}'
# 未知模型 → 404 MODEL_NOT_FOUND
curl -s --noproxy '*' $B/v1/chat/completions -H 'Content-Type: application/json' \
  -d '{"model": "gpt-9", "messages": [{"role": "user", "content": "hi"}]}'

# 7. 限流：用很小的限额另起一个实例，连发 3 次，后面的返回 429 + Retry-After
PORT=8001 FLASH_RPS=0.05 FLASH_BURST=1 python main.py &
for i in 1 2 3; do
  curl -s -i --noproxy '*' http://127.0.0.1:8001/v1/chat/completions -H 'Content-Type: application/json' \
    -d '{"model": "deepseek-v4-flash", "max_tokens": 20, "messages": [{"role": "user", "content": "hi"}]}' \
    | grep -E '^HTTP|Retry-After|"code"'
done
kill %1

# 8. 可观测：按模型汇总
curl -s --noproxy '*' $B/v1/stats
```

## 统一错误码

所有错误都是同一个形状：

```json
{"error": {"code": "RATE_LIMITED", "message": "…", "retryable": true, "trace_id": "gw_…"}}
```

| code | HTTP | 可重试 | 含义 / 谁该处理 |
| --- | --- | --- | --- |
| `INVALID_REQUEST` | 400 | 否 | 请求体不合法（调用方修） |
| `PROMPT_VARIABLES_INVALID` | 400 | 否 | 模板变量缺失 / 多余 / 类型不对（调用方修） |
| `MODEL_NOT_FOUND` | 404 | 否 | 逻辑模型名不存在 |
| `PROMPT_NOT_FOUND` | 404 | 否 | 模板名或版本不存在 |
| `RATE_LIMITED` | 429 | 是 | **Gateway 自己**按模型限流，按 `Retry-After` 等一会儿 |
| `INTERNAL_ERROR` | 500 | 否 | Gateway 自身 Bug |
| `UPSTREAM_AUTH_FAILED` | 502 | 否 | 我们的 Key / 余额问题（运维修，不是调用方的错，所以不回 401） |
| `UPSTREAM_REJECTED` | 502 | 否 | 上游拒绝了请求（400 / 422 等） |
| `OUTPUT_INVALID` | 502 | 是 | 模型输出不是合法 JSON / 不符合 Schema（Gateway 内部已经重试过） |
| `STREAM_INTERRUPTED` | 502 | 是 | 流没有正常结束（没收到终态事件） |
| `UPSTREAM_RATE_LIMITED` | 503 | 是 | **DeepSeek** 限了我们（上游 429） |
| `UPSTREAM_UNAVAILABLE` | 503 | 是 | 上游 5xx / 连接失败 |
| `UPSTREAM_TIMEOUT` | 504 | 是 | 上游超时 |

**重试策略**：只有 `retryable=true` 的错误才在 Gateway 内部重试，最多 3 次（含第 1 次）。`401`、`400` 这类重试也没用的错误直接返回。流式请求只在第一个字发出去之前重试，之后出错只能发 `error` 事件。

## 限流配置

每个逻辑模型一份，可以用环境变量覆盖：

| 模型 | 每秒请求数 | 突发 | 最大并发 | 环境变量前缀 |
| --- | --- | --- | --- | --- |
| `deepseek-v4-pro` | 2 | 4 | 4 | `PRO_RPS` / `PRO_BURST` / `PRO_CONCURRENCY` |
| `deepseek-v4-flash` | 5 | 10 | 8 | `FLASH_RPS` / `FLASH_BURST` / `FLASH_CONCURRENCY` |

数字刻意远小于 DeepSeek 的上限（pro 500 / flash 2500 并发）：Gateway 先保护自己和账户余额，而不是等上游报 429。超限**不排队**，直接回 429，因为排队会把延迟藏起来。

## 可观测

每次调用（一次调用方请求，不是一次上游尝试）在 `logs/calls.jsonl` 写一条记录。记录里**不含用户消息原文**，只有 `trace_id`、模型、协议、状态、错误码、尝试次数、Token、延迟、TTFT、模板版本和上游请求 id。

`GET /v1/stats` 示例（节选）：

```json
{
  "models": {
    "deepseek-v4-pro": {
      "requests": 5, "ok": 5, "errors": {}, "retried_requests": 0,
      "tokens": {"input": 703, "cached_input": 0, "uncached_input": 703,
                 "output": 688, "reasoning": 606, "visible_output": 82},
      "reasoning_unreported": 0,
      "latency_ms": {"p50": 3306, "p95": 3455},
      "ttft_ms": {"p50": 3333, "p95": 3333, "samples": 1}
    }
  },
  "total_requests": 11
}
```

- Token 分 4 类，是因为价格不同：缓存命中的输入更便宜；思考 Token 按输出计费，但用户看不到。
- `reasoning_unreported`：Anthropic 协议不单独报告思考 Token，这些请求的 `reasoning` 按 0 汇总，`visible_output` 会偏大。
- 失败的请求不进延迟分位数（否则大量“秒失败”会把 P50 拉低，看起来反而更快了）。失败但已经产生的 Token 照样计入。
- 统计数据在内存里，重启后清零；JSONL 文件会保留。

## 提示词模板

```text
prompts/
├── translator/
│   ├── v1.json        变量 target_language
│   └── v2.json        变量 target_language、tone
└── contact_extractor/
    └── v1.json        变量 language（配合 response_format 使用）
```

```json
{
  "name": "translator",
  "version": "v1",
  "description": "把用户的话翻译成目标语言",
  "variables": {"target_language": "string"},
  "template": "You are a professional translator. Translate the user's message into ${target_language}. Output only the translation."
}
```

规则：

- 一个版本一个文件，**发布后不改**。要改就新建 `v2.json`，旧调用方不受影响。
- 请求里必须写死版本，不支持 `latest`，保证同一个请求今天和明天拿到的是同一份 Prompt。
- 变量类型支持 `string` / `integer` / `number` / `boolean`。
- 启动时会检查：文件夹名和文件名要与 `name` / `version` 一致，模板里的 `${变量}` 必须都声明过，否则拒绝启动。
- `content_hash` 只对 `template + variables` 计算，改 `description` 不算新内容。
- 渲染结果作为 system 放在最前面。所以 `prompt` 和 system 消息不能同时给，否则两份 System Prompt 会冲突。

## 目录结构

```text
week1-llm-gateway/
├── main.py                     HTTP 服务（标准库 ThreadingHTTPServer）
├── gateway/
│   ├── config.py               路由表、限流配置、读 .env
│   ├── schemas.py              统一请求 ChatRequest / 统一结果 ChatResult / Usage
│   ├── errors.py               GatewayError + 上游状态码映射 + 错误码总表
│   ├── http_client.py          发 HTTP（非流式 / 流式），把网络异常翻译成统一错误
│   ├── sse.py                  SSE 解析（空行分帧、注释行、多行 data、最后一帧）
│   ├── stream_events.py        统一流式事件：text_delta / usage / done
│   ├── adapters/
│   │   ├── responses.py        Responses 协议 ⇄ 统一格式
│   │   └── anthropic.py        Anthropic Messages 协议 ⇄ 统一格式
│   ├── structured.py           response_format：校验、生成 JSON 说明、本地 Schema 校验
│   ├── prompts.py              模板注册表：加载、hash、变量校验、渲染
│   ├── resilience.py           重试（Full Jitter）+ 令牌桶 + 并发上限
│   ├── observability.py        调用记录 + /v1/stats 汇总
│   └── service.py              主链：校验 → 模板 → 路由 → 限流 → 重试 → 校验输出 → 记录
├── prompts/                    提示词模板
├── fixtures/                   两种协议的真实流式抓包（离线回放用，不含 Key）
├── try_step2.py … try_step5_8.py  分步检查脚本
└── verify.py                   总验证脚本
```

## 一次请求经过什么

```text
POST /v1/chat/completions
  │
  ├─ prepare()   校验请求体 → 找路由（逻辑名 → 协议）→ 渲染模板 → 加 JSON 说明
  │              出错：4xx，此时还没调模型，不花钱
  ├─ _admit()    按模型占并发座位 + 拿令牌；不够：429 RATE_LIMITED
  ├─ 重试循环    Adapter.generate() / stream()
  │              Responses ⇄ 统一格式、Anthropic ⇄ 统一格式
  │              可重试错误：等待 random(0, 上限) 后再来，最多 3 次
  ├─ 校验输出    有 response_format 时：json.loads + Schema；坏了 → OUTPUT_INVALID → 重试
  ├─ _record()   Token 4 类、latency、ttft、attempts、模板版本 → logs/calls.jsonl
  └─ 响应        JSON，或 SSE 的 delta… → done
```

## 两种协议的关键差异（Adapter 处理的东西）

| | Responses（pro） | Anthropic Messages（flash） |
| --- | --- | --- |
| 鉴权头 | `Authorization: Bearer` | `x-api-key` |
| System Prompt | 顶层 `instructions` | 顶层 `system` |
| 对话 | `input` | `messages` |
| 最大输出 | `max_output_tokens`（可选） | `max_tokens`（Gateway 默认补 4096） |
| 回答文本 | `output[]` 里 `type=message` 项 → `content[]` 里 `output_text` | `content[]` 里 `type=text` 块 |
| 输入 Token | `input_tokens` **已含**缓存 | `input_tokens` **不含**缓存，要加 `cache_read` + `cache_creation` |
| 思考 Token | `output_tokens_details.reasoning_tokens` | 不单独给（含在 `output_tokens` 里） |
| JSON 输出 | 原生 `text.format` | 不支持 → Schema 写进 system |
| 流式文字事件 | `response.output_text.delta` | `content_block_delta` 且 `delta.type=text_delta` |
| 流式终态 | `response.completed` / `incomplete` / `failed` | `message_stop`（用量在之前的 `message_delta` 里） |

## 已知限制

- 统计数据只在单进程内存里；多实例部署需要把记录发到集中存储（如 Prometheus / ClickHouse）。
- 限流是单进程的；多实例时需要换成 Redis 等共享计数。
- Schema 校验只实现了常用子集（`type` / `properties` / `required` / `additionalProperties` / `enum` / `items`）；生产可换成 `jsonschema` 库。
- 流式 + `response_format`：只能在最后校验，坏 JSON 时文字已经发出去了，只能用 `error` 事件结束，不能重试。
- 没有做费用换算（Token × 单价）：DeepSeek 单价会变，需要带价格版本的账本，这里只做到 Token 分类。
