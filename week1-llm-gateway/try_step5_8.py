"""第 5～8 步检查：全部离线（假 Adapter + 假时钟），不花钱、不用等。
运行：python try_step5_8.py          离线检查
      python try_step5_8.py --real   再真实调用 DeepSeek（结构化输出 + 模板 + 统计）
"""
import json
import os
import sys
import tempfile
import threading
from http.server import ThreadingHTTPServer
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from gateway.adapters.anthropic import AnthropicAdapter
from gateway.adapters.responses import ResponsesAdapter
from gateway.config import ROUTES
from gateway.errors import GatewayError, map_upstream_status
from gateway.observability import CallRecorder, percentile
from gateway.prompts import PromptRegistry
from gateway.resilience import LimitConfig, ModelLimiter, RetryPolicy, call_with_retry
from gateway.schemas import ChatResult, Usage
from gateway.service import GatewayService
from gateway.sse import iter_sse_events
from gateway.stream_events import done, text_delta, usage_event
from gateway.structured import normalize, parse_and_validate

os.environ.setdefault("DEEPSEEK_API_KEY", "offline-test-key")
results = []


def check(name, ok):
    print(("  ✅ " if ok else "  ❌ ") + name)
    results.append(bool(ok))


def error_code(fn):
    try:
        fn()
    except GatewayError as e:
        return e.code
    except ValueError:
        return "ValueError"
    return None


def upstream_503():
    return map_upstream_status(503)          # 每次新建：错误对象会被打上 attempts


class FakeClock:
    def __init__(self):
        self.now = 1000.0

    def __call__(self):
        return self.now


class Scripted:
    """假 Adapter：按剧本一次次返回。字符串 = 成功文本；异常 = 这次失败；列表 = 一次流式的事件。"""

    def __init__(self, *steps):
        self.steps = list(steps)
        self.requests = []

    def generate(self, request, route):
        self.requests.append(request)
        step = self.steps.pop(0)
        if isinstance(step, Exception):
            raise step
        return ChatResult(route.logical_model, step, "stop", Usage(100, 40, 30, 10), "up_1")

    def stream(self, request, route):
        self.requests.append(request)
        for item in self.steps.pop(0):
            if isinstance(item, Exception):
                raise item
            yield item


def ok_stream(*texts):
    return [text_delta(t) for t in texts] + [usage_event(Usage(50, 0, 20, None)), done("stop", "up_s")]


def service(adapter, **kw):
    sleeps = []
    kw.setdefault("limiter", ModelLimiter({}))
    svc = GatewayService(adapters={"responses": adapter, "anthropic": adapter}, recorder=CallRecorder(path=None),
                         sleep=sleeps.append, rng=lambda: 1.0, **kw)
    return svc, sleeps


def user(text="hi"):
    return [{"role": "user", "content": text}]


PERSON = {"type": "json_schema", "json_schema": {"name": "person", "schema": {
    "type": "object", "required": ["name", "age"], "additionalProperties": False,
    "properties": {"name": {"type": "string"}, "age": {"type": "integer"}}}}}

# ====================================================================
print("第 5 步 结构化输出")
check("json_object 合法", normalize({"type": "json_object"}) == {"type": "json_object"})
check("json_schema 缺 schema → 400 INVALID_REQUEST",
      error_code(lambda: normalize({"type": "json_schema", "json_schema": {"name": "x"}})) == "INVALID_REQUEST")
check("未知 type → 400", error_code(lambda: normalize({"type": "xml"})) == "INVALID_REQUEST")

real = GatewayService(recorder=CallRecorder(path=None), limiter=ModelLimiter({}))
call = real.prepare({"model": "deepseek-v4-pro", "messages": user(), "response_format": PERSON}, "t1")
payload = ResponsesAdapter().build_payload(call.request, call.route)
check("Responses：原生 text.format = json_schema（带 name + schema）",
      payload["text"]["format"]["type"] == "json_schema" and payload["text"]["format"]["schema"]["required"] == ["name", "age"])
call = real.prepare({"model": "deepseek-v4-flash", "messages": user(), "response_format": PERSON}, "t2")
payload = AnthropicAdapter().build_payload(call.request, call.route)
check("Anthropic：没有原生字段，Schema 写进顶层 system", "text" not in payload and '"required": ["name", "age"]' in payload["system"])

check("```json 代码块外壳能剥掉", parse_and_validate('```json\n{"name":"A","age":3}\n```', PERSON_N := normalize(PERSON)) == {"name": "A", "age": 3})
check("不是 JSON → OUTPUT_INVALID", error_code(lambda: parse_and_validate("好的：{name: A}", PERSON_N)) == "OUTPUT_INVALID")
check("缺 required 字段 → OUTPUT_INVALID", error_code(lambda: parse_and_validate('{"name":"A"}', PERSON_N)) == "OUTPUT_INVALID")
check("true 不能冒充 integer", error_code(lambda: parse_and_validate('{"name":"A","age":true}', PERSON_N)) == "OUTPUT_INVALID")
check("多余字段（additionalProperties:false）→ OUTPUT_INVALID",
      error_code(lambda: parse_and_validate('{"name":"A","age":1,"x":0}', PERSON_N)) == "OUTPUT_INVALID")

svc, sleeps = service(Scripted('not json', '{"name":"小王","age":30}'))
body = svc.complete({"model": "deepseek-v4-flash", "messages": user(), "response_format": PERSON}, "t3")
check("第 1 次输出坏 JSON → 自动重试，第 2 次合法，返回 parsed",
      body["attempts"] == 2 and body["parsed"] == {"name": "小王", "age": 30})
check("两次尝试的 Token 都算上（坏的那次也花钱了）", body["usage"]["output_tokens"] == 60)

# ====================================================================
print("第 6 步 Prompt 版本管理")
reg = PromptRegistry()
names = {(p["name"], p["version"]) for p in reg.list()}
check(f"启动时加载 {len(names)} 个版本", {("translator", "v1"), ("translator", "v2"), ("contact_extractor", "v1")} <= names)
text, v1 = reg.render({"name": "translator", "version": "v1", "variables": {"target_language": "English"}})
check("变量替换成功", "into English." in text)
check("同一内容 hash 稳定；v1 和 v2 hash 不同",
      v1.content_hash == PromptRegistry().get("translator", "v1").content_hash != reg.get("translator", "v2").content_hash)
check("缺变量 → PROMPT_VARIABLES_INVALID（调模型之前就拦住）",
      error_code(lambda: reg.render({"name": "translator", "version": "v2", "variables": {"target_language": "English"}}))
      == "PROMPT_VARIABLES_INVALID")
check("多传变量（多半是拼错）→ PROMPT_VARIABLES_INVALID",
      error_code(lambda: reg.render({"name": "translator", "version": "v1", "variables": {"target_language": "en", "tone": "x"}}))
      == "PROMPT_VARIABLES_INVALID")
check("类型不对 → PROMPT_VARIABLES_INVALID",
      error_code(lambda: reg.render({"name": "translator", "version": "v1", "variables": {"target_language": 1}}))
      == "PROMPT_VARIABLES_INVALID")
check("版本不存在 → 404 PROMPT_NOT_FOUND",
      error_code(lambda: reg.get("translator", "v9")) == "PROMPT_NOT_FOUND")
check("prompt 和 system 消息同时给 → 400",
      error_code(lambda: real.prepare({"model": "deepseek-v4-pro", "prompt": {"name": "translator", "version": "v1"},
                                       "messages": [{"role": "system", "content": "x"}] + user()}, "t")) == "INVALID_REQUEST")
with tempfile.TemporaryDirectory() as d:
    (Path(d) / "bad").mkdir()
    (Path(d) / "bad" / "v1.json").write_text(json.dumps(
        {"name": "bad", "version": "v1", "variables": {}, "template": "Hello ${who}"}))
    check("模板用了没声明的变量 → 启动时就拒绝加载", error_code(lambda: PromptRegistry(d)) == "ValueError")

fake = Scripted("Hello")
svc, _ = service(fake)
body = svc.complete({"model": "deepseek-v4-pro", "messages": user("你好"),
                     "prompt": {"name": "translator", "version": "v1", "variables": {"target_language": "English"}}}, "t4")
sent = fake.requests[0].messages
check("渲染结果作为 system 放在最前面", sent[0]["role"] == "system" and "English" in sent[0]["content"])
check("响应里带 name / version / content_hash", body["prompt"] == v1.ref())

# ====================================================================
print("第 7 步 韧性：重试 + 限流")
policy = RetryPolicy()
check("退避上限 0.5s → 1s → 2s …… 最多 4s", [policy.delay(n, lambda: 1.0) for n in (1, 2, 3, 6)] == [0.5, 1.0, 2.0, 4.0])
check("Full Jitter：实际等待在 0～上限 之间随机", policy.delay(3, lambda: 0.25) == 0.5)

svc, sleeps = service(Scripted(upstream_503(), upstream_503(), "ok"))
body = svc.complete({"model": "deepseek-v4-pro", "messages": user()}, "t5")
check("503 两次后成功：attempts=3，中间等了 2 次", body["attempts"] == 3 and sleeps == [0.5, 1.0])
svc, sleeps = service(Scripted(map_upstream_status(401), "ok"))
check("401 不重试（重试也没用）：直接 UPSTREAM_AUTH_FAILED，没等待",
      error_code(lambda: svc.complete({"model": "deepseek-v4-pro", "messages": user()}, "t6")) == "UPSTREAM_AUTH_FAILED" and sleeps == [])
svc, sleeps = service(Scripted(upstream_503(), upstream_503(), upstream_503(), "never"))
check("一直 503：最多 3 次就放弃", error_code(lambda: svc.complete({"model": "deepseek-v4-pro", "messages": user()}, "t7"))
      == "UPSTREAM_UNAVAILABLE" and svc.recorder.records[-1]["attempts"] == 3)


def run_stream(svc, body):
    handle = svc.open_stream(body, "ts")
    try:
        return list(iter_sse_events("".join(handle).splitlines()))
    finally:
        handle.close()


svc, sleeps = service(Scripted([upstream_503()], ok_stream("你", "好")))
frames = run_stream(svc, {"model": "deepseek-v4-flash", "stream": True, "messages": user()})
check("流式：首字之前失败 → 悄悄重试，调用方只看到成功", [n for n, _ in frames] == ["delta", "delta", "done"]
      and frames[-1][1]["attempts"] == 2)
svc, sleeps = service(Scripted([text_delta("你"), upstream_503()], ok_stream("不该出现")))
frames = run_stream(svc, {"model": "deepseek-v4-flash", "stream": True, "messages": user()})
check("流式：首字之后失败 → 不重试，以 error 事件结束", [n for n, _ in frames] == ["delta", "error"] and sleeps == [])

clock = FakeClock()
limiter = ModelLimiter({"m": LimitConfig(rate_per_second=1, burst=2, max_concurrency=5)}, clock=clock)
limiter.admit("m").release(); limiter.admit("m").release()
try:
    limiter.admit("m")
    third = None
except GatewayError as e:
    third = e
check("令牌桶 burst=2：第 3 个立刻 429 RATE_LIMITED，带 Retry-After",
      third and third.http_status == 429 and third.code == "RATE_LIMITED" and third.retry_after >= 1)
clock.now += 1.0
check("过 1 秒补回 1 个令牌 → 又能进", error_code(lambda: limiter.admit("m").release()) is None)
limiter = ModelLimiter({"m": LimitConfig(100, 100, max_concurrency=1)}, clock=clock)
held = limiter.admit("m")
check("并发上限 1：占着的时候第 2 个 → 429", error_code(lambda: limiter.admit("m")) == "RATE_LIMITED")
held.release(); held.release()
check("还座位后能再进（release 重复调用也只还一次）", error_code(lambda: limiter.admit("m").release()) is None)

svc, _ = service(Scripted(ok_stream("a", "b", "c")),
                 limiter=ModelLimiter({"deepseek-v4-flash": LimitConfig(100, 100, max_concurrency=1)}))
handle = svc.open_stream({"model": "deepseek-v4-flash", "stream": True, "messages": user()}, "tc")
next(iter(handle))                                             # 收到第一块就断开
handle.close()
check("流式中途断开：座位还回来了", error_code(lambda: svc.limiter.admit("deepseek-v4-flash").release()) is None)
check("断开也记一条 cancelled 记录", svc.recorder.records[-1]["status"] == "cancelled")

# ====================================================================
print("第 8 步 可观测")
rec = CallRecorder(path=None)
for latency in (100, 200, 300, 400, 1000):
    rec.record(trace_id="x", model="m", protocol="p", stream=True, status="ok", attempts=1, latency_ms=latency,
               ttft_ms=latency // 10, usage=Usage(100, 40, 30, 10))
rec.record(trace_id="y", model="m", protocol="p", stream=False, status="error", attempts=3, latency_ms=50,
           error_code="UPSTREAM_TIMEOUT")
s = rec.stats()["models"]["m"]
check("Token 分 4 类：未缓存输入 / 缓存输入 / 思考 / 可见输出",
      s["tokens"]["uncached_input"] == 300 and s["tokens"]["cached_input"] == 200
      and s["tokens"]["reasoning"] == 50 and s["tokens"]["visible_output"] == 100)
check("P50=300ms，P95=1000ms（失败请求不进延迟分位数）", s["latency_ms"] == {"p50": 300, "p95": 1000})
check("TTFT 也有 P50 / P95", s["ttft_ms"]["p50"] == 30 and s["ttft_ms"]["samples"] == 5)
check("错误按 code 计数，重试过的请求单独计数", s["errors"] == {"UPSTREAM_TIMEOUT": 1} and s["retried_requests"] == 1)
check("percentile 最近秩法", percentile([5, 1, 3], 50) == 3 and percentile([], 95) is None)
check("记录里没有用户消息原文", "messages" not in rec.records[0] and "content" not in json.dumps(rec.records[0]))

# ---------- HTTP：429 头、/v1/stats、/v1/prompts ----------
import main as gateway_main

gateway_main.SERVICE, _ = service(Scripted("one", "two"),
                                  limiter=ModelLimiter({"deepseek-v4-pro": LimitConfig(0.001, 1, 5)}))
server = ThreadingHTTPServer(("127.0.0.1", 0), gateway_main.Handler)
threading.Thread(target=server.serve_forever, daemon=True).start()
base = f"http://127.0.0.1:{server.server_address[1]}"


def http(method, path, body=None):
    req = Request(base + path, data=json.dumps(body).encode() if body else None, method=method,
                  headers={"Content-Type": "application/json"})
    try:
        with urlopen(req) as r:
            return r.status, r.headers, json.loads(r.read())
    except HTTPError as e:
        return e.code, e.headers, json.loads(e.read())


req_body = {"model": "deepseek-v4-pro", "messages": user()}
first = http("POST", "/v1/chat/completions", req_body)
second = http("POST", "/v1/chat/completions", req_body)
check("HTTP：第 1 个 200，第 2 个 429 + Retry-After 头 + RATE_LIMITED",
      first[0] == 200 and second[0] == 429 and second[1].get("Retry-After") and second[2]["error"]["code"] == "RATE_LIMITED")
status, _, stats = http("GET", "/v1/stats")
pro = stats["models"]["deepseek-v4-pro"]
check("GET /v1/stats 按模型返回：2 个请求，1 成功，1 次 RATE_LIMITED",
      status == 200 and pro["requests"] == 2 and pro["ok"] == 1 and pro["errors"] == {"RATE_LIMITED": 1})
status, _, prompts = http("GET", "/v1/prompts")
check("GET /v1/prompts 列出模板版本", status == 200 and len(prompts["prompts"]) >= 3)
server.shutdown()

print(f"\n{results.count(True)}/{len(results)} 通过")

# ====================================================================
if "--real" in sys.argv:
    from gateway.config import load_dotenv

    del os.environ["DEEPSEEK_API_KEY"]
    load_dotenv()
    svc = GatewayService(recorder=CallRecorder(path=None))
    print("\n真实调用：")
    for model in ("deepseek-v4-pro", "deepseek-v4-flash"):
        body = svc.complete({"model": model, "max_tokens": 800, "response_format": PERSON,
                             "messages": user("从这句话抽取人物：我叫李雷，今年 28 岁，住在北京。")}, "real")
        print(f"  {model} 结构化：parsed={body['parsed']}  attempts={body['attempts']}  {body['latency_ms']}ms")
    body = svc.complete({"model": "deepseek-v4-flash", "messages": user("今天天气真好"),
                         "prompt": {"name": "translator", "version": "v2",
                                    "variables": {"target_language": "English", "tone": "casual"}}}, "real")
    print(f"  模板 translator@v2：{body['content']!r}  {body['prompt']}")
    handle = svc.open_stream({"model": "deepseek-v4-flash", "stream": True, "messages": user("用一句话介绍你自己")}, "real")
    try:
        frames = list(iter_sse_events("".join(handle).splitlines()))
    finally:
        handle.close()
    print(f"  流式 done：{frames[-1][1]}")
    print("  /v1/stats：")
    print(json.dumps(svc.recorder.stats(), ensure_ascii=False, indent=2))
