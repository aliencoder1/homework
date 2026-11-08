"""Gateway 服务层：入口校验 → Prompt 渲染 → 路由 → 限流 → 重试调 Adapter → 校验输出 → 记录。

HTTP 细节不在这里（在 main.py）。一次请求的主链：

    prepare()   校验请求体、找路由、渲染 Prompt 模板、加结构化输出说明      （第 1/5/6 步）
    admit()     按模型限流，超了直接 429，不调模型                           （第 7 步）
    重试循环    只重试 retryable 错误，最多 3 次，流式只在首字之前重试        （第 7 步）
    校验输出    response_format 时本地 json.loads + Schema 校验               （第 5 步）
    record()    Token 分类、latency、TTFT、attempts 写进日志                  （第 8 步）
"""
import json
import random
import time
from contextlib import closing
from dataclasses import dataclass, replace
from typing import Optional
from uuid import uuid4

from .adapters.anthropic import AnthropicAdapter
from .adapters.responses import ResponsesAdapter
from .config import ROUTES, model_limits
from .errors import GatewayError
from .observability import CallRecorder
from .prompts import PromptRegistry
from .resilience import ModelLimiter, RetryPolicy, call_with_retry
from .schemas import ChatRequest, Usage
from .structured import instruction, normalize, parse_and_validate


def usage_dict(usage):
    if usage is None:
        return None
    return {
        "input_tokens": usage.input_tokens,
        "cached_input_tokens": usage.cached_input_tokens,
        "output_tokens": usage.output_tokens,
        "reasoning_tokens": usage.reasoning_tokens,
    }


def add_usage(a, b):
    """重试时每次尝试的用量都要算上：失败的尝试也花了钱。"""
    if a is None or b is None:
        return a or b
    reasoning = None if a.reasoning_tokens is None and b.reasoning_tokens is None else \
        (a.reasoning_tokens or 0) + (b.reasoning_tokens or 0)
    return Usage(a.input_tokens + b.input_tokens, a.cached_input_tokens + b.cached_input_tokens,
                 a.output_tokens + b.output_tokens, reasoning)


def sse(event, data):
    """把一个事件编码成 SSE 文本：event 行 + data 行 + 空行。"""
    return f"event: {event}\ndata: {json.dumps(data, ensure_ascii=False)}\n\n"


def ms(seconds):
    return round(seconds * 1000)


@dataclass
class Call:
    """prepare() 的产物：这次调用要用到的一切，后面的步骤只读它。"""
    trace_id: str
    request: ChatRequest      # 已经渲染好 Prompt、加好 JSON 说明的请求
    route: object
    adapter: object
    fmt: Optional[dict]       # 统一后的 response_format；None = 普通文本
    prompt: Optional[dict]    # {"name","version","content_hash"}；没用模板时 None
    started: float


class StreamHandle:
    """流式响应：可迭代出 SSE 文本；close() 一定会把限流座位还回去（哪怕一次都没迭代）。"""

    def __init__(self, chunks, permit):
        self.chunks, self.permit = chunks, permit

    def __iter__(self):
        return self.chunks

    def close(self):
        self.chunks.close()
        self.permit.release()


class GatewayService:
    def __init__(self, routes=ROUTES, adapters=None, *, registry=None, limiter=None, recorder=None,
                 retry=RetryPolicy(), sleep=time.sleep, rng=random.random, clock=time.perf_counter):
        self.routes = routes
        # 协议名 → Adapter；测试时可以换成回放抓包 / 故意报错的 Adapter
        self.adapters = adapters or {"responses": ResponsesAdapter(), "anthropic": AnthropicAdapter()}
        self.registry = registry or PromptRegistry()
        self.limiter = limiter or ModelLimiter(model_limits())
        self.recorder = recorder or CallRecorder()
        self.retry, self.sleep, self.rng, self.clock = retry, sleep, rng, clock

    # ---------- 入口：校验 + 路由 + Prompt + 结构化说明 ----------
    def prepare(self, body, trace_id):
        started = self.clock()
        request = ChatRequest.from_body(body)
        route = self.routes.get(request.model)
        if route is None:
            raise GatewayError("MODEL_NOT_FOUND", f"unknown model: {request.model}", http_status=404)
        fmt = normalize(request.response_format)

        system = []
        prompt_ref = None
        if request.prompt is not None:                       # 第 6 步：模板 → System Prompt
            text, version = self.registry.render(request.prompt)
            system.append({"role": "system", "content": text})
            prompt_ref = version.ref()
        if fmt:                                              # 第 5 步：告诉模型要 JSON
            native = route.protocol == "responses"
            system.append({"role": "system", "content": instruction(fmt, native)})
        request = replace(request, messages=system + list(request.messages), response_format=fmt)
        return Call(trace_id, request, route, self.adapters[route.protocol], fmt, prompt_ref, started)

    def _record(self, call, *, status, attempts, usage=None, error=None, finish_reason=None,
                ttft=None, upstream_request_id=None):
        return self.recorder.record(
            trace_id=call.trace_id, model=call.route.logical_model, protocol=call.route.protocol,
            stream=call.request.stream, status=status, attempts=attempts,
            latency_ms=ms(self.clock() - call.started), ttft_ms=ttft, usage=usage,
            error_code=error.code if error else None, prompt=call.prompt, finish_reason=finish_reason,
            upstream_request_id=upstream_request_id)

    def _admit(self, call):
        try:
            return self.limiter.admit(call.route.logical_model)
        except GatewayError as error:                        # 被限流也要记：429 多了说明配额该调了
            self._record(call, status="error", attempts=0, error=error)
            raise

    # ---------- 非流式 ----------
    def complete(self, body, trace_id):
        call = self.prepare(body, trace_id)
        spent = []                                           # 失败尝试的用量（如果有）

        def attempt():
            result = call.adapter.generate(call.request, call.route)
            parsed = None
            if call.fmt:
                try:
                    parsed = parse_and_validate(result.content, call.fmt)
                except GatewayError as error:
                    error.usage = result.usage               # 模型已经输出了，钱花了，要记下来
                    raise
            return result, parsed

        def on_error(_attempt, error):
            if getattr(error, "usage", None):
                spent.append(error.usage)

        with self._admit(call):                              # 超限这里直接抛 429，不调模型
            try:
                (result, parsed), attempts = call_with_retry(attempt, self.retry, sleep=self.sleep,
                                                             rng=self.rng, on_error=on_error)
            except GatewayError as error:
                total = None
                for u in spent:
                    total = add_usage(total, u)
                self._record(call, status="error", attempts=getattr(error, "attempts", 1), usage=total, error=error,
                             upstream_request_id=error.upstream_request_id)
                raise
        usage = result.usage
        for u in spent:
            usage = add_usage(usage, u)
        record = self._record(call, status="ok", attempts=attempts, usage=usage, finish_reason=result.finish_reason,
                              upstream_request_id=result.upstream_request_id)
        body = {
            "id": call.trace_id,
            "model": result.model,
            "content": result.content,
            "finish_reason": result.finish_reason,
            "usage": usage_dict(usage),
            "latency_ms": record["latency_ms"],
            "attempts": attempts,
        }
        if call.fmt:
            body["parsed"] = parsed
        if call.prompt:
            body["prompt"] = call.prompt
        return body

    # ---------- 流式 ----------
    def open_stream(self, body, trace_id):
        """校验 + 准入都在发响应头之前做完：失败还能回正确的 4xx / 429。"""
        call = self.prepare(body, trace_id)
        permit = self._admit(call)
        return StreamHandle(self._stream(call, permit), permit)

    def _stream(self, call, permit):
        state = {"attempts": 0, "usage": None, "first_text_at": None, "texts": [], "recorded": False}
        try:
            while True:
                state["attempts"] += 1
                try:
                    # closing：不管怎么离开（正常 / 出错 / 调用方断开），都关掉上游连接，不再花钱
                    with closing(call.adapter.stream(call.request, call.route)) as upstream:
                        yield from self._relay(call, upstream, state)
                    return
                except GatewayError as error:
                    # 首字之前：悄悄重来一次，调用方无感；首字之后：只能报错（15.4）
                    if error.retryable and state["first_text_at"] is None and state["attempts"] < self.retry.max_attempts:
                        self.sleep(self.retry.delay(state["attempts"], self.rng))
                        continue
                    yield self._stream_error(call, state, error)
                    return
                except Exception:
                    # 未知 Bug：不把内部细节给调用方
                    yield self._stream_error(call, state, GatewayError("INTERNAL_ERROR", "internal error"))
                    return
        finally:
            permit.release()
            if not state["recorded"]:                        # 调用方中途断开
                self._record(call, status="cancelled", attempts=state["attempts"], usage=state["usage"],
                             error=GatewayError("CLIENT_CANCELLED", "client disconnected"), ttft=self._ttft(call, state))

    def _relay(self, call, upstream, state):
        """一次尝试：统一事件 → 对外 SSE。正常结束 return；出错抛 GatewayError 给上面决定重不重试。"""
        for event in upstream:
            if event["type"] == "text_delta":
                if state["first_text_at"] is None:
                    state["first_text_at"] = self.clock()    # TTFT 从第一个字算，不是连上就算
                state["texts"].append(event["text"])
                yield sse("delta", {"text": event["text"]})
            elif event["type"] == "usage":
                state["usage"] = event["usage"]
            elif event["type"] == "done":
                if call.fmt:                                 # 流式也要校验，只是只能在最后校验
                    parse_and_validate("".join(state["texts"]), call.fmt)
                ttft = self._ttft(call, state)
                record = self._record(call, status="ok", attempts=state["attempts"], usage=state["usage"],
                                      finish_reason=event["finish_reason"], ttft=ttft,
                                      upstream_request_id=event.get("upstream_request_id"))
                state["recorded"] = True
                data = {"id": call.trace_id, "model": call.route.logical_model,
                        "finish_reason": event["finish_reason"], "usage": usage_dict(state["usage"]),
                        "ttft_ms": ttft, "latency_ms": record["latency_ms"], "attempts": state["attempts"]}
                if call.prompt:
                    data["prompt"] = call.prompt
                yield sse("done", data)
                return
        raise GatewayError("STREAM_INTERRUPTED", "stream ended without done", http_status=502, retryable=True)

    def _ttft(self, call, state):
        return ms(state["first_text_at"] - call.started) if state["first_text_at"] else None

    def _stream_error(self, call, state, error):
        self._record(call, status="error", attempts=state["attempts"], usage=state["usage"], error=error,
                     ttft=self._ttft(call, state))
        state["recorded"] = True
        # 响应头已经发出去了，不能再改 HTTP 状态码，只能发一个 error 事件作为终态
        return sse("error", error.to_body(call.trace_id)["error"])


def new_trace_id():
    return f"gw_{uuid4().hex[:12]}"
