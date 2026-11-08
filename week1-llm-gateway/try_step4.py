"""第 4 步检查：先用 fixtures/ 里的真实抓包离线回放，不花钱。"""
from pathlib import Path

from gateway.sse import iter_sse_events

FIX = Path(__file__).parent / "fixtures"


def check(name, ok):
    print(("  ✅ " if ok else "  ❌ ") + name)
    return ok


results = []
print("4.1 读 SSE")
r_events = list(iter_sse_events(open(FIX / "responses_stream.sse", encoding="utf-8")))
a_events = list(iter_sse_events(open(FIX / "anthropic_stream.sse", encoding="utf-8")))
r_names = [name for name, _ in r_events]
a_names = [name for name, _ in a_events]
results += [
    check(f"Responses 切出 {len(r_events)} 帧，第一帧 response.created", r_names[0] == "response.created"),
    check("Responses 最后一帧 response.completed", r_names[-1] == "response.completed"),
    check(f"Anthropic 切出 {len(a_events)} 帧，第一帧 message_start", a_names[0] == "message_start"),
    check("Anthropic 最后一帧 message_stop", a_names[-1] == "message_stop"),
    check("事件名和 data 里的 type 一致", all(n == d["type"] for n, d in r_events + a_events)),
]
# 边界：注释行、没有结尾空行、多行 data
tricky = [": keep-alive", "event: x", 'data: {"type":"x",', 'data: "v": 1}', "", 'data: {"type":"y"}']
t = list(iter_sse_events(tricky))
results += [
    check("忽略 : keep-alive 注释", t[0][0] == "x"),
    check("多行 data 拼起来", t[0][1] == {"type": "x", "v": 1}),
    check("最后一帧没有空行也不丢，没有 event: 时用 type", t[1] == ("y", {"type": "y"})),
]

print("4.2 Responses 流 → 统一事件")
import os
from gateway.adapters.responses import ResponsesAdapter
from gateway.config import ROUTES
from gateway.errors import GatewayError
from gateway.schemas import ChatRequest

os.environ.setdefault("DEEPSEEK_API_KEY", "offline-test-key")
req = ChatRequest.from_body({"model": "deepseek-v4-pro", "stream": True,
                             "messages": [{"role": "user", "content": "你好"}]})
sent = {}


def replay(path, cut_after=None):
    """假的 post_stream：把真实抓包一行行吐出来；cut_after 模拟流在第 N 行被切断。"""
    def fake(url, headers, payload, timeout):
        sent.update(payload=payload)
        for i, line in enumerate(open(path, encoding="utf-8")):
            if cut_after is not None and i >= cut_after:
                return
            yield line
    return fake


events = list(ResponsesAdapter(post_stream=replay(FIX / "responses_stream.sse")).stream(req, ROUTES["deepseek-v4-pro"]))
kinds = [e["type"] for e in events]
text = "".join(e["text"] for e in events if e["type"] == "text_delta")
usage = next(e["usage"] for e in events if e["type"] == "usage")
results += [
    check("请求体带 stream: true", sent["payload"].get("stream") is True),
    check(f"拼出完整回答：{text}", text == "你好！有什么可以帮你的吗？"),
    check("只产出三种统一事件", set(kinds) == {"text_delta", "usage", "done"}),
    check("顺序：先全部文字，最后 usage、done", kinds[-2:] == ["usage", "done"] and "usage" not in kinds[:-2]),
    check("usage 来自 response.completed：input 9 / output 8 / reasoning 0",
          (usage.input_tokens, usage.output_tokens, usage.reasoning_tokens) == (9, 8, 0)),
    check("finish_reason = stop", events[-1]["finish_reason"] == "stop"),
]
try:
    list(ResponsesAdapter(post_stream=replay(FIX / "responses_stream.sse", cut_after=20)).stream(req, ROUTES["deepseek-v4-pro"]))
    results.append(check("流被切断（没收到终态）要报错", False))
except GatewayError as e:
    results.append(check("流被切断（没收到终态）→ STREAM_INTERRUPTED", e.code == "STREAM_INTERRUPTED"))


print("4.3 Anthropic 流 → 统一事件（你来写）")
from gateway.adapters.anthropic import AnthropicAdapter

a_route = ROUTES["deepseek-v4-flash"]
a_req = ChatRequest.from_body({"model": "deepseek-v4-flash", "stream": True,
                               "messages": [{"role": "user", "content": "你好"}]})
a_path = FIX / "anthropic_stream.sse"
total_lines = sum(1 for _ in open(a_path, encoding="utf-8"))
sent.clear()


def run(adapter, request):
    try:
        return list(adapter.stream(request, a_route)), None
    except NotImplementedError:
        return None, "todo"
    except GatewayError as e:
        return None, e.code


a_events, err = run(AnthropicAdapter(post_stream=replay(a_path)), a_req)
if a_events is None:
    results.append(check("stream() 还没写（S2～S7）", False))
else:
    a_kinds = [e["type"] for e in a_events]
    a_text = "".join(e["text"] for e in a_events if e["type"] == "text_delta")
    a_usage = next((e["usage"] for e in a_events if e["type"] == "usage"), None)
    results += [
        check("S2 请求体带 stream: true", sent.get("payload", {}).get("stream") is True),
        check(f"S3 只拼 text_delta，跳过 50 个 thinking_delta：{a_text!r}", a_text == "你好"),
        check("只产出三种统一事件", set(a_kinds) == {"text_delta", "usage", "done"}),
        check("S5 顺序：最后是 usage、done", a_kinds[-2:] == ["usage", "done"]),
        check("S4 usage 用 message_delta 的最终值：input 35 / output 52",
              a_usage is not None and (a_usage.input_tokens, a_usage.output_tokens) == (35, 52)),
        check("reasoning_tokens 是 None", a_usage is not None and a_usage.reasoning_tokens is None),
        check("finish_reason = stop", a_events[-1].get("finish_reason") == "stop"),
    ]
_, err = run(AnthropicAdapter(post_stream=replay(a_path, cut_after=total_lines - 3)), a_req)
results.append(check("S7 收到 message_delta 但没收到 message_stop → STREAM_INTERRUPTED", err == "STREAM_INTERRUPTED"))
_, err = run(AnthropicAdapter(post_stream=replay(a_path, cut_after=40)), a_req)
results.append(check("S7 思考到一半被切断 → STREAM_INTERRUPTED", err == "STREAM_INTERRUPTED"))


print("4.4 对外 HTTP + SSE（用回放抓包的 Adapter 起一个真的本地服务）")
import json as _json
import threading
from http.server import ThreadingHTTPServer
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import main as gateway_main
from gateway.service import GatewayService

from gateway.observability import CallRecorder

gateway_main.SERVICE = GatewayService(recorder=CallRecorder(path=None), adapters={
    "responses": ResponsesAdapter(post_stream=replay(FIX / "responses_stream.sse")),
    "anthropic": AnthropicAdapter(post_stream=replay(FIX / "anthropic_stream.sse")),
})
server = ThreadingHTTPServer(("127.0.0.1", 0), gateway_main.Handler)
threading.Thread(target=server.serve_forever, daemon=True).start()
base = f"http://127.0.0.1:{server.server_address[1]}"


def call(body):
    req = Request(base + "/v1/chat/completions", data=_json.dumps(body).encode(), method="POST",
                  headers={"Content-Type": "application/json"})
    try:
        with urlopen(req) as resp:
            return resp.status, resp.headers.get("Content-Type"), resp.read().decode()
    except HTTPError as e:
        return e.code, e.headers.get("Content-Type"), e.read().decode()


for model, expect in [("deepseek-v4-pro", "你好！有什么可以帮你的吗？"), ("deepseek-v4-flash", "你好")]:
    status, ctype, raw = call({"model": model, "stream": True, "messages": [{"role": "user", "content": "你好"}]})
    frames = list(iter_sse_events(raw.splitlines()))
    names = [n for n, _ in frames]
    final = frames[-1][1]
    results += [
        check(f"{model}：200 + text/event-stream", status == 200 and ctype.startswith("text/event-stream")),
        check(f"{model}：拼出 {expect!r}", "".join(d["text"] for n, d in frames if n == "delta") == expect),
        check(f"{model}：最后一帧是 done，带 usage / ttft_ms / latency_ms",
              names[-1] == "done" and final["usage"] and final["ttft_ms"] is not None and final["latency_ms"] is not None),
        check(f"{model}：对外只有 delta 和 done 两种事件", set(names) == {"delta", "done"}),
    ]
status, _, raw = call({"model": "gpt-9", "stream": True, "messages": [{"role": "user", "content": "hi"}]})
results.append(check("未知模型 → 404 MODEL_NOT_FOUND（流还没开始，能回正确状态码）",
                     status == 404 and _json.loads(raw)["error"]["code"] == "MODEL_NOT_FOUND"))
status, _, raw = call({"model": "deepseek-v4-pro", "messages": []})
results.append(check("messages 为空 → 400 INVALID_REQUEST", status == 400 and _json.loads(raw)["error"]["code"] == "INVALID_REQUEST"))
gateway_main.SERVICE.adapters["responses"] = ResponsesAdapter(post_stream=replay(FIX / "responses_stream.sse", cut_after=20))
status, _, raw = call({"model": "deepseek-v4-pro", "stream": True, "messages": [{"role": "user", "content": "你好"}]})
frames = list(iter_sse_events(raw.splitlines()))
results.append(check("上游流中途断了 → 已发 200，最后一帧是 error 事件（STREAM_INTERRUPTED）",
                     status == 200 and frames[-1][0] == "error" and frames[-1][1]["code"] == "STREAM_INTERRUPTED"))
server.shutdown()

print(f"\n{results.count(True)}/{len(results)} 通过")

import sys
import time

if "--real" in sys.argv:
    from gateway.config import load_dotenv
    del os.environ["DEEPSEEK_API_KEY"]
    load_dotenv()
    for label, adapter, model in [("deepseek-v4-pro（Responses）", ResponsesAdapter(), "deepseek-v4-pro"),
                                  ("deepseek-v4-flash（Anthropic）", AnthropicAdapter(), "deepseek-v4-flash")]:
        real_req = ChatRequest.from_body({"model": model, "stream": True, "max_tokens": 2000,
                                          "messages": [{"role": "user", "content": "用三句话介绍 SSE。"}]})
        print(f"\n真实流式调用 {label}：")
        started = time.perf_counter()
        first = None
        for e in adapter.stream(real_req, ROUTES[model]):
            t = time.perf_counter() - started
            if e["type"] == "text_delta":
                first = first if first is not None else t
                print(e["text"], end="", flush=True)
            else:
                print(f"\n  [{t:5.2f}s] {e}")
        print(f"  首字时间 {first:.2f}s")
