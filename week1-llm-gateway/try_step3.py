"""第 3 步验收：检查你写的 AnthropicAdapter。先离线（假发送函数），加 --real 再真实调用一次。"""
import os
import sys
import time

from gateway.adapters.anthropic import DEFAULT_MAX_TOKENS, AnthropicAdapter
from gateway.config import ROUTES, load_dotenv
from gateway.errors import GatewayError
from gateway.schemas import ChatRequest, Usage

route = ROUTES["deepseek-v4-flash"]
with_system = ChatRequest.from_body({"model": "deepseek-v4-flash", "messages": [
    {"role": "system", "content": "你是简洁的助手，用一句中文回答。"},
    {"role": "user", "content": "用一句话解释什么是 LLM Gateway。"}]})
no_system = ChatRequest.from_body({"model": "deepseek-v4-flash", "max_tokens": 300,
                                   "messages": [{"role": "user", "content": "你好"}]})

FAKE_BODY = {
    "id": "msg_fake",
    "content": [{"type": "thinking", "thinking": "先想一想……"},
                {"type": "text", "text": "统一"}, {"type": "text", "text": "入口。"}],
    "stop_reason": "end_turn",
    "usage": {"input_tokens": 20, "cache_read_input_tokens": 10, "cache_creation_input_tokens": 0, "output_tokens": 50},
}
seen = {}

def fake_post(url, headers, payload, timeout):
    seen.update(url=url, headers=headers, payload=payload)
    return FAKE_BODY, None

def attempt(fn):
    """TODO 还没写完时不让脚本崩溃，只把对应检查标成 ❌。"""
    try:
        return fn()
    except NotImplementedError:
        return None


class Missing:
    content = finish_reason = upstream_request_id = None
    usage = Usage(-1, -1, -1, -1)


def check(name, ok):
    print(("  ✅ " if ok else "  ❌ ") + name)
    return ok

os.environ.setdefault("DEEPSEEK_API_KEY", "offline-test-key")
adapter = AnthropicAdapter(post=fake_post)
results = []

print("TODO 1 请求体")
p = attempt(lambda: adapter.build_payload(with_system, route)) or {}
results += [
    check("model 用真实模型名 deepseek-flash", p.get("model") == "deepseek-flash"),
    check("system 放在顶层", p.get("system") == "你是简洁的助手，用一句中文回答。"),
    check("messages 里只有用户消息，没有 system", p.get("messages") == [{"role": "user", "content": "用一句话解释什么是 LLM Gateway。"}]),
    check("没传 max_tokens 时用默认值", p.get("max_tokens") == DEFAULT_MAX_TOKENS),
]
p2 = attempt(lambda: adapter.build_payload(no_system, route)) or {"system": "missing"}
results += [
    check("没有 system 时不带 system 字段", "system" not in p2),
    check("传了 max_tokens 就用传的", p2.get("max_tokens") == 300),
]

print("TODO 2～4 解析响应")
r = attempt(lambda: adapter.generate(with_system, route)) or Missing()
results += [
    check("地址正确", seen.get("url") == "https://api.deepseek.com/anthropic/v1/messages"),
    check("文本只拼 text 块，跳过 thinking", r.content == "统一入口。"),
    check("input_tokens = 20 + 10 + 0（含缓存）", r.usage.input_tokens == 30),
    check("cached_input_tokens = 10", r.usage.cached_input_tokens == 10),
    check("output_tokens = 50", r.usage.output_tokens == 50),
    check("reasoning_tokens 拿不到单独数字", r.usage.reasoning_tokens is None),
    check("end_turn → stop", r.finish_reason == "stop"),
    check("上游请求 id", r.upstream_request_id == "msg_fake"),
]
# 写入缓存（cache_creation）不是命中，不能算进 cached_input_tokens
FAKE_BODY["usage"] = {"input_tokens": 20, "cache_read_input_tokens": 10, "cache_creation_input_tokens": 5, "output_tokens": 50}
r2 = attempt(lambda: adapter.generate(with_system, route)) or Missing()
results += [
    check("input_tokens = 20 + 10 + 5（三项都算输入）", r2.usage.input_tokens == 35),
    check("cached_input_tokens 只算命中的 10，不含写入缓存的 5", r2.usage.cached_input_tokens == 10),
]
FAKE_BODY["stop_reason"] = "max_tokens"
results.append(check("max_tokens → length", (attempt(lambda: adapter.generate(with_system, route)) or Missing()).finish_reason == "length"))

if not all(results):
    print(f"\n离线检查：{results.count(True)}/{len(results)} 通过，继续改 gateway/adapters/anthropic.py")
    raise SystemExit(1)
print(f"\n离线检查全部通过（{len(results)} 项）")

if "--real" in sys.argv:
    del os.environ["DEEPSEEK_API_KEY"]
    load_dotenv()
    started = time.perf_counter()
    try:
        real = AnthropicAdapter().generate(with_system, route)
    except GatewayError as e:
        print(f"真实调用失败: {e.code}（上游状态 {e.upstream_status}）")
        raise SystemExit(1)
    print(f"\n真实调用耗时 {time.perf_counter() - started:.2f} 秒")
    print("回答:", real.content)
    print("finish_reason:", real.finish_reason)
    print("usage:", real.usage)
    print("upstream_request_id:", real.upstream_request_id)
