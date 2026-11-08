"""第 2 步：先用假发送函数离线检查 Responses Adapter，再真实调用一次 DeepSeek。

运行：python3 try_step2.py          只跑离线检查
      python3 try_step2.py --real   离线检查 + 真实调用一次（会消耗极少量 Token）
"""
import os
import sys
import time

from gateway.adapters.responses import ResponsesAdapter
from gateway.config import ROUTES, load_dotenv
from gateway.errors import GatewayError
from gateway.schemas import ChatRequest

route = ROUTES["deepseek-v4-pro"]
request = ChatRequest.from_body({
    "model": "deepseek-v4-pro",
    "messages": [
        {"role": "system", "content": "你是简洁的助手，用一句中文回答。"},
        {"role": "user", "content": "用一句话解释什么是 LLM Gateway。"},
    ],
    "max_tokens": 2000,
})

# ---------- 1. 离线：看翻译出来的请求体 ----------
seen = {}

def fake_post(url, headers, payload, timeout):
    seen.update(url=url, headers=headers, payload=payload)
    return {
        "status": "completed",
        "output": [
            {"type": "reasoning", "summary": []},
            {"type": "message", "role": "assistant", "content": [{"type": "output_text", "text": "统一入口。"}]},
        ],
        "usage": {"input_tokens": 30, "input_tokens_details": {"cached_tokens": 10},
                  "output_tokens": 50, "output_tokens_details": {"reasoning_tokens": 42}},
    }, "req_fake"

os.environ.setdefault("DEEPSEEK_API_KEY", "offline-test-key")
result = ResponsesAdapter(post=fake_post).generate(request, route)
p = seen["payload"]
assert seen["url"] == "https://api.deepseek.com/responses"
assert seen["headers"]["Authorization"].startswith("Bearer ")
assert p["model"] == "deepseek-v4-pro" and "instructions" in p and p["max_output_tokens"] == 2000
assert all(m["role"] != "system" for m in p["input"])          # system 被挪进 instructions
assert result.content == "统一入口。" and result.finish_reason == "stop"   # reasoning 项被跳过
assert (result.usage.cached_input_tokens, result.usage.reasoning_tokens) == (10, 42)

for bad, code in [({"model": "x", "messages": []}, "INVALID_REQUEST"),
                  ({"model": "x", "messages": [{"role": "system", "content": "a"}], "prompt": {"name": "p"}}, "INVALID_REQUEST")]:
    try:
        ChatRequest.from_body(bad); raise AssertionError("should fail")
    except GatewayError as e:
        assert e.code == code
print("离线检查通过")
print("翻译后的请求体:", {k: v for k, v in p.items()})

# ---------- 2. 真实调用 ----------
if "--real" in sys.argv:
    del os.environ["DEEPSEEK_API_KEY"]   # 去掉离线用的假 Key，改从 .env 读真的
    load_dotenv()
    started = time.perf_counter()
    try:
        real = ResponsesAdapter().generate(request, route)
    except GatewayError as e:
        print(f"真实调用失败: {e.code}（上游状态 {e.upstream_status}）")
        raise SystemExit(1)
    print(f"\n真实调用耗时 {time.perf_counter() - started:.2f} 秒")
    print("回答:", real.content)
    print("finish_reason:", real.finish_reason)
    print("usage:", real.usage)
    print("upstream_request_id:", real.upstream_request_id)
