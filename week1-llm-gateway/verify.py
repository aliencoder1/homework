"""作业一验证脚本：覆盖全部功能。

    python verify.py             第一部分离线 + 第二部分真实端到端（需要 .env 里的 DEEPSEEK_API_KEY，约 1 分钟、花费不到 1 毛）
    python verify.py --offline   只跑离线部分（不需要 Key，不花钱）

第一部分（离线）：跑 try_step3 / try_step4 / try_step5_8 —— 用真实抓包回放 + 故障注入，
                 覆盖真实环境很难触发的情况：上游 503 重试、401 不重试、断流、首字后失败、坏 JSON 重试……
第二部分（真实）：启动真正的 Gateway 子进程（python main.py），只通过 HTTP 调用，像外部调用方一样验收。
"""
import json
import os
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import ProxyHandler, Request, build_opener

ROOT = Path(__file__).resolve().parent
OPENER = build_opener(ProxyHandler({}))          # 本机地址不走 HTTP_PROXY
results = []


def check(name, ok, detail=""):
    print(("  ✅ " if ok else "  ❌ ") + name + (f"  —— {detail}" if detail else ""))
    results.append(bool(ok))
    return ok


def section(title):
    print(f"\n【{title}】")


# =====================================================================
# 第一部分：离线
# =====================================================================
def offline():
    print("=" * 60 + "\n第一部分：离线检查（回放抓包 + 故障注入）\n" + "=" * 60)
    env = {**os.environ, "DEEPSEEK_API_KEY": os.environ.get("DEEPSEEK_API_KEY", "offline-test-key")}
    for script, covers in [("try_step3.py", "Anthropic Adapter 翻译"),
                           ("try_step4.py", "SSE 解析 + 两种协议的流式 + HTTP 流"),
                           ("try_step5_8.py", "结构化输出 / Prompt 版本 / 重试 / 限流 / 可观测")]:
        proc = subprocess.run([sys.executable, script], cwd=ROOT, env=env, capture_output=True, text=True)
        out = proc.stdout + proc.stderr
        summary = next((line.strip() for line in reversed(out.splitlines()) if "通过" in line), "没有输出汇总")
        check(f"{script}（{covers}）", proc.returncode == 0 and "❌" not in out and "通过" in out, summary)
        if "❌" in out or proc.returncode != 0:
            print("\n".join("      " + line for line in out.splitlines() if "❌" in line or "Error" in line))


# =====================================================================
# 第二部分：真实端到端
# =====================================================================
class Gateway:
    """把 main.py 当子进程启动，结束时关掉。extra_env 用来给某个实例设很小的限流。"""

    def __init__(self, port, **extra_env):
        self.base = f"http://127.0.0.1:{port}"
        env = {**os.environ, "PORT": str(port), **{k: str(v) for k, v in extra_env.items()}}
        self.proc = subprocess.Popen([sys.executable, "main.py"], cwd=ROOT, env=env,
                                     stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        for _ in range(50):
            try:
                if self.get("/healthz")[0] == 200:
                    return
            except URLError:
                time.sleep(0.1)
        raise RuntimeError(f"Gateway 没有在 {self.base} 启动起来")

    def request(self, method, path, body=None, raw=None):
        data = raw if raw is not None else (json.dumps(body).encode() if body is not None else None)
        req = Request(self.base + path, data=data, method=method, headers={"Content-Type": "application/json"})
        try:
            with OPENER.open(req, timeout=180) as r:
                return r.status, dict(r.headers), r.read().decode()
        except HTTPError as e:
            return e.code, dict(e.headers), e.read().decode()

    def get(self, path):
        status, headers, text = self.request("GET", path)
        return status, json.loads(text)

    def chat(self, body, raw=None):
        status, headers, text = self.request("POST", "/v1/chat/completions", body, raw)
        if headers.get("Content-Type", "").startswith("text/event-stream"):
            return status, headers, parse_sse(text)
        return status, headers, json.loads(text)

    def close(self):
        self.proc.terminate()
        self.proc.wait(timeout=5)


def parse_sse(text):
    frames = []
    for block in text.strip().split("\n\n"):
        lines = block.splitlines()
        event = next(l[6:].strip() for l in lines if l.startswith("event:"))
        data = json.loads("".join(l[5:].strip() for l in lines if l.startswith("data:")))
        frames.append((event, data))
    return frames


def user(text):
    return [{"role": "user", "content": text}]


PERSON = {"type": "json_schema", "json_schema": {"name": "person", "schema": {
    "type": "object", "required": ["name", "age", "city"], "additionalProperties": False,
    "properties": {"name": {"type": "string"}, "age": {"type": "integer"}, "city": {"type": "string"}}}}}
MODELS = ["deepseek-v4-pro", "deepseek-v4-flash"]


def real():
    print("\n" + "=" * 60 + "\n第二部分：真实端到端（启动 main.py，只走 HTTP）\n" + "=" * 60)
    sys.path.insert(0, str(ROOT))
    from gateway.config import load_dotenv
    load_dotenv()
    if not os.getenv("DEEPSEEK_API_KEY"):
        check("找到 DEEPSEEK_API_KEY（复制 .env.example 为 .env 并填入）", False)
        return

    gw = Gateway(int(os.getenv("VERIFY_PORT", "8790")))
    try:
        section("0 服务")
        check("GET /healthz → 200", gw.get("/healthz")[0] == 200)

        section("1 两种协议、统一接口（非流式）")
        for model in MODELS:
            status, _, body = gw.chat({"model": model, "max_tokens": 1000,
                                       "messages": [{"role": "system", "content": "只用一句中文回答。"}] + user("1+1 等于几？")})
            u = body.get("usage") or {}
            check(f"{model}：200，有回答，finish_reason=stop", status == 200 and body.get("content") and body["finish_reason"] == "stop",
                  f"{body.get('content', body)!r:.40}  {body.get('latency_ms')}ms")
            check(f"{model}：统一 usage 4 个字段 + latency_ms",
                  set(u) == {"input_tokens", "cached_input_tokens", "output_tokens", "reasoning_tokens"} and u["input_tokens"] > 0)

        section("2 流式 SSE（两种协议对外都只有 delta / done）")
        for model in MODELS:
            status, headers, frames = gw.chat({"model": model, "stream": True, "max_tokens": 1000,
                                               "messages": user("用一句话介绍杭州")})
            names = [n for n, _ in frames]
            done = frames[-1][1]
            text = "".join(d["text"] for n, d in frames if n == "delta")
            check(f"{model}：text/event-stream，{names.count('delta')} 个 delta + 最后 done",
                  status == 200 and set(names) == {"delta", "done"} and names[-1] == "done" and text, f"{text!r:.30}")
            check(f"{model}：done 带 usage / ttft_ms / latency_ms，且 ttft ≤ latency",
                  done["usage"] and done["ttft_ms"] is not None and done["ttft_ms"] <= done["latency_ms"],
                  f"ttft {done['ttft_ms']}ms / 总 {done['latency_ms']}ms")

        section("3 结构化输出 response_format")
        for model in MODELS:
            status, _, body = gw.chat({"model": model, "max_tokens": 1000, "response_format": PERSON,
                                       "messages": user("我叫李雷，今年 28 岁，住在北京。")})
            parsed = body.get("parsed") or {}
            check(f"{model}：json_schema → parsed 符合 Schema",
                  status == 200 and isinstance(parsed.get("age"), int) and set(parsed) == {"name", "age", "city"}, str(parsed))
        status, _, body = gw.chat({"model": "deepseek-v4-pro", "max_tokens": 1000, "response_format": {"type": "json_object"},
                                   "messages": user("给出北京和上海的人口（万人），键名用拼音")})
        check("deepseek-v4-pro：json_object → parsed 是对象", status == 200 and isinstance(body.get("parsed"), dict), str(body.get("parsed"))[:60])
        status, _, frames = gw.chat({"model": "deepseek-v4-flash", "stream": True, "max_tokens": 1000, "response_format": PERSON,
                                     "messages": user("韩梅梅，25 岁，上海人")})
        streamed = "".join(d["text"] for n, d in frames if n == "delta")
        check("流式 + json_schema：拼起来是合法 JSON，以 done 结束",
              frames[-1][0] == "done" and json.loads(streamed).get("name"), streamed[:50])

        section("4 Prompt 模板：存储 / 变量替换 / 版本引用")
        status, listing = gw.get("/v1/prompts")
        refs = {(p["name"], p["version"]): p["content_hash"] for p in listing["prompts"]}
        check(f"GET /v1/prompts 列出 {len(refs)} 个版本，各有 content_hash",
              status == 200 and ("translator", "v1") in refs and ("translator", "v2") in refs)
        for version, variables in [("v1", {"target_language": "English"}),
                                   ("v2", {"target_language": "Japanese", "tone": "polite"})]:
            status, _, body = gw.chat({"model": "deepseek-v4-flash", "max_tokens": 1000, "messages": user("今天天气真好"),
                                       "prompt": {"name": "translator", "version": version, "variables": variables}})
            check(f"translator@{version}：变量生效，响应带版本和 hash",
                  status == 200 and body["prompt"] == {"name": "translator", "version": version,
                                                       "content_hash": refs[("translator", version)]},
                  repr(body.get("content"))[:50])
        status, _, body = gw.chat({"model": "deepseek-v4-pro", "max_tokens": 1000, "response_format": PERSON,
                                   "messages": user("联系人：王芳，32 岁，在深圳工作。"),
                                   "prompt": {"name": "contact_extractor", "version": "v1", "variables": {"language": "English"}}})
        check("模板 + 结构化输出一起用", status == 200 and body.get("parsed", {}).get("age") == 32, str(body.get("parsed")))

        section("5 统一错误码（都在调模型之前拦住，不花钱）")
        cases = [
            ("未知模型", {"model": "gpt-9", "messages": user("hi")}, None, 404, "MODEL_NOT_FOUND"),
            ("messages 为空", {"model": "deepseek-v4-pro", "messages": []}, None, 400, "INVALID_REQUEST"),
            ("请求体不是 JSON", None, b"{oops", 400, "INVALID_REQUEST"),
            ("response_format 不合法", {"model": "deepseek-v4-pro", "messages": user("hi"), "response_format": {"type": "xml"}},
             None, 400, "INVALID_REQUEST"),
            ("模板缺变量", {"model": "deepseek-v4-pro", "messages": user("hi"),
                       "prompt": {"name": "translator", "version": "v2", "variables": {"target_language": "en"}}},
             None, 400, "PROMPT_VARIABLES_INVALID"),
            ("模板版本不存在", {"model": "deepseek-v4-pro", "messages": user("hi"),
                         "prompt": {"name": "translator", "version": "v9", "variables": {}}}, None, 404, "PROMPT_NOT_FOUND"),
            ("流式请求也能在开流前回 404", {"model": "gpt-9", "stream": True, "messages": user("hi")}, None, 404, "MODEL_NOT_FOUND"),
        ]
        for name, body_in, raw, want_status, want_code in cases:
            status, _, body = gw.chat(body_in, raw)
            err = body.get("error", {})
            check(f"{name} → {want_status} {want_code}",
                  status == want_status and err.get("code") == want_code and err.get("trace_id") and "retryable" in err)

        section("6 可观测：GET /v1/stats")
        status, stats = gw.get("/v1/stats")
        for model in MODELS:
            s = stats["models"].get(model, {})
            t = s.get("tokens", {})
            check(f"{model}：Token 分 4 类 + P50/P95 延迟 + TTFT",
                  s.get("ok", 0) >= 3 and t.get("uncached_input", 0) + t.get("cached_input", 0) == t.get("input")
                  and t.get("reasoning", 0) + t.get("visible_output", 0) == t.get("output")
                  and s["latency_ms"]["p50"] and s["ttft_ms"]["samples"] >= 1,
                  f"{s.get('ok')} 次成功，tokens={t}，latency P50={s['latency_ms']['p50']}ms，TTFT P50={s['ttft_ms']['p50']}ms")
        log = ROOT / "logs" / "calls.jsonl"
        last = json.loads(log.read_text("utf-8").splitlines()[-1]) if log.exists() else {}
        check("logs/calls.jsonl 有逐条记录，且不含用户原文", last.get("trace_id") and "今天天气" not in log.read_text("utf-8"))
    finally:
        gw.close()

    section("7 按模型限流 → 429（另起一个限流很小的实例：flash 每秒 0.05 个、突发 2 个）")
    limited = Gateway(int(os.getenv("VERIFY_PORT", "8790")) + 1, FLASH_RPS=0.05, FLASH_BURST=2)
    try:
        body = {"model": "deepseek-v4-flash", "max_tokens": 20, "messages": user("hi")}
        with ThreadPoolExecutor(4) as pool:
            replies = list(pool.map(lambda _: limited.request("POST", "/v1/chat/completions", body), range(4)))
        codes = sorted(status for status, _, _ in replies)
        limited_reply = next((r for r in replies if r[0] == 429), None)
        check("同时 4 个请求：2 个放行，2 个 429", codes == [200, 200, 429, 429], f"状态码 {codes}")
        check("429 带 RATE_LIMITED + Retry-After 头（被拦的请求没调模型）",
              limited_reply and json.loads(limited_reply[2])["error"]["code"] == "RATE_LIMITED"
              and limited_reply[1].get("Retry-After"))
        status, _, _ = limited.request("POST", "/v1/chat/completions", {**body, "model": "deepseek-v4-pro"})
        check("限流按模型分开：flash 满了，pro 照样 200", status == 200)
        _, stats = limited.get("/v1/stats")
        check("被限流的请求也进了 stats", stats["models"]["deepseek-v4-flash"]["errors"].get("RATE_LIMITED") == 2)
    finally:
        limited.close()


if __name__ == "__main__":
    started = time.perf_counter()
    offline()
    if "--offline" not in sys.argv:
        real()
    passed = results.count(True)
    print(f"\n{'=' * 60}\n验证结果：{passed}/{len(results)} 通过，用时 {time.perf_counter() - started:.0f} 秒")
    sys.exit(0 if passed == len(results) else 1)
