"""第 8 步：可观测 —— 每次调用记一条记录，按模型汇总 Token 分类和延迟。

一条记录 = 一次调用方请求（不是一次上游尝试），里面带 attempts 说明重试了几次。
Token 分 4 类（价格不同，所以要分开记，15.8）：
    uncached_input  没命中缓存的输入（贵）
    cached_input    命中缓存的输入（便宜）
    reasoning       思考 Token（算输出的钱，但用户看不到）
    visible_output  用户看得到的输出 = output - reasoning
延迟两个指标：
    latency_ms  从收到请求到最后一个字（含重试等待）
    ttft_ms     从收到请求到第一个字（只有流式才有意义）
不记录 messages 内容：日志里不放用户原文（隐私），只放 trace_id 方便关联。
"""
import json
import math
import threading
import time
from collections import Counter, defaultdict
from pathlib import Path

LOG_PATH = Path(__file__).resolve().parent.parent / "logs" / "calls.jsonl"


def token_breakdown(usage):
    if usage is None:
        return {"input": 0, "cached_input": 0, "uncached_input": 0, "output": 0, "reasoning": 0, "visible_output": 0}
    reasoning = usage.reasoning_tokens or 0      # None（协议不给）按 0 汇总，但单条记录里保留 None
    return {
        "input": usage.input_tokens,
        "cached_input": usage.cached_input_tokens,
        "uncached_input": usage.input_tokens - usage.cached_input_tokens,
        "output": usage.output_tokens,
        "reasoning": reasoning,
        "visible_output": usage.output_tokens - reasoning,
    }


def percentile(values, p):
    """最近秩法：P95 = 排序后第 ceil(0.95n) 个。"""
    if not values:
        return None
    ordered = sorted(values)
    return ordered[max(0, math.ceil(p / 100 * len(ordered)) - 1)]


class CallRecorder:
    def __init__(self, path=LOG_PATH, keep=10_000):
        self.path = Path(path) if path else None     # path=None：只在内存里（测试用）
        self.keep = keep
        self.records = []
        self.lock = threading.Lock()

    def record(self, *, trace_id, model, protocol, stream, status, attempts, latency_ms,
               ttft_ms=None, usage=None, error_code=None, prompt=None, finish_reason=None, upstream_request_id=None):
        rec = {
            "ts": round(time.time(), 3), "trace_id": trace_id, "model": model, "protocol": protocol,
            "stream": stream, "status": status, "error_code": error_code, "attempts": attempts,
            "finish_reason": finish_reason, "upstream_request_id": upstream_request_id, "latency_ms": latency_ms, "ttft_ms": ttft_ms,
            "reasoning_reported": bool(usage and usage.reasoning_tokens is not None),
            "tokens": token_breakdown(usage), "prompt": prompt,
        }
        with self.lock:
            self.records.append(rec)
            del self.records[:-self.keep]
            if self.path:
                self.path.parent.mkdir(exist_ok=True)
                with self.path.open("a", encoding="utf-8") as f:
                    f.write(json.dumps(rec, ensure_ascii=False) + "\n")
        return rec

    def stats(self):
        """GET /v1/stats：按模型汇总。"""
        with self.lock:
            records = list(self.records)
        by_model = defaultdict(list)
        for r in records:
            by_model[r["model"]].append(r)
        out = {}
        for model, rows in by_model.items():
            ok = [r for r in rows if r["status"] == "ok"]
            latencies = [r["latency_ms"] for r in ok]
            ttfts = [r["ttft_ms"] for r in ok if r["ttft_ms"] is not None]
            tokens = Counter()
            for r in rows:                    # 失败的请求如果已经产生用量，也要算（钱已经花了）
                tokens.update(r["tokens"])
            out[model] = {
                "requests": len(rows),
                "ok": len(ok),
                "errors": dict(Counter(r["error_code"] for r in rows if r["status"] != "ok")),
                "retried_requests": sum(1 for r in rows if r["attempts"] > 1),
                "tokens": dict(tokens),
                # Anthropic 协议不单独给思考 Token（算在 output 里）：这些请求的 reasoning 记 0、visible_output 偏大
                "reasoning_unreported": sum(1 for r in rows if r["status"] == "ok" and not r["reasoning_reported"]),
                "latency_ms": {"p50": percentile(latencies, 50), "p95": percentile(latencies, 95)},
                "ttft_ms": {"p50": percentile(ttfts, 50), "p95": percentile(ttfts, 95), "samples": len(ttfts)},
            }
        return {"models": out, "total_requests": len(records)}
