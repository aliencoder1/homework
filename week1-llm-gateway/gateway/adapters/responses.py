'''
Author: xuxinyi
Date: 2026-11-07 20:55:52
LastEditors: xuxinyi
LastEditTime: 2026-11-07 21:00:47
FilePath: /homework/week1-llm-gateway/gateway/adapters/responses.py
'''
"""OpenAI Responses 协议 Adapter（DeepSeek：POST https://api.deepseek.com/responses）。"""

from ..config import api_key_for
from ..errors import GatewayError
from ..http_client import post_json, post_stream
from ..schemas import ChatResult, Usage
from ..sse import iter_sse_events
from ..structured import responses_text_format
from ..stream_events import done, text_delta, usage_event


def usage_from(u):
    """Responses 的 usage → 统一 Usage（非流式、流式共用）。"""
    u = u or {}
    return Usage(
        input_tokens=u.get("input_tokens", 0),                                   # Responses 的 input 已含缓存
        cached_input_tokens=(u.get("input_tokens_details") or {}).get("cached_tokens", 0),
        output_tokens=u.get("output_tokens", 0),
        reasoning_tokens=(u.get("output_tokens_details") or {}).get("reasoning_tokens"),
    )


def finish_from(response):
    """Responses 的 status + incomplete_details → 统一 finish_reason（非流式、流式共用）。"""
    if response.get("status") == "completed":
        return "stop"
    reason = (response.get("incomplete_details") or {}).get("reason")
    return "length" if reason == "max_output_tokens" else (reason or "unknown")


class ResponsesAdapter:
    def __init__(self, post=post_json, post_stream=post_stream):
        self.post = post                # 测试时可以换成假的发送函数（15.3 依赖注入）
        self.post_stream = post_stream  # 测试时换成“回放 fixtures 抓包”的函数

    # ---------- 统一请求 → Responses 请求 ----------
    def build_payload(self, request, route):
        system_parts = [m["content"] for m in request.messages if m["role"] == "system"]
        conversation = [{"role": m["role"], "content": m["content"]} for m in request.messages if m["role"] != "system"]
        payload = {"model": route.provider_model, "input": conversation}
        if system_parts:
            payload["instructions"] = "\n\n".join(system_parts)   # system 放进 instructions
        if request.max_tokens is not None:
            payload["max_output_tokens"] = request.max_tokens     # 字段名不一样
        if request.response_format:                               # 第 5 步：原生结构化输出
            payload["text"] = responses_text_format(request.response_format)
        return payload

    # ---------- Responses 响应 → 统一结果 ----------
    @staticmethod
    def parse_response(body, route, request_id):
        texts = []
        for item in body.get("output") or []:
            if item.get("type") != "message":       # 跳过 reasoning 等其他类型
                continue
            for part in item.get("content") or []:
                if part.get("type") == "output_text":
                    texts.append(part.get("text", ""))
        if body.get("status") == "failed":
            raise GatewayError("UPSTREAM_UNAVAILABLE", "model response failed", http_status=503, retryable=True)

        # DeepSeek 不返回 x-request-id 头，用响应体里的 id 关联上游请求
        return ChatResult(route.logical_model, "".join(texts), finish_from(body), usage_from(body.get("usage")),
                          request_id or body.get("id"))

    def generate(self, request, route):
        payload = self.build_payload(request, route)
        headers = {"Authorization": f"Bearer {api_key_for(route)}"}
        body, request_id = self.post(route.url, headers, payload, route.timeout_seconds)
        return self.parse_response(body, route, request_id)

    # ---------- 流式：Responses 事件 → 统一事件 ----------
    def stream(self, request, route):
        payload = {**self.build_payload(request, route), "stream": True}
        headers = {"Authorization": f"Bearer {api_key_for(route)}"}
        for name, data in iter_sse_events(self.post_stream(route.url, headers, payload, route.timeout_seconds)):
            if name == "response.output_text.delta":            # 事件名本身就说明“这是文字”
                yield text_delta(data["delta"])
            elif name in ("response.completed", "response.incomplete"):   # 终态：用量在 response.usage 里
                response = data["response"]
                yield usage_event(usage_from(response.get("usage")))
                yield done(finish_from(response), response.get("id"))
                return
            elif name in ("response.failed", "error"):
                raise GatewayError("UPSTREAM_UNAVAILABLE", "model stream failed", http_status=503, retryable=True)
            # 其余事件（created、in_progress、output_item.added、reasoning_text.delta……）不需要转发
        # 流读完了却没收到终态：不能当成正常结束（v3 学过：没有终态 = 协议错误）
        raise GatewayError("STREAM_INTERRUPTED", "model stream ended without a terminal event", http_status=502, retryable=True)
