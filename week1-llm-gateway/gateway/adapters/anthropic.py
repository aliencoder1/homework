'''
Author: xuxinyi
Date: 2026-11-07 21:14:17
LastEditors: xuxinyi
LastEditTime: 2026-11-08 14:14:20
FilePath: /homework/week1-llm-gateway/gateway/adapters/anthropic.py
'''
"""Anthropic Messages 协议 Adapter（DeepSeek：POST https://api.deepseek.com/anthropic/v1/messages）。

第 3 步：由你来写。照着 responses.py 的结构，把 4 个 TODO 填上。
写完运行：python try_step3.py        （离线检查）
          python try_step3.py --real （再真实调用一次）
"""

from ..config import api_key_for
from ..errors import GatewayError
from ..http_client import post_json, post_stream
from ..schemas import ChatResult, Usage
from ..sse import iter_sse_events
from ..stream_events import done, text_delta, usage_event

# Anthropic 协议要求必须给最大输出；调用方没传时用这个默认值
DEFAULT_MAX_TOKENS = 4096

STOP_REASON_MAPPING = {
    "end_turn": "stop",
    "max_tokens": "length",
}


def usage_from(u):
    """Anthropic 的 usage → 统一 Usage（非流式、流式共用）。input_tokens 不含缓存，要自己加。"""
    u = u or {}
    return Usage(
        input_tokens=u.get("input_tokens", 0) + u.get("cache_read_input_tokens", 0) + u.get("cache_creation_input_tokens", 0),
        cached_input_tokens=u.get("cache_read_input_tokens", 0),
        output_tokens=u.get("output_tokens", 0),
        reasoning_tokens=None,  # 拿不到单独数字时是 None，不是 0
    )


def finish_from(stop_reason):
    """Anthropic 的 stop_reason → 统一 finish_reason（非流式、流式共用）。"""
    return STOP_REASON_MAPPING.get(stop_reason, stop_reason or "unknown")


class AnthropicAdapter:
    def __init__(self, post=post_json, post_stream=post_stream):
        self.post = post
        self.post_stream = post_stream  # 测试时换成“回放 fixtures 抓包”的函数

    # ---------- 统一请求 → Messages 请求 ----------
    def build_payload(self, request, route):
        # TODO 1：组装请求体，需要这几个字段：
        #   model       → 用 route.provider_model（真实模型名）
        #   messages    → 除 system 以外的消息（只保留 role 和 content）
        #   system      → 所有 system 消息的内容拼起来，放在顶层；没有 system 就不要这个字段
        #   max_tokens  → request.max_tokens，没传就用 DEFAULT_MAX_TOKENS
        model = route.provider_model
        messages = [{"role": m["role"], "content": m["content"]} for m in request.messages if m["role"] != "system"]
        
        payload = {"model": model, "messages": messages}

        system_parts = [m["content"] for m in request.messages if m["role"] == "system"]

        if system_parts:
            payload["system"] = "\n\n".join(system_parts)  # system 放进 system
      
        if request.max_tokens is not None:
            payload["max_tokens"] = request.max_tokens  # 字段名一样
        else:
            payload["max_tokens"] = DEFAULT_MAX_TOKENS  # 使用默认值
        return payload



    # ---------- Messages 响应 → 统一结果 ----------
    @staticmethod
    def parse_response(body, route, request_id):
        # TODO 2：取出回答文本
        #   正文在 body["content"] 列表里；只要 type == "text" 的块，取它的 "text"，拼起来
        #   （type == "thinking" 的块是思考过程，要跳过）
        texts = []
        for item in body.get("content") or []:
            if item.get("type") != "text":  # 只保留文本模块
                continue
            texts.append(item.get("text", ""))

        return ChatResult(route.logical_model, "".join(texts), finish_from(body.get("stop_reason")),
                          usage_from(body.get("usage")), request_id or body.get("id"))

    def generate(self, request, route):
        payload = self.build_payload(request, route)
        # 这个协议的鉴权头和 Responses 不同（看笔记里的协议对照表）
        headers = {"x-api-key": api_key_for(route)}
        body, request_id = self.post(route.url, headers, payload, route.timeout_seconds)
        return self.parse_response(body, route, request_id)

    # ---------- 4.3 流式：Anthropic 事件 → 统一事件 ----------
    def stream(self, request, route):
        payload = {**self.build_payload(request, route), "stream": True}          # S2
        headers = {"x-api-key": api_key_for(route)}
        final_usage, stop_reason, upstream_id = None, None, None
        for name, data in iter_sse_events(self.post_stream(route.url, headers, payload, route.timeout_seconds)):
            if name == "message_start":                                         # 记下上游请求 id
                upstream_id = (data.get("message") or {}).get("id")
            elif name == "content_block_delta":
                delta = data.get("delta") or {}
                if delta.get("type") == "text_delta":                            # S3 只转发文字，思考不转发
                    yield text_delta(delta.get("text", ""))
            elif name == "message_delta":                                         # S4 先记下，不 yield
                final_usage = data.get("usage")
                stop_reason = (data.get("delta") or {}).get("stop_reason")
            elif name == "message_stop":                                          # S5 这才是终态
                yield usage_event(usage_from(final_usage))
                yield done(finish_from(stop_reason), upstream_id)
                return
            elif name == "error":                                                 # S6
                raise GatewayError("UPSTREAM_UNAVAILABLE", "model stream failed", http_status=503, retryable=True)
            # content_block_start/stop、ping：不转发
        # S7 没见到 message_stop：不能当正常结束
        raise GatewayError("STREAM_INTERRUPTED", "model stream ended without message_stop", http_status=502, retryable=True)
