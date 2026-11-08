from dataclasses import dataclass, field
from typing import Optional

from .errors import GatewayError

ALLOWED_ROLES = {"system", "user", "assistant"}


@dataclass(frozen=True)
class Usage:
    input_tokens: int            # 全部输入，含缓存命中的部分
    cached_input_tokens: int     # 其中命中缓存的部分（更便宜）
    output_tokens: int           # 全部输出，含思考 Token
    reasoning_tokens: Optional[int] = None   # 拿不到单独数字时是 None，不是 0


@dataclass(frozen=True)
class ChatRequest:
    model: str
    messages: list
    stream: bool = False
    response_format: Optional[dict] = None
    max_tokens: Optional[int] = None
    prompt: Optional[dict] = None

    @staticmethod
    def from_body(body):
        if not isinstance(body, dict):
            raise GatewayError("INVALID_REQUEST", "body must be a JSON object", http_status=400)
        model = body.get("model")
        if not isinstance(model, str) or not model:
            raise GatewayError("INVALID_REQUEST", "model is required", http_status=400)
        messages = body.get("messages")
        if not isinstance(messages, list) or not messages:
            raise GatewayError("INVALID_REQUEST", "messages must be a non-empty list", http_status=400)
        for m in messages:
            if not isinstance(m, dict) or m.get("role") not in ALLOWED_ROLES or not isinstance(m.get("content"), str):
                raise GatewayError("INVALID_REQUEST", "each message needs a valid role and string content", http_status=400)
        prompt = body.get("prompt")
        if prompt is not None and any(m["role"] == "system" for m in messages):
            # 两份 System Prompt 会冲突，说不清哪份生效（15.7）
            raise GatewayError("INVALID_REQUEST", "use either prompt or a system message, not both", http_status=400)
        max_tokens = body.get("max_tokens")
        if max_tokens is not None and (not isinstance(max_tokens, int) or max_tokens < 1):
            raise GatewayError("INVALID_REQUEST", "max_tokens must be a positive integer", http_status=400)
        return ChatRequest(model, messages, bool(body.get("stream", False)), body.get("response_format"), max_tokens, prompt)


@dataclass
class ChatResult:
    model: str
    content: str
    finish_reason: str
    usage: Usage
    upstream_request_id: Optional[str] = None
    extra: dict = field(default_factory=dict)
