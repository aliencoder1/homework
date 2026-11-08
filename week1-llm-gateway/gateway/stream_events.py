"""统一的流式事件：不管哪种协议，Adapter.stream() 只产出这三种。"""


def text_delta(text):
    return {"type": "text_delta", "text": text}           # 一小段给用户看的文字


def usage_event(usage):
    return {"type": "usage", "usage": usage}              # 本次调用的用量（统一 Usage）


def done(finish_reason, upstream_request_id=None):
    return {"type": "done", "finish_reason": finish_reason,  # 正常结束（终态）
            "upstream_request_id": upstream_request_id}

# 出错不产出事件，而是抛 GatewayError，由上层决定能不能重试。
