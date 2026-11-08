import http.client
import json
import socket
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from .errors import GatewayError, map_upstream_status


# 默认的 Python-urllib User-Agent 会被上游直接断开连接，所以显式声明自己是谁。
USER_AGENT = "week1-llm-gateway/0.1"


def post_json(url, headers, payload, timeout_seconds):
    """发一个 JSON POST，返回 (body, request_id)。只报告网络事实，不决定要不要重试（15.3）。"""
    request = Request(url, data=json.dumps(payload).encode("utf-8"),
                      headers={**headers, "Content-Type": "application/json", "User-Agent": USER_AGENT}, method="POST")
    try:
        with urlopen(request, timeout=timeout_seconds) as response:
            return json.loads(response.read().decode("utf-8")), response.headers.get("x-request-id")
    except HTTPError as error:
        raise map_upstream_status(error.code, error.headers.get("x-request-id")) from error
    except (TimeoutError, socket.timeout) as error:
        raise GatewayError("UPSTREAM_TIMEOUT", "model service timed out", http_status=504, retryable=True) from error
    except (ConnectionError, http.client.HTTPException) as error:
        # 连接被对方断开、响应不完整：属于临时网络问题，可以重试
        raise GatewayError("UPSTREAM_UNAVAILABLE", "connection to model service was dropped", http_status=503, retryable=True) from error
    except URLError as error:
        if isinstance(error.reason, (TimeoutError, socket.timeout)):
            raise GatewayError("UPSTREAM_TIMEOUT", "model service timed out", http_status=504, retryable=True) from error
        raise GatewayError("UPSTREAM_UNAVAILABLE", "cannot reach model service", http_status=503, retryable=True) from error


def post_stream(url, headers, payload, timeout_seconds):
    """发一个流式 POST，一行一行地产出响应文本。

    按行读，每行在换行符处断开；UTF-8 里换行符不会出现在多字节汉字中间，
    所以按行解码不会把一个汉字切成两半（15.4 讲过的乱码问题在这里自然避开了）。
    """
    request = Request(url, data=json.dumps(payload).encode("utf-8"),
                      headers={**headers, "Content-Type": "application/json",
                               "Accept": "text/event-stream", "User-Agent": USER_AGENT},
                      method="POST")
    try:
        response = urlopen(request, timeout=timeout_seconds)
    except HTTPError as error:
        raise map_upstream_status(error.code, error.headers.get("x-request-id")) from error
    except (TimeoutError, socket.timeout) as error:
        raise GatewayError("UPSTREAM_TIMEOUT", "model service timed out", http_status=504, retryable=True) from error
    except (ConnectionError, http.client.HTTPException) as error:
        raise GatewayError("UPSTREAM_UNAVAILABLE", "connection to model service was dropped", http_status=503, retryable=True) from error
    except URLError as error:
        raise GatewayError("UPSTREAM_UNAVAILABLE", "cannot reach model service", http_status=503, retryable=True) from error
    with response:
        try:
            for raw in response:              # HTTPResponse 可以按行迭代，来一行处理一行
                yield raw.decode("utf-8")
        except (ConnectionError, http.client.HTTPException, socket.timeout, TimeoutError) as error:
            # 流到一半断了：交给上层判断（已经输出内容就不能重试）
            raise GatewayError("STREAM_INTERRUPTED", "model stream was interrupted", http_status=502, retryable=True) from error
