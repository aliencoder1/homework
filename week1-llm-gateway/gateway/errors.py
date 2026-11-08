class GatewayError(Exception):
    """统一错误：调用方只看到 code / message / retryable，看不到上游原始异常。"""

    def __init__(self, code, message, *, http_status=500, retryable=False, upstream_status=None, upstream_request_id=None, retry_after=None):
        super().__init__(message)
        self.code = code
        self.message = message
        self.http_status = http_status
        self.retryable = retryable
        # 下面两个只进内部日志 / Trace，不返回给调用方
        self.upstream_status = upstream_status
        self.upstream_request_id = upstream_request_id
        self.retry_after = retry_after   # 秒；有值时 HTTP 层加 Retry-After 头（第 7 步限流用）

    def to_body(self, trace_id):
        return {"error": {"code": self.code, "message": self.message, "retryable": self.retryable, "trace_id": trace_id}}


def map_upstream_status(status, request_id=None):
    """上游 HTTP 错误 → 统一错误码。原则：谁该去修，就用谁的错误码（15.3）。"""
    common = {"upstream_status": status, "upstream_request_id": request_id}
    if status in (401, 402, 403):
        # 我们自己的 Key / 余额问题，不是调用方的错，所以不回 401 给调用方
        return GatewayError("UPSTREAM_AUTH_FAILED", "model service account is not available", http_status=502, **common)
    if status == 429:
        return GatewayError("UPSTREAM_RATE_LIMITED", "model service is busy", http_status=503, retryable=True, **common)
    if status >= 500:
        return GatewayError("UPSTREAM_UNAVAILABLE", "model service is temporarily unavailable", http_status=503, retryable=True, **common)
    return GatewayError("UPSTREAM_REJECTED", "model service rejected the request", http_status=502, **common)


# 全部统一错误码（第 7 步）：调用方只需要按 code 分支，不用认识 DeepSeek 的原始报错
ERROR_CODES = {
    "INVALID_REQUEST":          (400, False, "请求体不合法（调用方修）"),
    "PROMPT_VARIABLES_INVALID": (400, False, "模板变量缺失 / 多余 / 类型不对（调用方修）"),
    "MODEL_NOT_FOUND":          (404, False, "逻辑模型名不存在"),
    "PROMPT_NOT_FOUND":         (404, False, "模板名或版本不存在"),
    "RATE_LIMITED":             (429, True,  "Gateway 按模型限流，稍后重试"),
    "INTERNAL_ERROR":           (500, False, "Gateway 自身 Bug"),
    "UPSTREAM_AUTH_FAILED":     (502, False, "我们的 Key / 余额问题（运维修）"),
    "UPSTREAM_REJECTED":        (502, False, "上游拒绝了请求（400/422 等）"),
    "OUTPUT_INVALID":           (502, True,  "模型输出不是合法 JSON / 不符合 Schema"),
    "STREAM_INTERRUPTED":       (502, True,  "流没有正常结束"),
    "UPSTREAM_RATE_LIMITED":    (503, True,  "上游 429"),
    "UPSTREAM_UNAVAILABLE":     (503, True,  "上游 5xx / 连接失败"),
    "UPSTREAM_TIMEOUT":         (504, True,  "上游超时"),
}
