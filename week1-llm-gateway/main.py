"""启动 Gateway：python main.py  （默认 http://127.0.0.1:8000）

POST /v1/chat/completions   统一调用（stream=true 时返回 SSE）
GET  /v1/stats              按模型汇总：Token 分类、P50/P95 延迟和首字时间、错误码（第 8 步）
GET  /v1/prompts            已注册的 Prompt 模板和版本（第 6 步）
GET  /healthz               健康检查
"""
import json
import os
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from gateway.config import load_dotenv
from gateway.errors import GatewayError
from gateway.service import GatewayService, new_trace_id

SERVICE = None


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def do_GET(self):
        if self.path == "/healthz":
            return self._json(200, {"status": "ok"})
        if self.path == "/v1/stats":
            return self._json(200, SERVICE.recorder.stats())
        if self.path == "/v1/prompts":
            return self._json(200, {"prompts": SERVICE.registry.list()})
        self._json(404, {"error": {"code": "NOT_FOUND", "message": "no such path"}})

    def do_POST(self):
        trace_id = new_trace_id()
        if self.path != "/v1/chat/completions":
            return self._json(404, {"error": {"code": "NOT_FOUND", "message": "no such path", "trace_id": trace_id}})
        try:
            length = int(self.headers.get("Content-Length") or 0)
            try:
                body = json.loads(self.rfile.read(length) or b"{}")
            except (json.JSONDecodeError, UnicodeDecodeError):
                raise GatewayError("INVALID_REQUEST", "body is not valid JSON", http_status=400)

            if isinstance(body, dict) and body.get("stream"):
                handle = SERVICE.open_stream(body, trace_id)    # 先校验、路由、限流，失败还能回 4xx / 429
                try:
                    self._stream(handle)
                finally:
                    handle.close()                             # 无论如何都还限流座位
            else:
                self._json(200, SERVICE.complete(body, trace_id))
        except GatewayError as error:
            headers = {"Retry-After": str(error.retry_after)} if error.retry_after else {}
            self._json(error.http_status, error.to_body(trace_id), headers)
        except Exception:
            # 未知异常：不把内部细节给调用方
            self._json(500, {"error": {"code": "INTERNAL_ERROR", "message": "internal error", "retryable": False,
                                       "trace_id": trace_id}})

    def _json(self, status, payload, headers=None):
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        for name, value in (headers or {}).items():
            self.send_header(name, value)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _stream(self, chunks):
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream; charset=utf-8")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Connection", "close")       # 流结束就关连接，不用 Content-Length
        self.end_headers()
        try:
            for chunk in chunks:
                self.wfile.write(chunk.encode("utf-8"))
                self.wfile.flush()                    # 每块立刻发出去，不攒着（15.4：中间层不能缓冲）
        except (BrokenPipeError, ConnectionResetError):
            # 调用方断开了：关闭生成器 → 上游连接随之关闭，不再继续花钱生成
            chunks.close()
        self.close_connection = True

    def log_message(self, fmt, *args):
        sys.stderr.write("[gateway] " + fmt % args + "\n")


def main():
    global SERVICE
    load_dotenv()
    SERVICE = GatewayService()
    port = int(os.getenv("PORT", "8000"))
    server = ThreadingHTTPServer(("127.0.0.1", port), Handler)
    print(f"Gateway listening on http://127.0.0.1:{port}")
    server.serve_forever()


if __name__ == "__main__":
    main()
