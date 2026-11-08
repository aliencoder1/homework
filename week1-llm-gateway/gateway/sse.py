"""读 SSE（Server-Sent Events）：把一行行文本切成一帧帧事件。两种协议共用。"""
import json


def iter_sse_events(lines):
    """输入：一行一行的文本（不含换行符也行）。输出：(事件名, data 字典) 一帧一帧地产出。

    SSE 的规则：
      event: 名字      → 这一帧的事件名
      data: 内容       → 这一帧的数据（可能有多行，用换行拼起来）
      : 注释           → 心跳 / keep-alive，忽略
      空行             → 这一帧结束
    """
    event_name, data_lines = None, []
    for raw in lines:
        line = raw.rstrip("\r\n")
        if line == "":                       # 空行：一帧结束
            if data_lines:
                yield _finish(event_name, data_lines)
            event_name, data_lines = None, []
        elif line.startswith(":"):           # 注释（DeepSeek 的 : keep-alive）
            continue
        elif line.startswith("event:"):
            event_name = line[len("event:"):].strip()
        elif line.startswith("data:"):
            data_lines.append(line[len("data:"):].lstrip())
    if data_lines:                           # 最后一帧后面没有空行也不能丢
        yield _finish(event_name, data_lines)


def _finish(event_name, data_lines):
    text = "\n".join(data_lines)
    if text == "[DONE]":                     # Chat Completions 风格的结束标记（这两个协议不发，但要能认）
        return event_name or "done", None
    data = json.loads(text)
    return event_name or data.get("type"), data   # 没有 event: 行时，用 data 里的 type
