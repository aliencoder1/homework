"""第 5 步：结构化输出（response_format）。

调用方用 OpenAI Chat 的写法：
    {"type": "json_object"}
    {"type": "json_schema", "json_schema": {"name": "...", "schema": {...}}}

两种协议能力不同（第 1 步对照表）：
    Responses  → 原生支持 text.format，Adapter 直接翻译过去
    Anthropic  → 不支持，只能把 Schema 写进 System Prompt 里“请求”模型照做
不管哪种，Gateway 最后都要**本地再校验一遍**：模型说“我输出了 JSON”不算数（15.5）。
"""
import json

from .errors import GatewayError

SIMPLE_TYPES = {
    "object": dict, "array": list, "string": str, "boolean": bool, "null": type(None),
}


def bad_request(message):
    return GatewayError("INVALID_REQUEST", message, http_status=400)


def normalize(response_format):
    """入口校验：把调用方的 response_format 变成统一形状；None 表示普通文本。"""
    if response_format is None:
        return None
    if not isinstance(response_format, dict):
        raise bad_request("response_format must be an object")
    kind = response_format.get("type")
    if kind == "text":
        return None
    if kind == "json_object":
        return {"type": "json_object"}
    if kind == "json_schema":
        spec = response_format.get("json_schema")
        if not isinstance(spec, dict) or not isinstance(spec.get("name"), str) or not isinstance(spec.get("schema"), dict):
            raise bad_request("json_schema needs name (string) and schema (object)")
        if spec["schema"].get("type") != "object":
            raise bad_request("json_schema.schema must describe an object")
        return {"type": "json_schema", "name": spec["name"], "schema": spec["schema"]}
    raise bad_request("response_format.type must be text, json_object or json_schema")


# ---------- 请求侧：怎么让模型输出 JSON ----------
def responses_text_format(fmt):
    """Responses 原生字段 text.format。"""
    if fmt["type"] == "json_object":
        return {"format": {"type": "json_object"}}
    return {"format": {"type": "json_schema", "name": fmt["name"], "schema": fmt["schema"]}}


def instruction(fmt, native):
    """追加到 System Prompt 的说明。native=True 表示协议原生支持，只需一句提示。"""
    if fmt["type"] == "json_object" or native:
        # json_object 模式通常要求提示词里出现 “JSON” 字样，否则上游可能拒绝
        return "Respond with a single valid JSON object only. No Markdown, no explanation."
    return ("Respond with a single valid JSON object only. No Markdown code fences, no explanation.\n"
            "The JSON must conform to this JSON Schema:\n" + json.dumps(fmt["schema"], ensure_ascii=False))


# ---------- 响应侧：本地校验 ----------
def output_invalid(message):
    # 模型这次没照做，下次可能就对了 → retryable=True，交给第 7 步的重试
    return GatewayError("OUTPUT_INVALID", f"model output is not valid JSON: {message}", http_status=502, retryable=True)


def parse_and_validate(text, fmt):
    """模型文本 → Python 对象；不合法就抛 OUTPUT_INVALID。"""
    cleaned = text.strip()
    if cleaned.startswith("```"):                      # 宽容一点：去掉 ```json ... ``` 代码块外壳
        cleaned = cleaned.split("\n", 1)[1] if "\n" in cleaned else ""
        cleaned = cleaned.rsplit("```", 1)[0].strip()
    try:
        value = json.loads(cleaned)
    except json.JSONDecodeError as error:
        raise output_invalid(f"parse error at char {error.pos}")
    if not isinstance(value, dict):
        raise output_invalid("top level must be an object")
    if fmt["type"] == "json_schema":
        problem = validate(value, fmt["schema"], "$")
        if problem:
            raise output_invalid(problem)
    return value


def type_ok(value, expected):
    if expected == "integer":
        return isinstance(value, int) and not isinstance(value, bool)
    if expected == "number":
        return isinstance(value, (int, float)) and not isinstance(value, bool)
    return isinstance(value, SIMPLE_TYPES.get(expected, object))


def validate(value, schema, path):
    """JSON Schema 的常用子集：type / properties / required / additionalProperties / enum / items。
    返回第一处问题的描述；没问题返回 None。（生产可换 jsonschema 库，这里保持零依赖。）"""
    expected = schema.get("type")
    if expected is not None:
        options = expected if isinstance(expected, list) else [expected]
        if not any(type_ok(value, t) for t in options):
            return f"{path} should be {expected}"
    if "enum" in schema and value not in schema["enum"]:
        return f"{path} should be one of {schema['enum']}"
    if isinstance(value, dict):
        props = schema.get("properties", {})
        for key in schema.get("required", []):
            if key not in value:
                return f"{path}.{key} is required"
        if schema.get("additionalProperties") is False:
            extra = [k for k in value if k not in props]
            if extra:
                return f"{path} has unexpected field {extra[0]}"
        for key, sub in props.items():
            if key in value:
                problem = validate(value[key], sub, f"{path}.{key}")
                if problem:
                    return problem
    if isinstance(value, list) and isinstance(schema.get("items"), dict):
        for i, item in enumerate(value):
            problem = validate(item, schema["items"], f"{path}[{i}]")
            if problem:
                return problem
    return None
