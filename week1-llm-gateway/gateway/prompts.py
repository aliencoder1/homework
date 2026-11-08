"""第 6 步：Prompt 模板存储 + 变量替换 + 版本引用（15.7 的简化版）。

存储：prompts/<name>/<version>.json，一个版本一个文件，发布后不改（要改就新建 v2）。
引用：请求里写 "prompt": {"name": "translator", "version": "v1", "variables": {...}}
      版本必须写死，不支持 “latest”——否则同一个请求今天和明天可能拿到不同的 Prompt。
渲染：string.Template.substitute，缺变量直接报错（相当于 15.7 的 StrictUndefined），
      而且是在调模型**之前**报错，不花钱。
追踪：每个版本算 content_hash，响应和日志里都带上 name / version / content_hash。
"""
import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from string import Template

from .errors import GatewayError

PROMPTS_DIR = Path(__file__).resolve().parent.parent / "prompts"
TYPES = {"string": str, "integer": int, "number": (int, float), "boolean": bool}


@dataclass(frozen=True)
class PromptVersion:
    name: str
    version: str
    template: str
    variables: dict          # 变量名 → 类型（就是这个模板的输入 Schema）
    description: str
    content_hash: str

    def ref(self):
        return {"name": self.name, "version": self.version, "content_hash": self.content_hash}


def content_hash(template, variables):
    # 只对“影响模型行为”的内容取 hash；description 改了不算新内容
    canonical = json.dumps({"template": template, "variables": variables}, sort_keys=True, ensure_ascii=False)
    return "sha256:" + hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:16]


class PromptRegistry:
    def __init__(self, directory=PROMPTS_DIR):
        self.versions = {}
        for path in sorted(Path(directory).glob("*/*.json")):
            data = json.loads(path.read_text("utf-8"))
            if (data["name"], data["version"]) != (path.parent.name, path.stem):
                raise ValueError(f"{path}: name/version must match folder/file name")   # 启动时就拒绝，不带病上线
            unknown = [t for t in data["variables"].values() if t not in TYPES]
            if unknown:
                raise ValueError(f"{path}: unknown variable type {unknown[0]}")
            try:   # 模板里用到的变量必须都声明过：用占位值试渲染一次
                Template(data["template"]).substitute({k: "x" for k in data["variables"]})
            except (KeyError, ValueError) as error:
                raise ValueError(f"{path}: template uses undeclared or bad placeholder {error}")
            self.versions[(data["name"], data["version"])] = PromptVersion(
                data["name"], data["version"], data["template"], data["variables"],
                data.get("description", ""), content_hash(data["template"], data["variables"]))

    def get(self, name, version):
        found = self.versions.get((name, version))
        if found is None:
            raise GatewayError("PROMPT_NOT_FOUND", f"prompt {name}@{version} not found", http_status=404)
        return found

    def list(self):
        return [{**p.ref(), "variables": p.variables, "description": p.description}
                for p in self.versions.values()]

    def render(self, ref):
        """请求里的 prompt 字段 → (渲染后的文本, PromptVersion)。先校验变量，再渲染。"""
        if not isinstance(ref, dict) or not isinstance(ref.get("name"), str) or not isinstance(ref.get("version"), str):
            raise GatewayError("INVALID_REQUEST", "prompt needs name and version (strings)", http_status=400)
        prompt = self.get(ref["name"], ref["version"])
        variables = ref.get("variables") or {}
        if not isinstance(variables, dict):
            raise invalid("variables must be an object")
        missing = [k for k in prompt.variables if k not in variables]
        extra = [k for k in variables if k not in prompt.variables]
        if missing:
            raise invalid(f"missing variable: {missing[0]}")
        if extra:
            raise invalid(f"unknown variable: {extra[0]}")     # 多传的变量多半是拼错了名字
        for key, kind in prompt.variables.items():
            value = variables[key]
            if not isinstance(value, TYPES[kind]) or (kind != "boolean" and isinstance(value, bool)):
                raise invalid(f"variable {key} should be {kind}")
        return Template(prompt.template).substitute({k: str(v) for k, v in variables.items()}), prompt


def invalid(message):
    return GatewayError("PROMPT_VARIABLES_INVALID", message, http_status=400)
