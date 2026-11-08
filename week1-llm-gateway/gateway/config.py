import os
from dataclasses import dataclass
from pathlib import Path


def load_dotenv(path=Path(__file__).resolve().parent.parent / ".env"):
    """读取 .env 里的 KEY=VALUE；系统环境变量里已有的不覆盖。"""
    if not path.exists():
        return
    for line in path.read_text("utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


@dataclass(frozen=True)
class ModelRoute:
    logical_model: str     # 调用方传的名字
    protocol: str          # 用哪个 Adapter："responses" 或 "anthropic"
    provider_model: str    # 真正发给 DeepSeek 的名字
    url: str
    api_key_env: str       # 只存“去哪找 Key”，不存 Key 本身
    timeout_seconds: float = 120


# 路由表：逻辑模型名 → 协议 → 真实模型名（第 1 步定的）
ROUTES = {
    "deepseek-v4-pro": ModelRoute(
        "deepseek-v4-pro", "responses", "deepseek-v4-pro",
        "https://api.deepseek.com/responses", "DEEPSEEK_API_KEY",
    ),
    "deepseek-v4-flash": ModelRoute(
        "deepseek-v4-flash", "anthropic", "deepseek-flash",
        "https://api.deepseek.com/anthropic/v1/messages", "DEEPSEEK_API_KEY",
    ),
}


# 第 7 步：按逻辑模型限流。数字刻意比 DeepSeek 的上限（pro 500 / flash 2500 并发）小很多：
# Gateway 要先保护自己和账户余额，而不是等上游报 429。环境变量可覆盖，方便演示。
def _limit(prefix, rate, burst, concurrency):
    from .resilience import LimitConfig
    return LimitConfig(float(os.getenv(f"{prefix}_RPS", rate)), int(os.getenv(f"{prefix}_BURST", burst)),
                       int(os.getenv(f"{prefix}_CONCURRENCY", concurrency)))


def model_limits():
    return {
        "deepseek-v4-pro": _limit("PRO", 2, 4, 4),
        "deepseek-v4-flash": _limit("FLASH", 5, 10, 8),
    }


def api_key_for(route):
    key = os.getenv(route.api_key_env)
    if not key:
        raise RuntimeError(f"missing environment variable {route.api_key_env}")
    return key
