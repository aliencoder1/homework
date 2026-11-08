"""第 7 步：韧性 —— 指数退避重试 + 按模型限流（15.8 的简化版）。

重试规则：
    - 只重试 retryable=True 的错误（5xx、超时、上游 429、OUTPUT_INVALID…）；401/400 重试也没用
    - 最多 3 次（含第 1 次）
    - 等待 = random(0, min(cap, base × 2^(n-1)))  —— Full Jitter，防止大家同时醒来一起打（惊群）
    - 流式：只在第一个字发出去之前重试；发出去之后不能重来（15.4）

限流规则（每个逻辑模型各一份）：
    - 令牌桶管“速率”：每秒补 rate 个令牌，最多存 burst 个
    - 并发上限管“同时在跑几个”：用完要还（with / finally）
    - 超限立刻返回 429 RATE_LIMITED，不排队（排队会把延迟藏起来）
"""
import random
import threading
import time
from dataclasses import dataclass

from .errors import GatewayError


@dataclass(frozen=True)
class RetryPolicy:
    max_attempts: int = 3
    base_seconds: float = 0.5
    cap_seconds: float = 4.0

    def delay(self, attempt, rng=random.random):
        """第 attempt 次失败后要等多久（attempt 从 1 开始）。"""
        ceiling = min(self.cap_seconds, self.base_seconds * 2 ** (attempt - 1))
        return rng() * ceiling


def call_with_retry(fn, policy, *, sleep=time.sleep, rng=random.random, on_error=None):
    """调用 fn()，可重试错误按退避重试。返回 (结果, 实际尝试次数)。"""
    for attempt in range(1, policy.max_attempts + 1):
        try:
            return fn(), attempt
        except GatewayError as error:
            if on_error:
                on_error(attempt, error)
            if not error.retryable or attempt == policy.max_attempts:
                error.attempts = attempt
                raise
            sleep(policy.delay(attempt, rng))


@dataclass(frozen=True)
class LimitConfig:
    rate_per_second: float     # 令牌桶补充速率
    burst: int                 # 桶容量（允许的瞬时突发）
    max_concurrency: int       # 同时在跑的上限


class TokenBucket:
    def __init__(self, rate, capacity, clock=time.monotonic):
        self.rate, self.capacity, self.clock = rate, capacity, clock
        self.tokens = float(capacity)
        self.updated = clock()
        self.lock = threading.Lock()

    def try_take(self):
        """拿到令牌返回 0；拿不到返回还要等几秒（用来填 Retry-After）。"""
        with self.lock:
            now = self.clock()
            self.tokens = min(self.capacity, self.tokens + (now - self.updated) * self.rate)
            self.updated = now
            if self.tokens >= 1:
                self.tokens -= 1
                return 0.0
            return (1 - self.tokens) / self.rate


class Permit:
    """一次准入凭证：release() 可以重复调用，只会真的还一次座位。"""

    def __init__(self, semaphore):
        self.semaphore = semaphore
        self.released = False
        self.lock = threading.Lock()

    def release(self):
        with self.lock:
            if not self.released:
                self.released = True
                if self.semaphore is not None:       # None = 这个模型没配限流，没占座位
                    self.semaphore.release()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.release()


class ModelLimiter:
    def __init__(self, limits, clock=time.monotonic):
        self.buckets = {m: TokenBucket(c.rate_per_second, c.burst, clock) for m, c in limits.items()}
        self.seats = {m: threading.BoundedSemaphore(c.max_concurrency) for m, c in limits.items()}

    def admit(self, model):
        """准入：先看并发座位，再拿令牌。任一不满足 → 429。"""
        seat = self.seats.get(model)
        if seat is None:                      # 没配限流的模型不限
            return Permit(None)
        if not seat.acquire(blocking=False):
            raise rate_limited(model, "too many concurrent requests", 1)
        wait = self.buckets[model].try_take()
        if wait > 0:
            seat.release()                    # 没拿到令牌，座位要还回去
            raise rate_limited(model, "too many requests per second", wait)
        return Permit(seat)


def rate_limited(model, reason, wait):
    return GatewayError("RATE_LIMITED", f"{model}: {reason}", http_status=429, retryable=True,
                        retry_after=max(1, round(wait + 0.5)))
