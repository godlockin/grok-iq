"""Randomized keep-alive prompt construction.

Keep-alive traffic exists only to keep accounts warm. It is deliberately kept
outside the probe pipeline: no marker to satisfy, no TPS to compare, and no
sample written to ``probe_samples``. Reusing one fixed prompt every time makes
the upstream request fingerprint trivially recognizable, so every request draws
an independent topic, phrasing, length and parameter jitter.

Prompts are short, self-contained, and cheap to answer. Long generations waste
tokens and raise the chance of hitting an output cap, so the pool is bounded to
tasks whose correct answer stays small.
"""

from __future__ import annotations

import random
import uuid
from dataclasses import dataclass
from typing import Any

# Each entry is a self-contained task. The answer is intentionally short so the
# upstream does not spend extra generation time or output tokens per keep-alive.
_TOPIC_TASKS: tuple[str, ...] = (
    "用两句话解释潮汐是怎么形成的。",
    "把「延迟」这个词写一个能让小学生听懂的类比。",
    "用一句话说明为什么天空是蓝色的。",
    "给一个三分钟能做完的原地开合跳训练建议。",
    "列两条把书桌整理干净的实际步骤。",
    "解释一下为什么切洋葱会流泪。",
    "推荐一个适合下雨天听的爵士乐歌手，并说明理由。",
    "把「缓存」这个计算机术语解释给文科生听。",
    "写一句适合贴在办公室门口的简短标语。",
    "说明一下每天走三千步大概需要多长时间。",
    "用比喻解释一下网络延迟和现实中的等待有什么不同。",
    "给一个刚开始学跑步的人三条避免受伤的建议。",
    "解释海水为什么是咸的。",
    "用一句话概括图书馆安静规则的意义。",
    "描述一下雨后柏油路面的气味，并用一句话解释来源。",
    "给一本入门编程书写一句推荐语，不超过二十字。",
    "说明一下为什么冬天呼出的气看起来像白雾。",
    "提出一个减少家中塑料垃圾的简单办法。",
    "解释「边际成本递减」在生活中的一个例子。",
    "用两句话讲清楚公积金和储蓄的区别。",
    "给长途坐车的人列三条晕车缓解建议。",
    "说明为什么洗手要二十秒以上。",
    "用一句话描述秋天的第一场雨。",
    "解释一下为什么铁轨之间要留缝隙。",
    "推荐一种适合办公室桌上摆放的小植物，并说明理由。",
    "说明一下冰箱冷藏和冷冻的区别。",
    "给刚学骑自行车的人两条安全建议。",
    "用比喻解释一下「优先级」这个概念。",
    "简述一下为什么面包店常在早上五点开始工作。",
    "解释一下为什么高山上煮东西不容易熟。",
)

_OPENERS: tuple[str, ...] = (
    "",
    "简单回答，",
    "不用太展开，",
    "一句话就行，",
    "帮我快速理解：",
    "请直接给结论：",
    "简短说明一下：",
    "不用举例，",
    "控制在两三句话内，",
)

_CLOSERS: tuple[str, ...] = (
    "",
    "，谢谢。",
    "，回答简短些。",
    "，别太啰嗦。",
    "，就当是随口一问。",
    "，保持自然口语。",
)

# Roughly 0.2 - 0.9. Sampling temperature per request spreads the request
# fingerprint further and is well inside the 0-2 range grok2api accepts.
_TEMPERATURE_RANGE = (0.2, 0.9)

# Keep-alive has no correctness bar, so a hard output cap keeps every request
# cheap. A too-small cap truncates the reply; a too-large one lets the model
# ramble. This value leaves room for a short answer plus formatting.
_DEFAULT_MAX_OUTPUT_TOKENS = 220


@dataclass(frozen=True, slots=True)
class KeepAlivePrompt:
    """One fully rendered keep-alive request."""

    system_prompt: str
    prompt: str
    temperature: float
    max_output_tokens: int


def render_prompt(rng: random.Random, *, max_output_tokens: int = _DEFAULT_MAX_OUTPUT_TOKENS) -> KeepAlivePrompt:
    """Draw one randomized keep-alive request.

    ``rng`` is injected rather than using the module-level generator so tests
    can pin the output and so a caller can derive a per-account stream without
    coupling to global state.
    """

    task = rng.choice(_TOPIC_TASKS)
    opener = rng.choice(_OPENERS)
    closer = rng.choice(_CLOSERS)
    temperature = round(rng.uniform(*_TEMPERATURE_RANGE), 2)
    # A short random suffix makes otherwise-identical requests differ on the
    # wire without changing what the model is asked to do.
    prompt = f"{opener}{task}{closer}（{uuid.uuid4().hex[:8]}）"
    return KeepAlivePrompt(
        system_prompt="",
        prompt=prompt,
        temperature=temperature,
        max_output_tokens=int(max_output_tokens),
    )
