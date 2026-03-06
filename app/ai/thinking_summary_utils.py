"""Shared utilities for thinking summary (副模型阶段性反馈)."""

import re


def extract_from_thinking_blocks(text: str) -> str:
    """当 strip 后为空时，从 <think> 或 <thinking> 块中提取内容作为 fallback。"""
    if not text or not isinstance(text, str):
        return ""
    for pattern in (
        r"<thinking>(.*?)</thinking>",
        r"<think>(.*?)</think>",
    ):
        m = re.search(pattern, text, re.DOTALL)
        if m and m.group(1).strip():
            return m.group(1).strip()
    m = re.search(r"<thinking>(.*)$", text, re.DOTALL)
    if m and m.group(1).strip():
        return m.group(1).strip()
    m = re.search(r"<think>(.*)$", text, re.DOTALL)
    if m and m.group(1).strip():
        return m.group(1).strip()
    return ""


def strip_think_tags(text: str, *, keep_explicit_cot: bool = False) -> str:
    """Remove <think>...</think> blocks and think> prefix from content. Hides raw thinking from user-facing output.
    keep_explicit_cot: when True (e.g. Nemotron), preserve ALL thinking blocks so user sees full CoT."""
    if not text or not isinstance(text, str):
        return text or ""
    if keep_explicit_cot:
        return text.strip()
    # 1. Remove <think>...</think> blocks (standard format)
    text = re.sub(r"<think>.*?</think>", "", text, flags=re.DOTALL)
    # 2. Remove unclosed <think>... at end (some models truncate)
    text = re.sub(r"<think>.*$", "", text, flags=re.DOTALL)
    # 3. Remove leading think> (Kimi etc. use this as response prefix after thinking)
    text = re.sub(r"^\s*think>\s*", "", text, flags=re.IGNORECASE)
    # 4. Remove <thinking>...</thinking> (Nemotron explicit CoT)
    text = re.sub(r"<thinking>.*?</thinking>", "", text, flags=re.DOTALL)
    text = re.sub(r"<thinking>.*$", "", text, flags=re.DOTALL)
    return text.strip()


def infer_stage_summary(text: str) -> str:
    """根据关键词推断分析阶段，用于副模型返回空时的 fallback。输出通俗中文，避免英文术语。"""
    t = (text or "")[-400:]
    if "整理成" in t or "合法的JSON" in t or "整合" in t or "不要有多余" in t:
        return "正在整理输出"
    if ("检查" in t or "验证" in t) and ("符合" in t or "要求" in t or "字段" in t):
        return "正在核对数据"
    if "anchors" in t and ("path" in t or "value" in t):
        return "正在核对引用来源"
    if "evidence" in t and ("timeframe" in t or "point" in t or "metrics" in t):
        return "正在整理依据"
    if "levels" in t and ("supports" in t or "resistances" in t):
        return "正在整理支撑阻力"
    if "risk" in t and ("invalidations" in t or "rr" in t):
        return "正在整理风险"
    if "scenarios" in t and ("base" in t or "bull" in t or "bear" in t):
        return "正在整理情景"
    if "youtube_reflection" in t or "validation_notes" in t:
        return "正在整理验证信息"
    if "trade_plan" in t and ("expiration" in t or "margin_mode" in t):
        return "正在整理交易计划"
    if "signal" in t and ("direction" in t or "confidence" in t):
        return "正在分析交易信号"
    if "market_regime" in t:
        return "正在判断市场状态"
    return ""


def extract_content_text(resp: dict) -> str:
    """从 API 响应中提取文本，兼容 content 为 str 或 list[dict] 格式。"""
    raw = resp.get("content")
    if raw is None:
        return ""
    if isinstance(raw, str):
        return raw.strip()
    if isinstance(raw, list):
        parts = []
        for block in raw:
            if isinstance(block, dict):
                t = block.get("type")
                text = block.get("text") or block.get("content")
                if text and isinstance(text, str):
                    parts.append(text)
                elif t == "text" and "text" in block:
                    parts.append(str(block["text"]))
            elif hasattr(block, "text"):
                parts.append(str(getattr(block, "text", "")))
        return " ".join(parts).strip()
    return str(raw).strip()


def refine_summary(summary: str, buffer_content: str, infer_fn=None) -> str:
    """
    若副模型返回的是技术片段（含 path、value 等），用阶段推断替换。
    返回 refined summary，无法推断时返回原 summary。
    """
    if infer_fn is None:
        infer_fn = infer_stage_summary
    tech_terms = ("：", "path", "value", "facts.", "metrics", "margin_mode", "trade_plan", "anchors", "evidence", "signal")
    if any(x in summary for x in tech_terms):
        inferred = infer_fn(buffer_content)
        if inferred:
            return inferred
    return summary
