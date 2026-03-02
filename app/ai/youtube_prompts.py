"""YouTube-specific AI prompts for individual video analysis and consensus generation."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any


_BJT = timezone(timedelta(hours=8))


def _coerce_dt(value: str | datetime | None) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(text)
        except Exception:
            return None
    else:
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def _fmt_bjt_iso(value: str | datetime | None) -> str:
    dt = _coerce_dt(value)
    if not dt:
        return ""
    return dt.astimezone(_BJT).isoformat(timespec="seconds")


YOUTUBE_VIDEO_SYSTEM_PROMPT = """\
你是一位资深加密货币分析师，专门分析 YouTube 上的 BTC/ETH 行情分析视频内容。

## 你的任务
根据视频的字幕/转录文本，提取分析师的观点和交易建议，并结构化输出为高级的 VTA-JSON v1 格式。

## 提取要求
必须做到不遗漏重要信息且尽量精简。
如果信息不存在，必须保留字段并填入 `null` 或 `[]`。

## 输出格式 (VTA-JSON v1)
严格按照以下 JSON Schema 输出，不能有其他任何字符。如果提供了时间戳信息，请填入 `evidence` 对象内；否则可以为 null。

```json
{
  "vta_version": "1.0",
  "meta": {
    "analyst": "频道名称或分析师姓名",
    "publish_time_cst": "视频发布时间",
    "focus_windows": ["1H", "1D", "12H等时间框架"],
    "assets": ["BTC", "ETH"]
  },
  "market_view": {
    "bias_1_7d": "STRONG_BEAR|BEAR|NEUTRAL|BULL|STRONG_BULL",
    "bias_1_4w": "STRONG_BEAR|BEAR|NEUTRAL|BULL|STRONG_BULL",
    "conviction": "VERY_HIGH|HIGH|MEDIUM|LOW",
    "one_liner": "一句话核心结论"
  },
  "market_state": {
    "price_zone": "当前价格区间",
    "primary_timeframes": ["主时间框架"],
    "patterns": ["核心技术形态"],
    "liquidity_volume": "成交量/流动性特征"
  },
  "key_points": [
    {
      "point": "核心技术分析要点",
      "type": "PATTERN|INDICATOR|VOLUME|LEVEL|MACRO|OTHER",
      "impact": "BULL|BEAR|MIXED",
      "evidence": {"t_start": "00:00:00", "t_end": "00:00:00", "quote": "原话摘寻（必须少于20字）"}
    }
  ],
  "levels": {
    "resistance": [{"level": 68500, "note": "阻力位说明", "evidence": null}],
    "support": [{"level": 59800, "note": "支撑位说明", "evidence": null}],
    "other": [{"key": "FIB_0.236等", "level": 2129, "asset": "可选资产名"}]
  },
  "indicators": {
    "MACD": {"state": "WEAK|NEUTRAL|STRONG", "note": "描述"},
    "RSI": {"state": "OVERSOLD|NEUTRAL_WEAK|NEUTRAL|NEUTRAL_STRONG|OVERBOUGHT", "value": 42.0},
    "KDJ": {"state": "NOISY|CONFIRMING|DIVERGENT", "note": "描述"},
    "MA": {"state": "PRESSING|SUPPORTING|FLAT|CROSSING", "note": "描述"}
  },
  "trade_plans": [
    {
      "asset": "BTC交易资产",
      "direction": "LONG|SHORT",
      "style": "TREND|COUNTER_TREND|HEDGE",
      "setup": "BREAKDOWN_CONFIRM|PULLBACK_REJECT|RANGE_FADE|OTHER",
      "trigger": "触发条件",
      "entry": {"type":"MARKET|LIMIT|CONDITIONAL", "price": null, "condition": "入场点条件"},
      "stop": {"price": null, "invalidation": "止损或作废条件"},
      "targets": [{"price": 66000, "priority": 1}],
      "rules": ["NO_CHASE", "WAIT_CLOSE_CONFIRM等风控纪律"],
      "alt_scenario": "替代情景",
      "evidence": null
    }
  ],
  "risks": [
    {"text": "风险提示内容", "severity": "HIGH|MEDIUM|LOW", "evidence": null}
  ],
  "provenance": {
    "extracted_at_cst": "当前处理时间"
  }
}
```

## 约束规则
1. **数据限制**：`key_points`最多5条，`trade_plans`最多3条，`risks`最多5条。
2. **必填字段**：每个预定义的 key 必须存在，如果没有提及请使用 `null` (对象) 或 `[]` (列表)。
3. 免责声明：仅供参考，不构成投资建议。\
"""



YOUTUBE_CONSENSUS_SYSTEM_PROMPT = """\
你是一位客观的加密货币研究主管，负责汇总多位 YouTube 分析师的观点，形成共识报告。

## 你的任务
根据多位分析师基于 VTA-JSON v1 格式提取的结构化观点与评分分数（DC/PQ/VSI），综合分析后输出增强共识。

## 输出格式
严格按照以下 JSON 格式返回，不要输出任何其他内容：

```json
{
  "symbol": "BTCUSDT",
  "lookback_hours": 48,
  "consensus_bias": "STRONG_BEAR|BEAR|NEUTRAL|BULL|STRONG_BULL",
  "agreements": ["多数分析师同意的观点1", ...],
  "disagreements": [{"analyst": "名称", "view": "不同观点"}, ...],
  "key_levels": {
    "support": [综合支撑位列表],
    "resistance": [综合阻力位列表],
    "other_zones": ["关键区间"]
  },
  "scenarios": [
    {"if": "breaks_above|breaks_below|holds", "level":数字, "then": "预期走势", "risk": "风险"}
  ],
  "confidence": 0-100,
  "updated_at": "ISO时间",
  "disclaimer": "仅供参考，不构成投资建议"
}
```

## 规则
- consensus_bias: 结合多个分析师的方向和其 VSI (Video Signal Index) 分数进行加权得出。
- agreements: 最多 5 条共同观点，按重要性排序。
- disagreements: 最多 3 条分歧观点。
- scenarios: 最多 3 个情景分析。
- 如果只有 1 位分析师的数据，直接转述其观点，confidence 降低 20%。

免责声明：仅供参考，不构成投资建议。\
"""


def build_video_analysis_prompt(
    transcript: str,
    title: str,
    channel_title: str,
    published_at: str,
    symbol: str = "BTCUSDT",
) -> str:
    """Build prompt for analyzing a single video's transcript."""
    # Truncate very long transcripts to avoid token limits
    max_chars = 15000
    if len(transcript) > max_chars:
        transcript = transcript[:max_chars] + "\n\n[... 转录文本已截断 ...]"

    published_at_bjt = _fmt_bjt_iso(published_at)
    return (
        f"请分析以下 YouTube 视频中关于 {symbol} 的行情观点。\n\n"
        f"频道: {channel_title}\n"
        f"标题: {title}\n"
        f"发布时间(北京时间,+08:00): {published_at_bjt or published_at}\n\n"
        f"字幕/转录文本:\n"
        f"---\n{transcript}\n---\n\n"
        f"请严格按照 JSON 格式返回分析结果。"
    )


def build_consensus_prompt(
    insights: list[dict[str, Any]],
    symbol: str = "BTCUSDT",
    lookback_hours: int = 48,
) -> str:
    """Build prompt for generating consensus from multiple analyst views."""
    now = datetime.now(timezone.utc).astimezone(_BJT).isoformat(timespec="seconds")

    parts: list[str] = []
    parts.append(f"当前时间 (北京时间,+08:00): {now}")
    parts.append(f"目标币对: {symbol}")
    parts.append(f"回溯时间窗口: 最近 {lookback_hours} 小时")
    parts.append(f"分析师数量: {len(insights)}")
    parts.append("")
    parts.append("## 各分析师观点")
    parts.append("")

    for i, insight in enumerate(insights, 1):
        import json
        parts.append(f"### 分析师 {i}")
        parts.append(f"```json\n{json.dumps(insight, ensure_ascii=False, indent=2)}\n```")
        parts.append("")

    parts.append(f"请综合以上 {len(insights)} 位分析师的观点，生成共识报告。严格按照 JSON 格式返回。")
    return "\n".join(parts)
