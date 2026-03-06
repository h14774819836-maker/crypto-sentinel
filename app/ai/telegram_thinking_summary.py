"""Telegram 对话的副模型阶段性反馈：主模型思考时调用副模型生成短摘要，通过 editMessageText 动态更新状态。"""

from __future__ import annotations

from app.ai.prompts import TELEGRAM_THINKING_SUMMARY_PROMPT
from app.ai.thinking_summary_utils import extract_content_text, refine_summary
from app.ai.thinking_summarizer import ThinkingSummarizer
from app.logging import logger


def infer_telegram_stage_summary(text: str) -> str:
    """通用对话思考阶段的 fallback 推断（非市场分析）。"""
    t = (text or "")[-200:]
    if "工具" in t or "tool" in t.lower() or "调用" in t:
        return "正在调用工具"
    if "检索" in t or "查询" in t or "search" in t.lower():
        return "正在检索信息"
    if "理解" in t or "分析" in t or "考虑" in t:
        return "正在理解问题"
    if "整理" in t or "组织" in t or "总结" in t:
        return "正在整理输出"
    if "回答" in t or "回复" in t:
        return "正在组织回答"
    return ""


async def _summarize_and_edit_telegram(
    summarizer: ThinkingSummarizer,
    buffer_content: str,
    fast_provider,
    client,
    chat_id: int,
    status_msg_id: int,
    summaries_pushed_ref: list,
    max_summaries: int,
    summary_task_ref: list,
) -> None:
    """
    副模型总结主模型思考过程，并通过 editMessageText 更新 Telegram 状态消息。
    输入更短：首次 30 字，后续 150 字。
    """
    try:
        if summaries_pushed_ref[0] >= max_summaries:
            return
        is_first = summaries_pushed_ref[0] == 0
        max_chars = 30 if is_first else 150
        buf = buffer_content[-max_chars:] if len(buffer_content) > max_chars else buffer_content
        if len(buf.strip()) < 15:
            return
        content = TELEGRAM_THINKING_SUMMARY_PROMPT.format(buffer_content=buf)
        resp = await fast_provider.generate_response(
            messages=[{"role": "user", "content": content}],
            max_tokens=64,
            temperature=0.3,
            use_reasoning=False,
        )
        summary = extract_content_text(resp)
        if not summary:
            summary = infer_telegram_stage_summary(buffer_content)
        else:
            summary = refine_summary(summary, buffer_content, infer_fn=infer_telegram_stage_summary) or summary
        if not summary:
            buf = (buffer_content or "").strip()
            for sep in ("。", "？", "！"):
                parts = buf.split(sep)
                if len(parts) >= 2:
                    last_sent = parts[-2].strip()
                    if len(last_sent) >= 8:
                        summary = (last_sent[:35] + "…") if len(last_sent) > 40 else last_sent
                        break
            if not summary:
                summary = (buf[-60:].strip()[:35] + "…") if len(buf) > 40 else (buf[:35] or "思考中")
        if not summary or len(summary) < 4:
            return
        if summarizer.is_duplicate(summary):
            return
        summarizer.set_last_summary(summary)
        summaries_pushed_ref[0] += 1
        status_text = "⏳ " + summary
        ok = await client.edit_message_text(chat_id, status_msg_id, status_text)
        if not ok:
            logger.warning("Telegram edit_message_text failed chat_id=%s msg_id=%s", chat_id, status_msg_id)
    except Exception as e:
        logger.warning("Telegram thinking summary failed: %s", e)
    finally:
        summary_task_ref[0] = None
