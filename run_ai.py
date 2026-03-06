import asyncio
import argparse
import logging
import os
import sys
import time

from app.config import get_settings

# Enable ANSI colors on Windows
if os.name == "nt":
    try:
        import ctypes
        kernel32 = ctypes.windll.kernel32
        kernel32.SetConsoleMode(kernel32.GetStdHandle(-11), 7)
    except Exception:
        pass

# Force UTF-8 stdout/stderr to avoid UnicodeEncodeError when output is redirected
for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

# ANSI colors for terminal
C_RESET = "\033[0m"
C_BOLD = "\033[1m"
C_CYAN = "\033[96m"
C_YELLOW = "\033[93m"
C_GREEN = "\033[92m"
C_DIM = "\033[2m"


def _log(msg: str, color: str = "") -> None:
    print(f"{color}{msg}{C_RESET}", flush=True)
from app.db.session import SessionLocal
from app.web.routers.api_ai import (
    _build_recent_alerts_by_symbol,
    _build_funding_current_by_symbol,
    _build_market_ai_symbol_inputs,
    _resolve_market_ai_request_options,
)
from app.ai.openai_provider import OpenAICompatibleProvider
from app.ai.analyst import MarketAnalyst
from app.ai.analysis_flow import (
    build_scanner_hold_signal,
    prepare_context_and_snapshots,
    scanner_gate_passes,
)
from app.ai.thinking_summarizer import ThinkingSummarizer
from app.ai.prompts import THINKING_SUMMARY_PROMPT
from app.ai.thinking_summary_utils import (
    extract_content_text,
    infer_stage_summary,
    refine_summary,
)

# Suppress app logs for clean output（MARKET_AI_LOG_VERBOSE=true 时保留 INFO 以便诊断流式首 chunk）
_log_verbose = bool(os.environ.get("MARKET_AI_LOG_VERBOSE", "").lower() in ("true", "1", "yes"))
logging.getLogger("app").setLevel(logging.INFO if _log_verbose else logging.WARNING)


async def _summarize_and_print(
    summarizer: ThinkingSummarizer,
    buffer_content: str,
    fast_provider: OpenAICompatibleProvider,
    summaries_collected: list[str],
    summary_task_ref: list,
    content_started_ref: list,
) -> None:
    """副模型唯一使命：在主模型思考时给出阶段性反馈（实时打印）。"""
    try:
        is_first = len(summaries_collected) == 0
        max_chars = 50 if is_first else 300
        buf = buffer_content[-max_chars:] if len(buffer_content) > max_chars else buffer_content
        if len(buf.strip()) < 15:
            return
        content = THINKING_SUMMARY_PROMPT.format(buffer_content=buf)
        _log(f"\n[副模型] 完整输入 ({len(content)} 字符):\n{content}\n", C_CYAN)
        t0 = time.perf_counter()
        resp = await fast_provider.generate_response(
            messages=[{"role": "user", "content": content}],
            max_tokens=128,
            temperature=0.3,
            use_reasoning=False,
        )
        elapsed_ms = (time.perf_counter() - t0) * 1000
        _log(f"\n[副模型] 输入→输出 耗时: {elapsed_ms:.0f} ms\n", C_CYAN)
        summary = extract_content_text(resp)
        if not summary:
            summary = infer_stage_summary(buffer_content)
        else:
            summary = refine_summary(summary, buffer_content) or summary
        if not summary:
            buf = (buffer_content or "").strip()
            for sep in ("。", "？", "！"):
                parts = buf.split(sep)
                if len(parts) >= 2:
                    last_sent = parts[-2].strip()
                    if len(last_sent) >= 10:
                        summary = (last_sent[:47] + "…") if len(last_sent) > 50 else last_sent
                        break
            if not summary:
                summary = (buf[-60:].strip()[:47] + "…") if len(buf) > 50 else (buf[:47] or "思考中")
            if not summary or len(summary) < 8:
                return
        if summarizer.is_duplicate(summary):
            return
        summarizer.set_last_summary(summary)
        summaries_collected.append(summary)
        if not content_started_ref[0]:
            _log(f"\n  ═══ [副模型] {summary} ═══\n", C_BOLD + C_CYAN)
    except Exception as e:
        _log(f"\n  [副模型总结失败] {e}", C_YELLOW)
    finally:
        summary_task_ref[0] = None


DEFAULT_MODEL = "nvidia_nim/kimi-k2.5"


def _select_model_interactive(settings) -> str:
    """终端交互选择模型，返回 model id。"""
    catalog = list(settings.llm_model_catalog)
    if not catalog:
        from app.config import MODEL_CATALOG
        catalog = [{"id": item["id"], "label": item.get("label", item["id"])} for item in MODEL_CATALOG]

    default = DEFAULT_MODEL if DEFAULT_MODEL in settings.allowed_llm_models else (catalog[0]["id"] if catalog else DEFAULT_MODEL)
    if catalog and default not in {c.get("id") for c in catalog}:
        default = catalog[0]["id"]

    _log("\n可用模型:", C_BOLD)
    for i, item in enumerate(catalog, 1):
        mid = item.get("id", "")
        label = item.get("label", mid)
        mark = " [默认]" if mid == default else ""
        _log(f"  {i}. {label}{mark}", C_DIM)
    _log(f"\n直接回车 = {default}", C_DIM)
    try:
        raw = input("\n请输入数字选择: ").strip()
    except (EOFError, KeyboardInterrupt):
        return default
    if not raw:
        return default
    try:
        idx = int(raw)
        if 1 <= idx <= len(catalog):
            return catalog[idx - 1]["id"]
    except ValueError:
        pass
    _log(f"无效输入，使用默认 {default}", C_YELLOW)
    return default


async def main():
    parser = argparse.ArgumentParser(description="Standalone AI Analysis Test")
    parser.add_argument("--symbol", type=str, default="BTCUSDT", help="Symbol to analyze")
    parser.add_argument("--model", type=str, default=None, help="Override model name (不指定则交互选择)")
    args = parser.parse_args()

    settings = get_settings()
    symbol = args.symbol.upper()

    model = args.model
    if not model or not model.strip():
        model = _select_model_interactive(settings)

    try:
        request_opts = _resolve_market_ai_request_options(model)
        market_config = request_opts.llm_config
    except Exception as e:
        detail = getattr(e, "detail", str(e))
        _log(f"解析 market 配置失败: {detail}", C_YELLOW)
        return

    if not market_config.enabled or not market_config.api_key:
        _log("Market AI is disabled or API key is missing.", C_YELLOW)
        return

    _log(f"Initializing AI analysis for {symbol} using model {market_config.model}...", C_GREEN)

    provider = OpenAICompatibleProvider(market_config)
    analyst = MarketAnalyst(settings, provider, market_config)

    # Fast provider for thinking summary (副模型)
    thinking_summary_enabled = bool(getattr(settings, "ai_thinking_summary_enabled", True))
    fast_provider = None
    if thinking_summary_enabled:
        try:
            fast_config = settings.resolve_llm_config(
                str(getattr(settings, "ai_thinking_summary_profile", "thinking_summary"))
            )
            if fast_config.enabled and fast_config.api_key:
                fast_provider = OpenAICompatibleProvider(fast_config)
                _log(f"  副模型已启用: {fast_config.model}", C_DIM)
            else:
                _log("  副模型未配置 (profile 未启用或缺少 API key)", C_YELLOW)
        except Exception as e:
            _log(f"  副模型加载失败: {e}", C_YELLOW)
    else:
        _log("  副模型已禁用 (ai_thinking_summary_enabled=false)", C_DIM)

    # 与主程序一致：仅当副模型就绪时使用 typed callback
    use_typed = thinking_summary_enabled and fast_provider is not None
    summarizer = (
        ThinkingSummarizer(
            min_chars=int(getattr(settings, "ai_thinking_summary_min_chars", 100) or 100),
            min_chars_first=int(getattr(settings, "ai_thinking_summary_min_chars_first", 30) or 30),
            min_interval_sec=float(getattr(settings, "ai_thinking_summary_interval_sec", 6.0) or 6.0),
        )
        if use_typed
        else None
    )
    max_streaming_summaries = int(getattr(settings, "ai_thinking_summary_max_streaming", 15) or 15)
    reasoning_buffer: list[str] = []
    summaries_collected: list[str] = []
    summary_task_ref: list = [None]
    reasoning_chunk_count = [0]
    content_started = [False]

    _log("Fetching contexts from DB...", C_DIM)
    with SessionLocal() as db:
        recent_alerts_by_symbol = _build_recent_alerts_by_symbol(db, limit=60)
        funding_current_by_symbol = _build_funding_current_by_symbol(db)
        snapshots, context = _build_market_ai_symbol_inputs(
            db,
            symbol,
            recent_alerts_by_symbol=recent_alerts_by_symbol,
            funding_current_by_symbol=funding_current_by_symbol,
        )

    if not snapshots:
        _log(f"No market snapshots found for {symbol}. Run the worker first to collect data.", C_YELLOW)
        return

    # 与主程序一致：Scanner Gate 检查
    two_stage = bool(getattr(settings, "ai_two_stage_enabled", True))
    scan_thresh = int(getattr(settings, "ai_scan_confidence_threshold", 60) or 60)
    gate_ok, skip_reason = scanner_gate_passes(
        context,
        two_stage_enabled=two_stage,
        scan_threshold=scan_thresh,
    )
    if not gate_ok and skip_reason:
        _log(f"Scanner Gate 未通过: {skip_reason}，直接 HOLD", C_DIM)
        signals = [build_scanner_hold_signal(symbol, context, skip_reason)]
        _log("\n========== Analysis Completed (Scanner Skip) ==========", C_BOLD + C_GREEN)
        for sig in signals:
            _log(f"Signal: {sig.direction} (Confidence: {sig.confidence})", C_GREEN)
            print(f"Reasoning: {sig.reasoning}")
        return

    _log("Preparing context and snapshots...", C_DIM)
    context, snapshots = prepare_context_and_snapshots(
        context,
        snapshots,
        symbol,
        min_context_on_poor_data=bool(getattr(settings, "ai_min_context_on_poor_data", True)),
        min_context_on_non_tradeable=bool(getattr(settings, "ai_min_context_on_non_tradeable", True)),
    )

    _log("Starting analysis...", C_GREEN)
    if use_typed and fast_provider:
        _log("  (思考过程实时显示，副模型在思考阶段给出阶段性反馈)", C_DIM)
    elif not fast_provider:
        _log("  (副模型未就绪，仅显示主模型输出)", C_YELLOW)
    timeout_s = float(getattr(settings, "market_ai_stream_symbol_timeout_seconds", 420.0) or 420.0)
    model_l = (market_config.model or "").lower()
    if "kimi" in model_l or "k2.5" in model_l or "k2-" in model_l:
        timeout_s = max(timeout_s, 420.0)
        _log(f"  (大模型 Kimi K2.5 等首 token 较慢，超时已设为 {int(timeout_s)}s，请耐心等待)", C_DIM)
    _log("  等待模型响应（大模型可能需要 1-3 分钟才输出首字）...", C_DIM)
    print()

    async def stream_callback(*args):
        if len(args) == 2:
            chunk_type, text = args[0], args[1]
        else:
            chunk_type, text = "content", (args[0] if args else "")
        if chunk_type == "reasoning":
            reasoning_buffer.append(text)
            reasoning_chunk_count[0] += 1
            if reasoning_chunk_count[0] == 1:
                _log("---------- 思考过程 ----------", C_DIM)
            print(f"{C_DIM}{text}{C_RESET}", end="", flush=True)
            if summarizer and fast_provider and len(summaries_collected) < max_streaming_summaries:
                triggered = summarizer.add_reasoning(text)
                if triggered:
                    buf = summarizer.get_buffer_for_summary()
                    running = summary_task_ref[0]
                    if buf and (running is None or running.done()):
                        summary_task_ref[0] = asyncio.create_task(
                            _summarize_and_print(
                                summarizer, buf, fast_provider, summaries_collected, summary_task_ref, content_started
                            )
                        )
        else:
            if not content_started[0]:
                content_started[0] = True
                _log("\n\n---------- 主模型输出 ----------", C_BOLD + C_GREEN)
            print(text, end="", flush=True)

    try:
        signals, metadata = await asyncio.wait_for(
            analyst.analyze(
                symbol,
                snapshots,
                context=context,
                stream_callback=stream_callback,
                stream_callback_typed=use_typed,
            ),
            timeout=timeout_s,
        )
        # Wait for any pending summary task
        if summary_task_ref[0] and not summary_task_ref[0].done():
            await summary_task_ref[0]

        full_reasoning = "".join(reasoning_buffer)
        reasoning_from_stream = bool(full_reasoning.strip())
        if not full_reasoning.strip() and metadata:
            full_reasoning = (metadata.get("reasoning_content") or "").strip()
        if not full_reasoning.strip() and signals:
            meta = getattr(signals[0], "analysis_json", None) or {}
            meta = meta.get("meta") if isinstance(meta, dict) else {}
            full_reasoning = (meta.get("reasoning_content") or "").strip()

        _log("\n\n========== Analysis Completed ==========", C_BOLD + C_GREEN)
        for sig in signals:
            _log(f"Signal: {sig.direction} (Confidence: {sig.confidence})", C_GREEN)
            print(f"Reasoning: {sig.reasoning}")
            if sig.validation_warnings:
                _log(f"Warnings: {sig.validation_warnings}", C_YELLOW)
            print(f"Action Limits: Entry={sig.entry_price}, TP={sig.take_profit}, SL={sig.stop_loss}")

        if full_reasoning.strip() and not reasoning_from_stream:
            _log("\n---------- 思考过程 ----------", C_DIM)
            print(f"{C_DIM}{full_reasoning.strip()}{C_RESET}")
            _log("-------------------------------", C_DIM)

        if metadata:
            meta_short = {k: v for k, v in metadata.items() if k not in ("failure_events", "reasoning_content")}
            _log(f"\nMetadata: {meta_short}", C_DIM)
    except asyncio.TimeoutError:
        _log(f"\nAnalysis failed: timeout after {int(timeout_s)}s", C_YELLOW)
        return
    except Exception as e:
        _log(f"\nAnalysis failed: {e}", C_YELLOW)
        return

if __name__ == "__main__":
    asyncio.run(main())
