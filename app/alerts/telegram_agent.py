from __future__ import annotations

import json
from typing import Any
from datetime import datetime, timezone
import logging

from app.db.session import SessionLocal
from app.db.repository import (
    get_or_create_telegram_session,
    insert_telegram_message_log,
    get_recent_telegram_messages,
    update_telegram_session,
)
from app.ai.provider import LLMProvider, LLMRateLimitError, LLMTimeoutError
from app.ai.prompts import TELEGRAM_AGENT_PROMPT
from app.agents.tools import agent_tools, ToolCategory

logger = logging.getLogger(__name__)


def _tg_trace(msg: str, *args) -> None:
    logger.warning("[TG交互追踪][agent] " + msg, *args)


class TelegramAgent:
    """
    Manages interactive Telegram LLM conversations.
    Responsible for:
    - Loading short-term chat history (last N=10 interactions)
    - Enforcing strictly separated Read and Action tools
    - Recording token usage and estimating costs
    - Preventing hallucinative outputs via Prompt Engineering
    """

    def __init__(self, provider: LLMProvider, max_history: int = 10):
        self.provider = provider
        self.max_history = max_history

    async def chat(self, chat_id: int, user_message: str) -> dict[str, Any]:
        """
        Process a user message from Telegram, optionally calling tools, and return the final AI response.
        """
        # 1. Fetch/Create Session
        _tg_trace("进入TelegramAgent.chat chat_id=%s 用户消息长度=%d", chat_id, len(user_message or ""))
        with SessionLocal() as db:
             session_obj, _ = get_or_create_telegram_session(db, chat_id)
             summary_context = session_obj.summary_context
             preferred_model = session_obj.preferred_model_override or getattr(self.provider, "model", "default")
             
             # Overwrite provider model if dynamic override exists (simplified for now)
             if hasattr(self.provider, "model") and session_obj.preferred_model_override:
                 self.provider.model = session_obj.preferred_model_override

             # Fetch recent history
             history_logs = get_recent_telegram_messages(db, chat_id, limit=self.max_history)

        _tg_trace(
            "会话加载完成 chat_id=%s 历史消息数=%d summary_present=%s 当前provider_model=%s",
            chat_id,
            len(history_logs),
            bool(summary_context),
            getattr(self.provider, "model", "default"),
        )

        # 2. Build Messages Array
        messages = [{"role": "system", "content": TELEGRAM_AGENT_PROMPT}]
        
        # Inject Summary as System Context if available
        if summary_context:
            messages.append({"role": "system", "content": f"Previous conversation summary: {summary_context}"})

        # Inject History
        for log in history_logs:
            # We skip storing raw technical tool payloads in DB to save space, just user/assist
            if log.role in ("user", "assistant"):
                messages.append({"role": log.role, "content": log.content})

        # Inject Current Request
        messages.append({"role": "user", "content": user_message})

        # 3. Pull Available Tools (Read-only for now)
        # Action tools require the state machine interception logic which we build in Phase 3
        available_schemas = agent_tools.get_all_schemas(categories=[ToolCategory.READ_ONLY])
        _tg_trace(
            "准备调用LLM chat_id=%s messages=%d tools=%d",
            chat_id,
            len(messages),
            len(available_schemas),
        )

        # Execute LLM Call
        response = await self._run_llm_cycle(chat_id, messages, available_schemas)

        final_content = response.get("content", "")
        if not final_content:
            final_content = "抱歉，由于模型错误，我无法提供完整的回答。"
        _tg_trace(
            "LLM返回完成 chat_id=%s 回复长度=%d model=%s prompt_tokens=%s completion_tokens=%s",
            chat_id,
            len(final_content),
            response.get("model", ""),
            response.get("prompt_tokens"),
            response.get("completion_tokens"),
        )

        # 4. Save to Database for Audit and Memory
        with SessionLocal() as db:
            # Save User Request
            insert_telegram_message_log(db, {
                "chat_id": chat_id,
                "role": "user",
                "content": user_message,
                "model_used": None,
            })
            # Save Assistant Response
            insert_telegram_message_log(db, {
                "chat_id": chat_id,
                "role": "assistant",
                "content": final_content,
                "model_used": response.get("model", ""),
                "prompt_tokens": response.get("prompt_tokens", 0),
                "completion_tokens": response.get("completion_tokens", 0),
                "duration_ms": response.get("duration_ms", 0),
                "tool_calls_json": response.get("tool_calls", []),
            })

            # TODO: Phase 2.2 Cost Threshold and Summary Check logic will hook here

        return {
            "text": final_content,
            "cost": response.get("cost"),
            "model": response.get("model", "")
        }

    async def _run_llm_cycle(self, chat_id: int, messages: list[dict], tools_schema: list[dict]) -> dict:
        """Runs the LLM. If LLM requests a tool call, executes it and recurses (max 3 loops)."""
        max_loops = 3
        current_loop = 0
        
        # Track aggregated token usages
        total_prompt_tokens = 0
        total_comp_tokens = 0
        total_cost = 0.0
        
        import time
        start_time = time.perf_counter()

        while current_loop < max_loops:
            current_loop += 1
            logger.info("Agent Tool Loop [%d/%d]", current_loop, max_loops)
            _tg_trace(
                "LLM循环开始 chat_id=%s loop=%d/%d messages=%d tools=%d",
                chat_id,
                current_loop,
                max_loops,
                len(messages),
                len(tools_schema or []),
            )
            
            try:
                # Issue the API call. Wait for completion.
                response = await self.provider.generate_response(
                    messages=messages,
                    max_tokens=2048,
                    tools=tools_schema if tools_schema else None
                )
            except Exception as e:
                logger.error("LLM Generation failed: %s", e)
                _tg_trace("LLM调用异常 chat_id=%s loop=%d error=%s", chat_id, current_loop, e)
                return {"content": f"系统错误: LLM 调用失败 {str(e)}"}

            total_prompt_tokens += response.get("prompt_tokens", 0)
            total_comp_tokens += response.get("completion_tokens", 0)
            if response.get("cost"):
                total_cost += response.get("cost", 0)

            tool_calls = response.get("tool_calls", [])
            content = response.get("content", "")
            _tg_trace(
                "LLM循环返回 chat_id=%s loop=%d content_len=%d tool_calls=%d",
                chat_id,
                current_loop,
                len(content or ""),
                len(tool_calls or []),
            )
            
            if not tool_calls:
                # Normal completion without wanting tools
                response["prompt_tokens"] = total_prompt_tokens
                response["completion_tokens"] = total_comp_tokens
                response["duration_ms"] = int((time.perf_counter() - start_time) * 1000)
                response["cost"] = total_cost
                return response
                
            # LLM requested Tool Calls
            # Append AI's own request message to history (OpenAI spec)
            ai_msg = {"role": "assistant", "content": content or ""}
            openai_formatted_tool_calls = []
            
            for tc in tool_calls:
                openai_formatted_tool_calls.append({
                    "id": tc["id"],
                    "type": "function",
                    "function": {
                        "name": tc["function"]["name"],
                        "arguments": tc["function"]["arguments"]
                    }
                })
            
            ai_msg["tool_calls"] = openai_formatted_tool_calls
            messages.append(ai_msg)

            # Execute each requested tool and append as tool responses
            for tc in tool_calls:
                func_name = tc["function"]["name"]
                args_str = tc["function"]["arguments"]
                
                try:
                    args_dict = json.loads(args_str) if args_str else {}
                    args_dict["_meta_chat_id"] = chat_id 
                    
                    tool_obj = agent_tools.get_tool(func_name)
                    if not tool_obj:
                        tool_res = json.dumps({"error": f"Tool '{func_name}' is not registered."})
                    else:
                        logger.info("Agent requested tool '%s' with args %s", func_name, args_str)
                        _tg_trace("执行工具 chat_id=%s tool=%s", chat_id, func_name)
                        tool_res = tool_obj.func(args_dict)
                        if not isinstance(tool_res, str):
                            tool_res = json.dumps(tool_res, default=str)
                            
                except Exception as ex:
                    logger.error("Error executing tool %s: %s", func_name, ex)
                    _tg_trace("工具执行异常 chat_id=%s tool=%s error=%s", chat_id, func_name, ex)
                    tool_res = json.dumps({"error": f"Tool execution failed: {str(ex)}"})

                messages.append({
                    "role": "tool",
                    "tool_call_id": tc["id"],
                    "name": func_name,
                    "content": tool_res
                })

        # Loop exhausted
        return {
            "content": "警告: 工具检索达到深度限制。这是当前能够确认的最终结论。",
            "prompt_tokens": total_prompt_tokens,
            "completion_tokens": total_comp_tokens,
            "duration_ms": int((time.perf_counter() - start_time) * 1000),
            "cost": total_cost
        }
