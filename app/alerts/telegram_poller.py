from __future__ import annotations

import asyncio
import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Awaitable, Callable

import httpx

logger = logging.getLogger(__name__)

TelegramUpdateHandler = Callable[[dict[str, Any]], Awaitable[None]]
_ALLOWED_UPDATES = ["message", "edited_message", "callback_query"]


def _tg_poll_log(level: int, msg: str, *args) -> None:
    logger.log(level, "[TG轮询] " + msg, *args)


class TelegramPoller:
    """
    Long polling via Telegram getUpdates for local/intranet development.

    Notes:
    - Requires webhook to be deleted (or empty) to receive getUpdates.
    - Intentionally advances and persists next_offset before handling an update to avoid retry loops.
      Tradeoff: a handler failure will skip that update (no automatic retry).
    """

    def __init__(
        self,
        token: str,
        handle_update: TelegramUpdateHandler,
        *,
        timeout_seconds: int = 50,
        interval_seconds: float = 0.2,
        state_file: str = "data/telegram_poller_state.json",
        auto_delete_webhook: bool = True,
        drop_pending_updates: bool = True,
    ) -> None:
        self.base = f"https://api.telegram.org/bot{token}"
        self.handle_update = handle_update
        self.timeout_seconds = max(1, int(timeout_seconds))
        self.interval_seconds = max(0.0, float(interval_seconds))
        self.state_file = state_file
        self.auto_delete_webhook = bool(auto_delete_webhook)
        self.drop_pending_updates = bool(drop_pending_updates)
        self.offset: int | None = None
        self._stop = asyncio.Event()

    def stop(self) -> None:
        self._stop.set()

    async def run_forever(self) -> None:
        self.offset = self._load_next_offset()
        _tg_poll_log(
            logging.WARNING,
            "启动轮询 timeout=%s interval=%.2f state_file=%s next_offset=%s",
            self.timeout_seconds,
            self.interval_seconds,
            self.state_file,
            self.offset,
        )

        timeout = httpx.Timeout(self.timeout_seconds + 10.0, connect=10.0)
        try:
            async with httpx.AsyncClient(timeout=timeout) as client:
                await self._bootstrap_webhook_mode(client)

                while not self._stop.is_set():
                    try:
                        params: dict[str, Any] = {
                            "timeout": self.timeout_seconds,
                            "allowed_updates": json.dumps(_ALLOWED_UPDATES, ensure_ascii=False),
                        }
                        if self.offset is not None:
                            params["offset"] = self.offset

                        response = await client.get(f"{self.base}/getUpdates", params=params)
                        response.raise_for_status()
                        data = response.json()
                        if not isinstance(data, dict):
                            _tg_poll_log(logging.WARNING, "getUpdates 返回非对象JSON，已忽略")
                            await self._sleep_interval()
                            continue

                        if not data.get("ok", False):
                            _tg_poll_log(logging.WARNING, "getUpdates 返回 ok=false: %s", data)
                            await self._sleep_interval()
                            continue

                        updates = data.get("result", [])
                        if not isinstance(updates, list):
                            _tg_poll_log(logging.WARNING, "getUpdates result 非列表，已忽略: %r", type(updates))
                            await self._sleep_interval()
                            continue

                        if updates:
                            _tg_poll_log(
                                logging.WARNING,
                                "拉取到 updates=%d 当前next_offset=%s",
                                len(updates),
                                self.offset,
                            )

                        for upd in updates:
                            if self._stop.is_set():
                                break
                            if not isinstance(upd, dict):
                                _tg_poll_log(logging.WARNING, "跳过异常update格式: %r", type(upd))
                                continue

                            uid = upd.get("update_id")
                            if isinstance(uid, int):
                                next_offset = uid + 1
                                self.offset = next_offset
                                self._save_next_offset(next_offset)
                            else:
                                next_offset = None

                            _tg_poll_log(
                                logging.WARNING,
                                "分发 update_id=%s next_offset=%s keys=%s",
                                uid,
                                next_offset,
                                list(upd.keys())[:10],
                            )
                            try:
                                await self.handle_update(upd)
                            except asyncio.CancelledError:
                                _tg_poll_log(logging.INFO, "处理 update 时收到取消信号，准备退出")
                                raise
                            except Exception:
                                logger.exception(
                                    "[TG轮询] 处理 update 失败（该条将被跳过，不会自动重试）update_id=%s",
                                    uid,
                                )

                        await self._sleep_interval()

                    except asyncio.CancelledError:
                        _tg_poll_log(logging.INFO, "收到任务取消信号，轮询正常退出")
                        break
                    except httpx.TimeoutException as exc:
                        _tg_poll_log(logging.WARNING, "网络超时: %s", exc)
                        await self._sleep_interval(on_error=True)
                    except httpx.RequestError as exc:
                        _tg_poll_log(logging.WARNING, "网络错误: %s", exc)
                        await self._sleep_interval(on_error=True)
                    except httpx.HTTPStatusError as exc:
                        body = ""
                        try:
                            body = exc.response.text
                        except Exception:
                            body = ""
                        _tg_poll_log(logging.WARNING, "HTTP错误: %s body=%s", exc, body)
                        await self._sleep_interval(on_error=True)
                    except Exception:
                        logger.exception("[TG轮询] 轮询循环出现未预期错误")
                        await self._sleep_interval(on_error=True)
        except asyncio.CancelledError:
            _tg_poll_log(logging.INFO, "run_forever 被取消，按正常退出处理")
        finally:
            _tg_poll_log(logging.INFO, "轮询已停止")

    async def _sleep_interval(self, on_error: bool = False) -> None:
        if self._stop.is_set():
            return
        delay = max(self.interval_seconds, 2.0) if on_error else self.interval_seconds
        if delay > 0:
            await asyncio.sleep(delay)

    async def _bootstrap_webhook_mode(self, client: httpx.AsyncClient) -> None:
        info_before = await self._get_webhook_info(client)
        self._log_webhook_info("启动前 Webhook 状态", info_before)

        if self.auto_delete_webhook:
            deleted = await self._delete_webhook(client, self.drop_pending_updates)
            if deleted:
                _tg_poll_log(
                    logging.WARNING,
                    "已自动调用 deleteWebhook drop_pending_updates=%s",
                    self.drop_pending_updates,
                )
            else:
                _tg_poll_log(
                    logging.WARNING,
                    "自动 deleteWebhook 失败，请检查网络/Token；若Webhook仍存在，getUpdates不会工作",
                )

        info_after = await self._get_webhook_info(client)
        self._log_webhook_info("轮询后 Webhook 状态", info_after)

        info_effective = info_after if info_after is not None else info_before
        if isinstance(info_effective, dict):
            url = str(info_effective.get("url") or "")
            pending = info_effective.get("pending_update_count")
            if url:
                _tg_poll_log(
                    logging.ERROR,
                    "Webhook 仍存在，getUpdates 不会工作；必须先 deleteWebhook。url=%s pending_update_count=%s",
                    url,
                    pending,
                )

    def _state_path(self) -> Path:
        return Path(self.state_file)

    def _load_next_offset(self) -> int | None:
        path = self._state_path()
        if not path.exists():
            _tg_poll_log(logging.WARNING, "未找到状态文件，将从当前队列开始轮询 state_file=%s", path)
            return None

        try:
            raw = path.read_text(encoding="utf-8")
            data = json.loads(raw)
            next_offset = data.get("next_offset")
            if isinstance(next_offset, int):
                _tg_poll_log(logging.WARNING, "已加载 next_offset=%s state_file=%s", next_offset, path)
                return next_offset
            _tg_poll_log(logging.WARNING, "状态文件缺少合法 next_offset，已忽略 state_file=%s", path)
            return None
        except Exception as exc:
            _tg_poll_log(logging.WARNING, "读取状态文件失败，已忽略 state_file=%s error=%s", path, exc)
            return None

    def _save_next_offset(self, next_offset: int) -> None:
        path = self._state_path()
        try:
            if path.parent and str(path.parent) not in ("", "."):
                path.parent.mkdir(parents=True, exist_ok=True)
            elif str(path.parent) == ".":
                path.parent.mkdir(parents=True, exist_ok=True)

            payload = {
                "next_offset": int(next_offset),
                "updated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            }
            tmp_path = path.with_suffix(path.suffix + ".tmp")
            with tmp_path.open("w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)
            os.replace(tmp_path, path)
        except Exception as exc:
            _tg_poll_log(logging.WARNING, "保存状态文件失败 state_file=%s error=%s", path, exc)

    async def _get_webhook_info(self, client: httpx.AsyncClient) -> dict[str, Any] | None:
        data = await self._telegram_api_json(client, "getWebhookInfo", method="GET")
        if not isinstance(data, dict):
            return None
        result = data.get("result")
        return result if isinstance(result, dict) else None

    async def _delete_webhook(self, client: httpx.AsyncClient, drop_pending_updates: bool) -> bool:
        data = await self._telegram_api_json(
            client,
            "deleteWebhook",
            method="POST",
            json_payload={"drop_pending_updates": bool(drop_pending_updates)},
        )
        return bool(isinstance(data, dict) and data.get("ok") is True)

    async def _telegram_api_json(
        self,
        client: httpx.AsyncClient,
        endpoint: str,
        *,
        method: str = "GET",
        params: dict[str, Any] | None = None,
        json_payload: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        url = f"{self.base}/{endpoint}"
        try:
            if method.upper() == "POST":
                resp = await client.post(url, params=params, json=json_payload)
            else:
                resp = await client.get(url, params=params)
            resp.raise_for_status()
            data = resp.json()
            if not isinstance(data, dict):
                _tg_poll_log(logging.WARNING, "%s 返回非对象JSON", endpoint)
                return None
            if not data.get("ok", False):
                _tg_poll_log(logging.WARNING, "%s 返回 ok=false: %s", endpoint, data)
            return data
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            _tg_poll_log(logging.WARNING, "调用 Telegram API %s 失败: %s", endpoint, exc)
            return None

    def _log_webhook_info(self, prefix: str, info: dict[str, Any] | None) -> None:
        if not info:
            _tg_poll_log(logging.WARNING, "%s: 获取失败", prefix)
            return
        _tg_poll_log(
            logging.WARNING,
            "%s url=%s pending_update_count=%s",
            prefix,
            info.get("url", ""),
            info.get("pending_update_count"),
        )
