#!/usr/bin/env python3
"""快速测试 Binance 账户 API 连接，用于排查账户监控无法连接的问题。"""
from __future__ import annotations

import asyncio
import sys
from pathlib import Path

# 确保能导入 app
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.config import Settings
from app.providers.binance_provider import BinanceProvider


async def main() -> None:
    settings = Settings()
    if not settings.binance_api_key or not settings.binance_api_secret:
        print("错误: BINANCE_API_KEY 或 BINANCE_API_SECRET 未配置")
        return

    provider = BinanceProvider(settings)
    print("正在测试 Binance 账户 API 连接...\n")

    # 1. 期货账户
    print("1. 期货账户 (fapi.binance.com)...")
    try:
        data = await provider.get_futures_account()
        bal = data.get("availableBalance") or data.get("totalMarginBalance")
        print(f"   成功! availableBalance={bal}")
    except Exception as e:
        print(f"   失败: {e}")
        resp = getattr(e, "response", None)
        if resp is not None:
            try:
                body = resp.json()
                print(f"   币安返回: code={body.get('code')} msg={body.get('msg')}")
            except Exception:
                pass

    # 2. 杠杆账户
    print("\n2. 杠杆账户 (sapi margin)...")
    try:
        data = await provider.get_margin_account()
        print(f"   成功! marginLevel={data.get('marginLevel')}")
    except Exception as e:
        print(f"   失败: {e}")
        resp = getattr(e, "response", None)
        if resp is not None:
            try:
                body = resp.json()
                print(f"   币安返回: code={body.get('code')} msg={body.get('msg')}")
            except Exception:
                pass

    # 3. ListenKey (User Stream)
    if settings.account_user_stream_enabled:
        print("\n3. User Stream ListenKey...")
        try:
            key = await provider.create_futures_listen_key()
            print(f"   成功! listenKey={key[:20]}...")
        except Exception as e:
            print(f"   失败: {e}")
            resp = getattr(e, "response", None)
            if resp is not None:
                try:
                    body = resp.json()
                    print(f"   币安返回: code={body.get('code')} msg={body.get('msg')}")
                except Exception:
                    pass

    print("\n完成。若某一步失败，请根据上面的 code/msg 调整 API Key 权限或 IP 白名单。")


if __name__ == "__main__":
    asyncio.run(main())
