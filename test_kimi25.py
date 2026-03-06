import logging
import httpx
import asyncio
import os
import sys

# add parent dir so app package can be imported
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from app.config import get_settings
from app.ai.openai_provider import OpenAICompatibleProvider


async def main():
    settings = get_settings()
    config = settings.resolve_llm_config("nvidia_nim")
    config.model = "nvidia_nim/kimi-k2.5"
    
    print(f"Testing model {config.model} on {config.provider}")
    provider = OpenAICompatibleProvider(config)
    
    messages = [{"role": "user", "content": "How many 'r' in strawberry?"}]

    async def cb(*args):
        if len(args) == 2:
            print(f"[{args[0]}] {args[1]}", end="", flush=True)
        else:
            print(f"[text] {args[0]}", end="", flush=True)

    print("Sending request...")
    res = await provider.generate_response(
        messages=messages,
        use_reasoning=False,
        stream_callback=cb,
        stream_callback_typed=True
    )
    print("\nDone")

if __name__ == "__main__":
    asyncio.run(main())
