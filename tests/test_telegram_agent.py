import pytest
import json
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from app.db.models import Base
from unittest.mock import AsyncMock, MagicMock
from app.alerts.telegram_agent import TelegramAgent
from app.agents.tools.base import agent_tools, ToolCategory
from app.ai.provider import LLMProvider, LLMCapabilities

class DummyProvider(LLMProvider):
    def __init__(self):
        self.model = "dummy-model"
        self.mock_responses = []

    @property
    def capabilities(self) -> LLMCapabilities:
        return LLMCapabilities(True, True, False, False)
        
    async def generate_response(self, *args, **kwargs) -> dict:
        if self.mock_responses:
            return self.mock_responses.pop(0)
        return {"content": "default empty response"}

@pytest.fixture
def dummy_provider():
    return DummyProvider()

@pytest.fixture
def db_session():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    SessionClass = sessionmaker(bind=engine)
    session = SessionClass()
    yield session
    session.close()

@pytest.mark.anyio
async def test_telegram_agent_basic_chat(db_session, dummy_provider):
    agent = TelegramAgent(provider=dummy_provider, max_history=5)
    
    # 模拟第一次回复是纯文本，不调用工具
    dummy_provider.mock_responses = [
        {
            "content": "Hello! I am your assistant.",
            "prompt_tokens": 10,
            "completion_tokens": 5,
            "cost": 0.0001,
            "model": "dummy-model"
        }
    ]
    
    # Needs to mock SessionLocal inside agent to use our db_session
    import app.alerts.telegram_agent as agent_module
    
    class MockSessionLocal:
        def __enter__(self): return db_session
        def __exit__(self, *args): pass
        
    agent_module.SessionLocal = MockSessionLocal
    
    res = await agent.chat(chat_id=123, user_message="Hi")
    assert res["text"] == "Hello! I am your assistant."
    assert res["cost"] == 0.0001
    
    # Verify DB logging
    from app.db.models import TelegramMessageLog
    logs = db_session.query(TelegramMessageLog).filter_by(chat_id=123).all()
    assert len(logs) == 2
    assert logs[0].role == "user"
    assert logs[1].role == "assistant"
    assert logs[1].content == "Hello! I am your assistant."


@pytest.mark.anyio
async def test_telegram_agent_tool_loop(db_session, dummy_provider):
    # Register a dummy tool strictly for this test
    @agent_tools.register("test_echo_tool", "Echos back", ToolCategory.READ_ONLY, {
        "type": "object", "properties": {"word": {"type": "string"}}, "required": ["word"]
    })
    def test_echo_tool(args):
        return json.dumps({"echo_result": args.get("word")})

    agent = TelegramAgent(provider=dummy_provider, max_history=5)
    
    # 模拟请求: 第一次要求调工具，第二次正常返回结论
    dummy_provider.mock_responses = [
        {
            "content": None,
            "tool_calls": [{
                "id": "call_123",
                "type": "function",
                "function": {"name": "test_echo_tool", "arguments": '{"word": "apple"}'}
            }],
            "prompt_tokens": 20,
            "completion_tokens": 5,
        },
        {
            "content": "The tool returned apple.",
            "prompt_tokens": 30,
            "completion_tokens": 10,
            "cost": 0.001
        }
    ]
    
    import app.alerts.telegram_agent as agent_module
    class MockSessionLocal:
        def __enter__(self): return db_session
        def __exit__(self, *args): pass
    agent_module.SessionLocal = MockSessionLocal
    
    # Needs to mock to prevent missing foreign key references if relying on real DB models
    res = await agent.chat(chat_id=999, user_message="Please echo apple")
    
    # Should automatically iterate and hit loop 2
    assert res["text"] == "The tool returned apple."
    assert res["cost"] == 0.001
    
    # Verify DB contains the tool trace
    from app.db.models import TelegramMessageLog
    logs = db_session.query(TelegramMessageLog).filter_by(chat_id=999).all()
    assert len(logs) == 2
    
    ast_log = logs[1]
    assert ast_log.prompt_tokens == 50 # 20 + 30
    assert ast_log.completion_tokens == 15 # 5 + 10
    
    # 移除测试中的临时 Tool 防止污染全局
    del agent_tools._tools["test_echo_tool"]
