import pytest
from datetime import datetime, timezone
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from app.db.models import TelegramSession, TelegramMessageLog, Base
from app.db.repository import (
    get_or_create_telegram_session,
    update_telegram_session,
    insert_telegram_message_log,
    get_recent_telegram_messages
)

@pytest.fixture
def db_session():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    SessionClass = sessionmaker(bind=engine)
    session = SessionClass()
    yield session
    session.close()

def test_telegram_session_creation(db_session):
    chat_id = 123456789
    
    # 1. Test Creation
    session_obj, created = get_or_create_telegram_session(db_session, chat_id)
    assert created is True
    assert session_obj.chat_id == chat_id
    assert session_obj.preferred_style == "professional"
    assert session_obj.risk_tolerance == "moderate"
    
    # 2. Test Get Existing
    session_obj2, created2 = get_or_create_telegram_session(db_session, chat_id)
    assert created2 is False
    assert session_obj2.id == session_obj.id

def test_telegram_session_update(db_session):
    chat_id = 987654321
    get_or_create_telegram_session(db_session, chat_id)
    
    update_telegram_session(db_session, chat_id, {
        "preferred_model_override": "gpt-4",
        "risk_tolerance": "high",
        "summary_context": "User likes meme coins"
    })
    
    # Verify update
    session_obj, _ = get_or_create_telegram_session(db_session, chat_id)
    assert session_obj.preferred_model_override == "gpt-4"
    assert session_obj.risk_tolerance == "high"
    assert session_obj.summary_context == "User likes meme coins"

def test_telegram_message_log(db_session):
    chat_id = 111222333
    
    # Insert messages out of order to test sorting
    insert_telegram_message_log(db_session, {
        "chat_id": chat_id,
        "role": "user",
        "content": "Hello",
        "created_at": datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    })
    
    insert_telegram_message_log(db_session, {
        "chat_id": chat_id,
        "role": "assistant",
        "content": "Hi there",
        "model_used": "claude-3",
        "prompt_tokens": 10,
        "completion_tokens": 5,
        "duration_ms": 1500,
        "created_at": datetime(2025, 1, 1, 12, 0, 5, tzinfo=timezone.utc)
    })
    
    insert_telegram_message_log(db_session, {
        "chat_id": chat_id,
        "role": "user",
        "content": "Analyze BTC",
        "created_at": datetime(2025, 1, 1, 12, 0, 10, tzinfo=timezone.utc)
    })
    
    # Needs to retrieve recent messages in chronological order (oldest first in the slice)
    # The limit applies to the newest messages
    messages = get_recent_telegram_messages(db_session, chat_id, limit=2)
    assert len(messages) == 2
    
    # Because rows are returned reversed (oldest first among the N newest),
    # the first item should be the assistant's reply, the second should be the "Analyze BTC" command.
    assert messages[0].content == "Hi there"
    assert messages[0].role == "assistant"
    
    assert messages[1].content == "Analyze BTC"
    assert messages[1].role == "user"
