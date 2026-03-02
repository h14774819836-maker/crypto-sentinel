import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from app.db.models import Base, LlmCall
from app.db.repository import insert_llm_call, get_llm_calls, get_llm_stats_1h

@pytest.fixture
def db_session():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(bind=engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    try:
        yield session
    finally:
        session.close()
        Base.metadata.drop_all(bind=engine)

def test_insert_and_get_llm_call(db_session):
    data = {
        "task": "market",
        "provider_name": "OpenAICompatibleProvider",
        "model": "deepseek-reasoner",
        "status": "ok",
        "duration_ms": 1500,
        "prompt_tokens": 100,
        "completion_tokens": 200,
        "error_summary": None
    }
    
    call_id = insert_llm_call(db_session, data)
    assert call_id is not None
    
    calls = get_llm_calls(db_session, limit=10, task="market")
    assert len(calls) == 1
    assert calls[0].model == "deepseek-reasoner"
    assert calls[0].status == "ok"
    assert calls[0].duration_ms == 1500
    
def test_llm_stats_aggregation(db_session):
    # Insert 1 OK market
    insert_llm_call(db_session, {
        "task": "market", "provider_name": "test", "model": "test",
        "status": "ok", "duration_ms": 1000
    })
    
    # Insert 1 Timeout market
    insert_llm_call(db_session, {
        "task": "market", "provider_name": "test", "model": "test",
        "status": "timeout", "duration_ms": 5000, "error_summary": "Timeout"
    })
    
    # Insert 1 RateLimit youtube
    insert_llm_call(db_session, {
        "task": "youtube", "provider_name": "test", "model": "gpt-4o",
        "status": "429", "duration_ms": 100
    })
    
    stats = get_llm_stats_1h(db_session)
    
    assert "market" in stats
    assert stats["market"]["total"] == 2
    assert stats["market"]["ok"] == 1
    assert stats["market"]["error"] == 0 # timeout goes to error or just total-ok depending on query semantics, but actually wait, the query maps "error" to "status == error". Timeout is not matched for "error" sum.
    assert stats["market"]["rate_limited"] == 0
    assert stats["market"]["avg_duration"] == 3000
    
    assert "youtube" in stats
    assert stats["youtube"]["total"] == 1
    assert stats["youtube"]["rate_limited"] == 1
    assert stats["youtube"]["ok"] == 0
