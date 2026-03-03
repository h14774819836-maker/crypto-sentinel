from functools import lru_cache
from typing import Any, List, Optional
from dataclasses import dataclass

from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict
import json

@dataclass
class LLMConfig:
    enabled: bool
    provider: str
    api_key: str
    base_url: str
    model: str
    use_reasoning: str  # auto, true, false
    max_concurrency: int
    max_retries: int
    reasoning_effort: Optional[str] = None  # low, medium, high
    http_referer: str = ""
    x_title: str = ""
    market_temperature: float = 0.1

DEEPSEEK_BASE_URL_DEFAULT = "https://api.deepseek.com"
OPENROUTER_BASE_URL_DEFAULT = "https://openrouter.ai/api/v1"
ARK_BASE_URL_DEFAULT = "https://ark.cn-beijing.volces.com/api/v3"
DEFAULT_LLM_TASK_ROUTING: dict[str, str] = {
    "general": "general",
    "telegram_chat": "general",
    "market": "market",
    "youtube": "youtube",
    "selfcheck": "general",
}

class ProfileConfig(BaseModel):
    provider: str
    model: str
    use_reasoning: str = "auto"
    base_url_override: Optional[str] = None
    api_key_override: Optional[str] = None
    extra_headers: Optional[dict] = None
    enabled: Optional[bool] = True
    max_concurrency: Optional[int] = 2
    max_retries: Optional[int] = 3
    reasoning_effort: Optional[str] = None
    http_referer: Optional[str] = ""
    x_title: Optional[str] = ""



MODEL_REGISTRY: list[dict[str, str]] = [
    {"id": "doubao-seed-2-0-pro-260215", "label": "豆包2.0"},
    {"id": "deepseek/deepseek-r1", "label": "DeepSeek R1"},
    {"id": "anthropic/claude-sonnet-4.6", "label": "Claude Sonnet 4.6"},
    {"id": "anthropic/claude-opus-4.6", "label": "Claude Opus 4.6"},
    {"id": "google/gemini-3.1-pro-preview", "label": "Gemini 3.1 Pro Preview"},
    {"id": "qwen/qwen3.5-plus-02-15", "label": "Qwen3.5 Plus (2026-02-15)"},
    {"id": "qwen/qwen3.5-397b-a17b", "label": "Qwen3.5 397B A17B"},
    {"id": "minimax/minimax-m2.5", "label": "MiniMax M2.5"},
    {"id": "z-ai/glm-5", "label": "GLM 5"},
]

TIER_DEFAULTS: dict[str, dict[str, Any]] = {
    "cheap": {"use_reasoning": "false", "max_concurrency": 4, "max_retries": 2, "base_url_mode": "provider_default"},
    "balanced": {"use_reasoning": "auto", "max_concurrency": 2, "max_retries": 3, "base_url_mode": "provider_default"},
    "premium": {"use_reasoning": "true", "max_concurrency": 1, "max_retries": 3, "base_url_mode": "provider_default"},
}

MODEL_CATALOG: list[dict[str, Any]] = [
    {
        "id": "doubao-seed-2-0-pro-260215",
        "label": "\u8c46\u53052.0",
        "provider": "ark",
        "tier": "balanced",
        "description": "\u706b\u5c71 Ark \u901a\u9053\u4e3b\u529b\u6a21\u578b",
        "defaults": dict(TIER_DEFAULTS["balanced"]),
    },
    {
        "id": "deepseek-chat",
        "label": "DeepSeek Chat",
        "provider": "deepseek",
        "tier": "cheap",
        "description": "\u901f\u5ea6\u4f18\u5148\uff0c\u9002\u5408\u804a\u5929/\u81ea\u68c0",
        "defaults": dict(TIER_DEFAULTS["cheap"]),
    },
    {
        "id": "deepseek-reasoner",
        "label": "DeepSeek Reasoner",
        "provider": "deepseek",
        "tier": "premium",
        "description": "\u63a8\u7406\u80fd\u529b\u4f18\u5148\uff0c\u9002\u5408\u590d\u6742\u5206\u6790",
        "defaults": dict(TIER_DEFAULTS["premium"]),
    },
    {
        "id": "deepseek/deepseek-r1",
        "label": "DeepSeek R1",
        "provider": "openrouter",
        "tier": "balanced",
        "description": "OpenRouter \u5747\u8861\u63a8\u7406\u6a21\u578b",
        "defaults": dict(TIER_DEFAULTS["balanced"]),
    },
    {
        "id": "anthropic/claude-sonnet-4.6",
        "label": "Claude Sonnet 4.6",
        "provider": "openrouter",
        "tier": "balanced",
        "description": "\u8d28\u91cf/\u901f\u5ea6\u5747\u8861",
        "defaults": dict(TIER_DEFAULTS["balanced"]),
    },
    {
        "id": "anthropic/claude-opus-4.6",
        "label": "Claude Opus 4.6",
        "provider": "openrouter",
        "tier": "premium",
        "description": "\u9ad8\u8d28\u91cf\u89e3\u8bfb\u573a\u666f",
        "defaults": dict(TIER_DEFAULTS["premium"]),
    },
    {
        "id": "google/gemini-3.1-pro-preview",
        "label": "Gemini 3.1 Pro Preview",
        "provider": "openrouter",
        "tier": "premium",
        "description": "\u5e02\u573a\u5206\u6790\u4f18\u9009\u6a21\u578b",
        "defaults": dict(TIER_DEFAULTS["premium"]),
    },
    {
        "id": "qwen/qwen3.5-plus-02-15",
        "label": "Qwen3.5 Plus (2026-02-15)",
        "provider": "openrouter",
        "tier": "balanced",
        "description": "\u5e38\u89c4\u5206\u6790\u4e0e\u7efc\u5408\u95ee\u7b54",
        "defaults": dict(TIER_DEFAULTS["balanced"]),
    },
    {
        "id": "qwen/qwen3.5-397b-a17b",
        "label": "Qwen3.5 397B A17B",
        "provider": "openrouter",
        "tier": "premium",
        "description": "\u5927\u53c2\u6570\u9ad8\u9636\u6a21\u578b",
        "defaults": dict(TIER_DEFAULTS["premium"]),
    },
    {
        "id": "minimax/minimax-m2.5",
        "label": "MiniMax M2.5",
        "provider": "openrouter",
        "tier": "balanced",
        "description": "\u5747\u8861\u54c1\u8d28\u4e0e\u6210\u672c",
        "defaults": dict(TIER_DEFAULTS["balanced"]),
    },
    {
        "id": "z-ai/glm-5",
        "label": "GLM 5",
        "provider": "openrouter",
        "tier": "cheap",
        "description": "\u4f4e\u6210\u672c\u5feb\u901f\u54cd\u5e94",
        "defaults": dict(TIER_DEFAULTS["cheap"]),
    },
]
MODEL_CATALOG_BY_ID: dict[str, dict[str, Any]] = {item["id"]: item for item in MODEL_CATALOG}
MODEL_REGISTRY = [{"id": item["id"], "label": item["label"]} for item in MODEL_CATALOG]
SUPPORTED_LLM_PROVIDERS: set[str] = {"deepseek", "openrouter", "openai_compatible", "ark"}
PROVIDER_DEFAULT_MODEL: dict[str, str] = {
    "deepseek": "deepseek-chat",
    "openrouter": "deepseek/deepseek-r1",
    "openai_compatible": "gpt-4o-mini",
    "ark": "doubao-seed-2-0-pro-260215",
}
DEFAULT_PROFILE_TEMPLATES: dict[str, dict[str, Any]] = {
    "general": {
        "provider": "deepseek",
        "model": "deepseek-reasoner",
        "use_reasoning": "auto",
        "enabled": True,
        "max_concurrency": 2,
        "max_retries": 3,
    },
    "market": {
        "provider": "openrouter",
        "model": "deepseek/deepseek-r1",
        "use_reasoning": "auto",
        "enabled": True,
        "max_concurrency": 2,
        "max_retries": 3,
    },
    "youtube": {
        "provider": "deepseek",
        "model": "deepseek-chat",
        "use_reasoning": "false",
        "enabled": True,
        "max_concurrency": 2,
        "max_retries": 3,
    },
}


def _normalize_provider_name(provider: Any, *, fallback: str = "deepseek") -> str:
    provider_norm = str(provider or "").strip().lower()
    return provider_norm if provider_norm in SUPPORTED_LLM_PROVIDERS else fallback


def _default_model_for_provider(provider: Any) -> str:
    provider_norm = _normalize_provider_name(provider)
    return PROVIDER_DEFAULT_MODEL.get(provider_norm, PROVIDER_DEFAULT_MODEL["deepseek"])


def _guess_provider_for_model(model_id: str) -> str:
    model_l = str(model_id or "").lower().strip()
    if model_l.startswith("doubao-"):
        return "ark"
    if model_l.startswith("deepseek-"):
        return "deepseek"
    return "openrouter"


def _tier_defaults(tier: Any) -> dict[str, Any]:
    tier_norm = str(tier or "").strip().lower()
    return dict(TIER_DEFAULTS.get(tier_norm, TIER_DEFAULTS["balanced"]))


def _build_dynamic_catalog_item(model_id: str, *, provider: str | None = None) -> dict[str, Any]:
    pid = str(model_id or "").strip()
    provider_norm = _normalize_provider_name(provider or _guess_provider_for_model(pid))
    tier = "balanced"
    if provider_norm == "deepseek" and "reasoner" in pid.lower():
        tier = "premium"
    return {
        "id": pid,
        "label": _model_label(pid),
        "provider": provider_norm,
        "tier": tier,
        "description": "Runtime discovered model",
        "defaults": _tier_defaults(tier),
    }


def _model_label(model_id: str) -> str:
    catalog_item = MODEL_CATALOG_BY_ID.get(model_id)
    if catalog_item and catalog_item.get("label"):
        return str(catalog_item["label"])
    for item in MODEL_REGISTRY:
        if item["id"] == model_id and item.get("label"):
            return str(item["label"])
    tail = model_id.split("/")[-1].replace("-", " ").strip()
    return tail.title() if tail else model_id


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        populate_by_name=True,
    )

    app_name: str = "Crypto Sentinel"
    app_env: str = Field(default="dev", alias="APP_ENV")
    app_version: str = Field(default="0.1.0", alias="APP_VERSION")
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")
    market_ai_log_file: str = Field(default="data/logs/market_ai.log", alias="MARKET_AI_LOG_FILE")
    market_ai_log_max_bytes: int = Field(default=5_000_000, alias="MARKET_AI_LOG_MAX_BYTES")
    market_ai_log_backup_count: int = Field(default=5, alias="MARKET_AI_LOG_BACKUP_COUNT")
    market_ai_prompt_log_max_chars: int = Field(default=12000, alias="MARKET_AI_PROMPT_LOG_MAX_CHARS")
    market_ai_response_log_max_chars: int = Field(default=8000, alias="MARKET_AI_RESPONSE_LOG_MAX_CHARS")
    timezone: str = Field(default="UTC", alias="TIMEZONE")

    database_url: str = Field(default="sqlite:///./data/crypto_sentinel.db", alias="DATABASE_URL")
    db_enforce_postgres_envs: str = Field(default="stage,prod", alias="DB_ENFORCE_POSTGRES_ENVS")
    db_disable_fallback_envs: str = Field(default="stage,prod", alias="DB_DISABLE_FALLBACK_ENVS")

    watchlist: str = Field(default="BTCUSDT,ETHUSDT,SOLUSDT", alias="WATCHLIST")

    binance_rest_url: str = Field(default="https://api.binance.com", alias="BINANCE_REST_URL")
    binance_ws_url: str = Field(default="wss://stream.binance.com:9443/stream", alias="BINANCE_WS_URL")
    enable_miniticker: bool = Field(default=False, alias="ENABLE_MINITICKER")

    poll_seconds: int = Field(default=10, alias="POLL_SECONDS")
    kline_sync_seconds: int = Field(default=60, alias="KLINE_SYNC_SECONDS")
    gap_fill_interval_seconds: int = Field(default=120, alias="GAP_FILL_INTERVAL_SECONDS")
    backfill_days_default: int = Field(default=7, alias="BACKFILL_DAYS_DEFAULT")

    worker_id: str = Field(default="worker-1", alias="WORKER_ID")
    worker_heartbeat_seconds: int = Field(default=15, alias="WORKER_HEARTBEAT_SECONDS")

    alert_cooldown_seconds: int = Field(default=1800, alias="ALERT_COOLDOWN_SECONDS")

    vol_p75_min_candles: int = Field(default=10080, alias="VOL_P75_MIN_CANDLES")
    vol_fallback_min_candles: int = Field(default=200, alias="VOL_FALLBACK_MIN_CANDLES")
    vol_fallback_k: float = Field(default=2.0, alias="VOL_FALLBACK_K")

    telegram_enabled: bool = Field(default=False, alias="TELEGRAM_ENABLED")
    telegram_bot_token: str = Field(default="", alias="TELEGRAM_BOT_TOKEN")
    telegram_chat_id: str = Field(default="", alias="TELEGRAM_CHAT_ID")
    telegram_webhook_secret: str = Field(default="", alias="TELEGRAM_WEBHOOK_SECRET")
    telegram_allowed_chat_ids: str = Field(default="", alias="TELEGRAM_ALLOWED_CHAT_IDS")
    telegram_inbound_mode: str = Field(default="polling", alias="TELEGRAM_INBOUND_MODE")
    telegram_polling_timeout_seconds: int = Field(default=50, alias="TELEGRAM_POLLING_TIMEOUT_SECONDS")
    telegram_polling_interval_seconds: float = Field(default=0.2, alias="TELEGRAM_POLLING_INTERVAL_SECONDS")
    telegram_polling_auto_delete_webhook: bool = Field(default=True, alias="TELEGRAM_POLLING_AUTO_DELETE_WEBHOOK")
    telegram_polling_drop_pending_updates: bool = Field(default=True, alias="TELEGRAM_POLLING_DROP_PENDING_UPDATES")
    telegram_polling_state_file: str = Field(default="data/telegram_poller_state.json", alias="TELEGRAM_POLLING_STATE_FILE")

    # Common Keys
    deepseek_api_key: str | None = Field(default=None, alias="DEEPSEEK_API_KEY")
    openrouter_api_key: str | None = Field(default=None, alias="OPENROUTER_API_KEY")
    openai_api_key: str | None = Field(default=None, alias="OPENAI_API_KEY")
    ark_api_key: str | None = Field(default=None, alias="ARK_API_KEY")

    # Task Profiles Config
    llm_profiles_json: str = Field(default="{}", alias="LLM_PROFILES_JSON")
    llm_task_routing_json: str = Field(default="{}", alias="LLM_TASK_ROUTING_JSON")
    llm_hot_reload_signal_file: str = Field(default="data/llm_hot_reload_signal.json", alias="LLM_HOT_RELOAD_SIGNAL_FILE")
    llm_hot_reload_ack_file: str = Field(default="data/llm_hot_reload_ack.json", alias="LLM_HOT_RELOAD_ACK_FILE")

    llm_allowed_models: str = Field(default="", alias="LLM_ALLOWED_MODELS")

    ai_analysis_interval_seconds: int = Field(default=600, alias="AI_ANALYSIS_INTERVAL_SECONDS")
    ai_signal_confidence_threshold: int = Field(default=70, alias="AI_SIGNAL_CONFIDENCE_THRESHOLD")
    ai_history_candles: int = Field(default=50, alias="AI_HISTORY_CANDLES")
    llm_market_temperature: float = Field(default=0.1, alias="LLM_MARKET_TEMPERATURE")
    grounding_mode: str = Field(default="balanced", alias="GROUNDING_MODE")
    grounding_severe_multiplier: float = Field(default=3.0, alias="GROUNDING_SEVERE_MULTIPLIER")

    # Multi-timeframe
    multi_tf_intervals: str = Field(default="5m,15m,1h,4h", alias="MULTI_TF_INTERVALS")
    multi_tf_sync_seconds: int = Field(default=300, alias="MULTI_TF_SYNC_SECONDS")
    multi_tf_backfill_days: int = Field(default=30, alias="MULTI_TF_BACKFILL_DAYS")

    # Funding rate (Binance Futures)
    binance_futures_url: str = Field(default="https://fapi.binance.com", alias="BINANCE_FUTURES_URL")
    funding_rate_sync_seconds: int = Field(default=300, alias="FUNDING_RATE_SYNC_SECONDS")

    # Dynamic anomaly thresholds / anomaly engine
    anomaly_engine_mode: str = Field(default="score_v1", alias="ANOMALY_ENGINE_MODE")
    anomaly_volume_zscore_threshold: float = Field(default=3.0, alias="ANOMALY_VOLUME_ZSCORE_THRESHOLD")
    anomaly_score_threshold_trending: int = Field(default=70, alias="ANOMALY_SCORE_THRESHOLD_TRENDING")
    anomaly_score_threshold_ranging: int = Field(default=85, alias="ANOMALY_SCORE_THRESHOLD_RANGING")
    anomaly_score_threshold_volatile: int = Field(default=80, alias="ANOMALY_SCORE_THRESHOLD_VOLATILE")
    anomaly_score_threshold_unconfirmed_extreme: int = Field(default=92, alias="ANOMALY_SCORE_THRESHOLD_UNCONFIRMED_EXTREME")
    anomaly_persist_enter_bars: int = Field(default=2, alias="ANOMALY_PERSIST_ENTER_BARS")
    anomaly_hysteresis_exit_delta: int = Field(default=20, alias="ANOMALY_HYSTERESIS_EXIT_DELTA")
    anomaly_require_mtf_confirm: bool = Field(default=True, alias="ANOMALY_REQUIRE_MTF_CONFIRM")
    anomaly_mtf_confirm_tfs: str = Field(default="5m,15m", alias="ANOMALY_MTF_CONFIRM_TFS")
    anomaly_budget_per_symbol_per_day: int = Field(default=5, alias="ANOMALY_BUDGET_PER_SYMBOL_PER_DAY")
    anomaly_budget_excess_action: str = Field(default="store_only", alias="ANOMALY_BUDGET_EXCESS_ACTION")
    anomaly_cooldown_seconds_score_80_84: int = Field(default=1800, alias="ANOMALY_COOLDOWN_SECONDS_SCORE_80_84")
    anomaly_cooldown_seconds_score_85_91: int = Field(default=3600, alias="ANOMALY_COOLDOWN_SECONDS_SCORE_85_91")
    anomaly_cooldown_seconds_score_92_plus: int = Field(default=600, alias="ANOMALY_COOLDOWN_SECONDS_SCORE_92_PLUS")
    telegram_alert_template_style: str = Field(default="readable", alias="TELEGRAM_ALERT_TEMPLATE_STYLE")
    telegram_alert_include_debug: bool = Field(default=True, alias="TELEGRAM_ALERT_INCLUDE_DEBUG")

    spike_atr_multiplier: float = Field(default=1.5, alias="SPIKE_ATR_MULTIPLIER")
    spike_fallback_threshold: float = Field(default=0.012, alias="SPIKE_FALLBACK_THRESHOLD")
    breakout_lookback: int = Field(default=50, alias="BREAKOUT_LOOKBACK")

    # YouTube
    youtube_enabled: bool = Field(default=False, alias="YOUTUBE_ENABLED")
    youtube_channel_ids: str = Field(default="", alias="YOUTUBE_CHANNEL_IDS")
    youtube_poll_seconds: int = Field(default=1800, alias="YOUTUBE_POLL_SECONDS")
    youtube_max_videos_per_run: int = Field(default=10, alias="YOUTUBE_MAX_VIDEOS_PER_RUN")
    youtube_analyze_poll_seconds: int | None = Field(default=None, alias="YOUTUBE_ANALYZE_POLL_SECONDS")
    youtube_asr_backfill_seconds: int | None = Field(default=None, alias="YOUTUBE_ASR_BACKFILL_SECONDS")
    youtube_analyze_max_per_run: int | None = Field(default=None, alias="YOUTUBE_ANALYZE_MAX_PER_RUN")
    youtube_subtitle_fetch_max_per_run: int | None = Field(default=None, alias="YOUTUBE_SUBTITLE_FETCH_MAX_PER_RUN")
    youtube_langs: str = Field(default="zh-Hans,zh-Hant,en", alias="YOUTUBE_LANGS")
    youtube_consensus_lookback_hours: int = Field(default=48, alias="YOUTUBE_CONSENSUS_LOOKBACK_HOURS")
    youtube_target_symbol: str = Field(default="BTCUSDT", alias="YOUTUBE_TARGET_SYMBOL")
    youtube_analyze_max_auto_retries: int = Field(default=2, alias="YOUTUBE_ANALYZE_MAX_AUTO_RETRIES")
    youtube_analyze_retry_base_seconds: int = Field(default=60, alias="YOUTUBE_ANALYZE_RETRY_BASE_SECONDS")
    youtube_analyze_retry_max_seconds: int = Field(default=900, alias="YOUTUBE_ANALYZE_RETRY_MAX_SECONDS")
    youtube_analysis_stall_running_seconds: int = Field(default=420, alias="YOUTUBE_ANALYSIS_STALL_RUNNING_SECONDS")
    youtube_auth_auto_recover_enabled: bool = Field(default=True, alias="YOUTUBE_AUTH_AUTO_RECOVER_ENABLED")
    youtube_auth_auto_recover_batch: int = Field(default=20, alias="YOUTUBE_AUTH_AUTO_RECOVER_BATCH")
    youtube_auth_auto_recover_max_attempts: int = Field(default=2, alias="YOUTUBE_AUTH_AUTO_RECOVER_MAX_ATTEMPTS")

    # ASR (Local)
    asr_enabled: bool = Field(default=False, alias="ASR_ENABLED")
    asr_backend: str = Field(default="local_faster_whisper", alias="ASR_BACKEND")
    asr_model: str = Field(default="small", alias="ASR_MODEL")
    asr_device: str = Field(default="cuda", alias="ASR_DEVICE")
    asr_compute_type: str = Field(default="float16", alias="ASR_COMPUTE_TYPE")
    asr_vad_filter: bool = Field(default=True, alias="ASR_VAD_FILTER")
    asr_max_videos_per_run: int = Field(default=3, alias="ASR_MAX_VIDEOS_PER_RUN")
    asr_audio_cache_dir: str = Field(default="data/audio", alias="ASR_AUDIO_CACHE_DIR")
    asr_keep_audio: bool = Field(default=False, alias="ASR_KEEP_AUDIO")

    # Feature incremental pipeline
    feature_incremental_enabled: bool = Field(default=True, alias="FEATURE_INCREMENTAL_ENABLED")
    feature_lookback_rows: int = Field(default=180, alias="FEATURE_LOOKBACK_ROWS")
    feature_max_pending_bars: int = Field(default=20, alias="FEATURE_MAX_PENDING_BARS")
    feature_max_batches_per_run: int = Field(default=3, alias="FEATURE_MAX_BATCHES_PER_RUN")
    feature_timeframes: str = Field(default="1m,5m,10m,15m,1h,4h", alias="FEATURE_TIMEFRAMES")

    # Ops/metrics
    ops_job_metrics_window: int = Field(default=200, alias="OPS_JOB_METRICS_WINDOW")
    ops_job_metrics_file: str = Field(default="data/job_metrics.json", alias="OPS_JOB_METRICS_FILE")

    @property
    def watchlist_symbols(self) -> List[str]:
        return [item.strip().upper() for item in self.watchlist.split(",") if item.strip()]

    @property
    def db_enforce_postgres_env_list(self) -> List[str]:
        return [item.strip().lower() for item in self.db_enforce_postgres_envs.split(",") if item.strip()]

    @property
    def db_disable_fallback_env_list(self) -> List[str]:
        return [item.strip().lower() for item in self.db_disable_fallback_envs.split(",") if item.strip()]

    @property
    def telegram_allowed_chats(self) -> List[int]:
        if not self.telegram_allowed_chat_ids:
            return []
        try:
            return [int(item.strip()) for item in self.telegram_allowed_chat_ids.split(",") if item.strip()]
        except ValueError:
            return []

    @property
    def telegram_inbound_mode_normalized(self) -> str:
        return (self.telegram_inbound_mode or "").strip().lower()

    @property
    def anomaly_engine_mode_normalized(self) -> str:
        return (self.anomaly_engine_mode or "").strip().lower()

    @property
    def anomaly_mtf_confirm_tf_list(self) -> List[str]:
        return [item.strip() for item in (self.anomaly_mtf_confirm_tfs or "").split(",") if item.strip()]

    @property
    def multi_tf_interval_list(self) -> List[str]:
        return [item.strip() for item in self.multi_tf_intervals.split(",") if item.strip()]

    @property
    def feature_timeframe_list(self) -> List[str]:
        return [item.strip() for item in self.feature_timeframes.split(",") if item.strip()]

    @property
    def youtube_channel_id_list(self) -> List[str]:
        return [item.strip() for item in self.youtube_channel_ids.split(",") if item.strip()]

    @property
    def youtube_lang_list(self) -> List[str]:
        return [item.strip() for item in self.youtube_langs.split(",") if item.strip()]

    @property
    def youtube_analyze_poll_seconds_effective(self) -> int:
        return int(self.youtube_analyze_poll_seconds or self.youtube_poll_seconds)

    @property
    def youtube_asr_backfill_seconds_effective(self) -> int:
        return int(self.youtube_asr_backfill_seconds or self.youtube_poll_seconds)

    @property
    def youtube_analyze_max_per_run_effective(self) -> int:
        return int(self.youtube_analyze_max_per_run or self.youtube_max_videos_per_run)

    @property
    def youtube_subtitle_fetch_max_per_run_effective(self) -> int:
        return int(self.youtube_subtitle_fetch_max_per_run or self.youtube_max_videos_per_run)

    @property
    def youtube_analysis_stall_waiting_seconds_effective(self) -> int:
        poll = max(1, int(self.youtube_analyze_poll_seconds_effective))
        return max(420, poll * 2)

    @property
    def llm_model_registry(self) -> list[dict[str, str]]:
        return [{"id": item["id"], "label": item["label"]} for item in self.llm_model_catalog]

    @property
    def llm_model_catalog(self) -> list[dict[str, Any]]:
        configured_ids = [item.strip() for item in self.llm_allowed_models.split(",") if item.strip()]
        if configured_ids:
            catalog: list[dict[str, Any]] = []
            for mid in configured_ids:
                item = MODEL_CATALOG_BY_ID.get(mid)
                catalog.append(dict(item) if item else _build_dynamic_catalog_item(mid))
        else:
            catalog = [dict(item) for item in MODEL_CATALOG]

        market_cfg = self.resolve_llm_config("market")
        if market_cfg.model and all(item.get("id") != market_cfg.model for item in catalog):
            catalog.insert(0, _build_dynamic_catalog_item(market_cfg.model, provider=market_cfg.provider))
        return catalog

    @property
    def llm_model_tiers(self) -> dict[str, list[dict[str, Any]]]:
        tiers: dict[str, list[dict[str, Any]]] = {"premium": [], "balanced": [], "cheap": []}
        for item in self.llm_model_catalog:
            tier = str(item.get("tier") or "balanced").lower()
            if tier not in tiers:
                tier = "balanced"
            if item.get("id"):
                tiers[tier].append(item)
        return tiers

    @property
    def allowed_llm_models(self) -> set[str]:
        return {item["id"] for item in self.llm_model_catalog}

    @property
    def llm_profiles(self) -> dict[str, ProfileConfig]:
        try:
            data = json.loads(self.llm_profiles_json)
        except json.JSONDecodeError:
            data = {}

        if not isinstance(data, dict):
            data = {}

        merged_raw: dict[str, dict[str, Any]] = {}
        for key, default_profile in DEFAULT_PROFILE_TEMPLATES.items():
            raw_profile = data.get(key)
            merged_raw[key] = dict(raw_profile) if isinstance(raw_profile, dict) else dict(default_profile)

        for raw_key, raw_profile in data.items():
            if not isinstance(raw_key, str) or not raw_key.strip():
                continue
            key = raw_key.strip()
            if key in merged_raw:
                continue
            if isinstance(raw_profile, dict):
                merged_raw[key] = dict(raw_profile)

        profiles: dict[str, ProfileConfig] = {}
        for profile_name, raw_profile in merged_raw.items():
            fallback_template = DEFAULT_PROFILE_TEMPLATES.get(profile_name) or DEFAULT_PROFILE_TEMPLATES["general"]
            provider = _normalize_provider_name(
                raw_profile.get("provider"),
                fallback=str(fallback_template.get("provider", "deepseek")),
            )
            model = (
                str(raw_profile.get("model") or "").strip()
                or str(fallback_template.get("model") or "").strip()
                or _default_model_for_provider(provider)
            )

            normalized = dict(fallback_template)
            normalized.update(raw_profile)
            normalized["provider"] = provider
            normalized["model"] = model
            profiles[profile_name] = ProfileConfig(**normalized)
        return profiles

    @property
    def llm_task_routing(self) -> dict[str, str]:
        try:
            data = json.loads(self.llm_task_routing_json)
        except json.JSONDecodeError:
            data = {}

        routing = dict(DEFAULT_LLM_TASK_ROUTING)
        if not isinstance(data, dict):
            return routing

        for raw_task, raw_profile in data.items():
            if not isinstance(raw_task, str) or not isinstance(raw_profile, str):
                continue
            task = raw_task.strip().lower()
            profile = raw_profile.strip()
            if not task or not profile:
                continue
            routing[task] = profile
        return routing

    def normalize_llm_task(self, task: str) -> str:
        task_norm = (task or "").strip().lower()
        aliases = {
            "default": "general",
            "chat": "telegram_chat",
            "telegram": "telegram_chat",
            "telegram_agent": "telegram_chat",
        }
        return aliases.get(task_norm, task_norm or "general")

    def resolve_llm_profile_name(self, task: str) -> str:
        task_norm = self.normalize_llm_task(task)
        routing = self.llm_task_routing
        return routing.get(task_norm, task_norm)

    def resolve_llm_config(self, task: str) -> LLMConfig:
        profiles = self.llm_profiles
        task_norm = self.normalize_llm_task(task)
        profile_name = self.resolve_llm_profile_name(task_norm)
        profile = profiles.get(profile_name) or profiles.get(task_norm) or profiles.get("general")
        if not profile:
            raise ValueError(
                f"No profile found for task={task!r} (normalized={task_norm!r}, routed={profile_name!r}) and no general profile fallback available."
            )

        provider = profile.provider
        
        base_url = profile.base_url_override
        api_key = profile.api_key_override

        if not base_url:
            if provider == "deepseek":
                base_url = DEEPSEEK_BASE_URL_DEFAULT
            elif provider == "openrouter":
                base_url = OPENROUTER_BASE_URL_DEFAULT
            elif provider == "ark":
                base_url = ARK_BASE_URL_DEFAULT

        if not api_key:
            if provider == "deepseek":
                api_key = self.deepseek_api_key
            elif provider == "openrouter":
                api_key = self.openrouter_api_key
            elif provider == "openai_compatible":
                api_key = self.openai_api_key
            elif provider == "ark":
                api_key = self.ark_api_key

        return LLMConfig(
            enabled=profile.enabled,
            provider=provider,
            api_key=api_key or "",
            base_url=base_url or "",
            model=profile.model,
            use_reasoning=profile.use_reasoning,
            max_concurrency=profile.max_concurrency or 2,
            max_retries=profile.max_retries or 3,
            reasoning_effort=profile.reasoning_effort,
            http_referer=profile.http_referer or "",
            x_title=profile.x_title or "",
            market_temperature=max(0.0, min(0.3, float(self.llm_market_temperature))),
        )

    def default_model_for_provider(self, provider: str) -> str:
        return _default_model_for_provider(provider)


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
