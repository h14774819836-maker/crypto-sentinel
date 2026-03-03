# Crypto Sentinel V0.2

AI 驱动的 24/7 加密货币市场监控与技术分析系统。

**核心能力:**
- 📊 多时间框架 K线分析（1m / 5m / 15m / 1h / 4h）
- 🤖 DeepSeek Reasoner 智能交易信号（做多/做空/止盈/止损）
- 📈 技术指标：RSI、MACD、布林带、OBV、Stochastic RSI、EMA均线束
- 💰 Binance Futures 资金费率 & 持仓量监控
- 📺 YouTube 博主观点追踪（字幕 + 本地 ASR 转录 → AI 共识分析）
- 🚨 动态异常检测（ATR自适应阈值）
- 📱 Telegram 推送（可选）
- 🖥️ Web 仪表盘

## 一键启动

**首选方式 — 双击运行：**

```
run.bat
```

或 PowerShell：
```powershell
.\run.ps1
```

**自动完成：** Python 检查 → 虚拟环境 → 依赖安装 → 数据库迁移 → 启动 → 打开浏览器

> 首次运行需编辑 `.env` 填入你的 `DEEPSEEK_API_KEY`。

## Upgrade Note (YouTube AI Progress Runtime)

If you are upgrading an existing deployment/database to a version that includes YouTube AI analysis progress tracking, run this one-time migration before starting the updated Web/Worker processes:

```bash
python scripts/migrate_youtube_analysis_runtime.py
```

This is idempotent and safe to run multiple times on SQLite.

## Dashboard

启动后自动打开 `http://127.0.0.1:8000`

- `/` — 市场总览 + AI 信号 + YouTube 共识
- `/youtube` — YouTube 频道管理
- `/alerts` — 告警列表

## 🎨 界面设计 (UI Design)

Crypto Sentinel 提供了一个极致现代的 SaaS 级监控面板，专为多屏监控与数据沉浸感打造：

- **赛博朋克深色主题 (Cyberpunk Dark Theme)**：强制深色模式，搭配高对比度的状态色（荧光绿、告警红、AI紫），长时间盯盘不伤眼。
- **玻璃拟态质感 (Glassmorphism)**：半透明磨砂面板、微妙的边框发光与阴影过渡，呈现高级的科技感。
- **响应式卡片网格 (Responsive Grid Layout)**：彻底抛弃传统拥挤的表格，采用流畅的网格结构，从手机端到 1440px 超宽屏都能完美自适应。
- **高阶分析弹窗 (Rich AI Modals)**：YouTube 视频分析配有专注模式的独立详情页，内含清晰的 AI 指标记分卡（VSI 评分、方向置信度、策略质量）与可折叠的原始文稿。
- **动态语言切换 (Dynamic Localization)**：纯前端实现的毫秒级中英双语无缝切换，无需刷新整个页面。

## YouTube 观点追踪

自动发现关注的 YouTube 博主最新视频 → 抓取字幕/本地 ASR 转录 → AI 结构化分析 → 多观点共识融合。

**启用方式：**

1. 编辑 `.env` 设置 `YOUTUBE_ENABLED=true`
2. 在 Dashboard `/youtube` 页面添加频道（粘贴频道 URL）
3. 无字幕视频需启用 ASR：设置 `ASR_ENABLED=true`（推荐 GPU + CUDA 12.x）

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `YOUTUBE_ENABLED` | 启用 YouTube 追踪 | `false` |
| `YOUTUBE_POLL_SECONDS` | 轮询间隔 | `1800` (30min) |
| `YOUTUBE_LANGS` | 字幕语言优先级 | `zh-Hans,zh-Hant,en` |
| `ASR_ENABLED` | 启用本地 ASR 转录 | `false` |
| `ASR_MODEL` | Whisper 模型 | `small` |
| `ASR_DEVICE` | 设备 | `cuda` |

## Docker 启动

```bash
docker compose up --build
```

## CLI 命令

```bash
# 启动全部服务
sentinel up --open-browser

# 回填历史数据
sentinel backfill --days 7

# 健康检查
sentinel doctor
```

## 配置

编辑 `.env` 文件，关键配置：

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `DEEPSEEK_API_KEY` | DeepSeek 原厂 API 密钥（便宜，适合聊天/轻任务） | 可选 |
| `OPENROUTER_API_KEY` | OpenRouter API 密钥（适合高级模型/多模型路由） | 可选 |
| `ARK_API_KEY` | Ark API 密钥（火山引擎 OpenAI-compatible 通道） | 可选 |
| `NVIDIA_NIM_API_KEY` | NVIDIA NIM API 密钥（VLM/多模态通道） | 可选 |
| `LLM_PROFILES_JSON` | LLM profile 定义（provider/model/并发/重试） | JSON |
| `LLM_TASK_ROUTING_JSON` | 任务到 profile 的映射（如 `telegram_chat -> general`） | JSON |
| `WATCHLIST` | 监控币对 | `BTCUSDT,ETHUSDT,SOLUSDT` |
| `MULTI_TF_INTERVALS` | 多时间框架 | `5m,15m,1h,4h` |
| `TELEGRAM_ENABLED` | Telegram 推送 | `false` |
| `DATABASE_URL` | 数据库 | SQLite 本地文件 |

### LLM 双 Provider / 任务分工示例（DeepSeek + OpenRouter）

你可以同时配置两个 Key，然后按任务分工：

- `telegram_chat`（聊天回复）走 DeepSeek 原厂，降低成本
- `market`（市场分析）走 OpenRouter 上的高级推理模型
- `youtube`（视频摘要）按成本自由选择

示例（`.env`）：

```env
DEEPSEEK_API_KEY=your_deepseek_key
OPENROUTER_API_KEY=your_openrouter_key
ARK_API_KEY=your_ark_key
NVIDIA_NIM_API_KEY=your_nvidia_nim_key

LLM_PROFILES_JSON={"general":{"provider":"deepseek","model":"deepseek-chat","use_reasoning":"auto","enabled":true},"market":{"provider":"openrouter","model":"google/gemini-3.1-pro-preview","use_reasoning":"auto","enabled":true},"youtube":{"provider":"deepseek","model":"deepseek-chat","use_reasoning":"auto","enabled":true}}
LLM_TASK_ROUTING_JSON={"telegram_chat":"general","market":"market","youtube":"youtube","selfcheck":"general"}
LLM_HOT_RELOAD_SIGNAL_FILE=data/llm_hot_reload_signal.json
LLM_HOT_RELOAD_ACK_FILE=data/llm_hot_reload_ack.json
```

### LLM ????????????
- `/llm` ????????????????API ???????
- Worker ???? `LLM_HOT_RELOAD_SIGNAL_FILE` / `LLM_HOT_RELOAD_ACK_FILE` ?????????????? `WORKER_HEARTBEAT_SECONDS` ???
- ??????? signal revision ? Worker ACK ???????????????


说明：
- `provider=deepseek` 会自动使用 `DEEPSEEK_API_KEY` 和 DeepSeek 官方 Base URL
- `provider=openrouter` 会自动使用 `OPENROUTER_API_KEY` 和 OpenRouter Base URL
- `provider=ark` 会自动使用 `ARK_API_KEY` 和 `https://ark.cn-beijing.volces.com/api/v3`
- `provider=nvidia_nim` 会自动使用 `NVIDIA_NIM_API_KEY` 和 `https://integrate.api.nvidia.com/v1`
- `nvidia_nim/qwen3.5-397b-a17b` 会在请求上游时自动映射为 `qwen/qwen3.5-397b-a17b`
- 业务代码只按任务取配置（例如 `market` / `youtube` / `telegram_chat`），无需手写 API 地址

## Telegram 本地交互调试（Polling 模式）

适用于本地/内网开发调试，无需公网 URL。线上部署仍建议使用 webhook。

### 关键配置

在 `.env` 中设置（示例）：

```env
TELEGRAM_ENABLED=true
TELEGRAM_BOT_TOKEN=你的bot token
TELEGRAM_INBOUND_MODE=polling
TELEGRAM_POLLING_AUTO_DELETE_WEBHOOK=true
TELEGRAM_POLLING_DROP_PENDING_UPDATES=true
```

- `TELEGRAM_INBOUND_MODE=polling`：Worker 使用 `getUpdates` 长轮询接收 Telegram 入站消息
- `TELEGRAM_POLLING_AUTO_DELETE_WEBHOOK=true`：启动时自动调用 `deleteWebhook`
- `TELEGRAM_POLLING_DROP_PENDING_UPDATES=true`：清空积压消息（本地调试更干净）

### 重要警告（必须注意）

- 如果 Telegram 仍存在 webhook（`getWebhookInfo.url` 非空），`getUpdates` **不会工作**
- 程序启动日志会打印 `url` 和 `pending_update_count`，用于确认 webhook 是否已清理

### 失败处理语义（Polling）

- 轮询模式采用“先推进 offset 再处理 update”
- 若处理失败，该条 update 会被跳过（不会自动重试）

> 后续可选增强：把失败的 update 写入 dead-letter 文件（例如 `data/telegram_poller_deadletter.jsonl`）供排查。

## 测试

```bash
python -m pytest
```

## 安全说明

- 仅提供交易信号，**不会自动下单**
- 所有建议仅供参考，不构成投资建议
