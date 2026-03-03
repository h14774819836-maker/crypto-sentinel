# Crypto Sentinel AI 分析逻辑与信息过载优化报告

## 1. 现状逻辑分析

目前 Crypto Sentinel 的 AI 分析模块采用 **Retrieval-Augmented Generation (RAG) 变体** 的思路，通过预先计算和聚合市场数据，构建结构化 Prompt 发送给 LLM，并对输出进行严格的事实校对。

### 1.1 信息输入 (Input / Context Building)
**核心模块**: `app.ai.market_context_builder` & `app.ai.prompts`

数据输入分为“事实源”和“观点源”两类，经过多层处理和压缩：

1.  **事实源 (Facts Source)**: 具有最高优先级，LLM 必须依据。
    *   **多周期快照 (Multi-timeframe Snapshots)**: 涵盖 4h, 1h, 15m, 5m, 1m 五个周期。
        *   *预处理*: `_sanitize_snapshots_for_prompt` 将原始 K 线列表压缩为 **摘要统计** (Range High/Low, Change %)，仅保留 `latest` 字典中的最新指标值。
        *   *指标集*: RSI, Stoch RSI, MACD, Bollinger Bands, ATR, OBV, EMA Ribbon, Volume Z-Score, Returns (1m/5m/10m)。
    *   **资金费率 (Funding Deltas)**: 聚合当前费率、OI (Open Interest) 及其 1h/4h/24h 变化量。
    *   **数据质量 (Data Quality)**: 标记数据完整性，作为置信度的参考。

2.  **观点源 (Opinion Source)**: 仅作为参考，冲突时降权。
    *   **YouTube Radar**: 聚合市场分析视频的观点（支撑/阻力位、多空倾向）。
        *   *预处理*: `_apply_context_clipping` 对文本进行截断，限制 Top Voices 数量 (2个) 和 Risk Notes 数量 (2条)，防止 Token 爆炸。
    *   **Alerts Digest**: 最近 1h/4h 的告警聚合（如价格突变、指标背离）。
        *   *预处理*: 同样有字符数限制和条目截断。

3.  **Prompt 构建**:
    *   **System Prompt**: 包含极其详细的 JSON Schema 定义、业务规则（如“冲突降级法”、“事实优先原则”）和格式禁令。
    *   **User Prompt**: 注入上述结构化数据。

### 1.2 信息输出 (Output / Response Parsing)
**核心模块**: `app.ai.analyst`

LLM 被要求输出严格的 JSON 格式，包含：
*   **Market Regime**: 市场状态（趋势/震荡）。
*   **Signal**: 交易信号（方向、入场、止盈、止损、置信度）。
*   **Evidence & Anchors**: **关键设计**。
    *   `evidence`: 必须引用具体的指标数值作为论据。
    *   `anchors`: 必须提供 JSON Path 和对应的原始数值，用于后续校对。
*   **Scenarios**: 多空剧本推演。
*   **YouTube Reflection**: 对外部观点的采纳或反驳状态。

### 1.3 事实校对 (Fact Grounding / Validation)
**核心模块**: `app.ai.grounding.engine`

系统实现了一个 **Grounding Engine**，包含 7 层验证器，确保 LLM 没有产生幻觉：
1.  **AnchorPathValidator**: 验证 `anchors` 中的路径是否存在于输入 JSON 中。
2.  **AnchorValueToleranceValidator**: 验证引用的数值是否与输入一致（允许微小误差）。
3.  **EvidenceMetricNearestMatchValidator**: 检查 `evidence` 文本中提及的数值是否能在事实源中找到对应指标。
4.  **RangePlausibilityValidator**: 检查价格、RSI 等数值是否在合理区间。
5.  **TimeframeCoherenceValidator**: 检查分析的时间周期是否匹配。
6.  **CrossFieldConsistencyValidator**: 检查入场/止损方向是否逻辑自洽。
7.  **CoverageQualityValidator**: 检查引用证据的覆盖面。

---

## 2. 信息过载问题分析

“信息过载”在当前架构中主要体现在两个方面：

1.  **LLM 上下文压力 (Context Load)**:
    *   **Schema 定义过重**: System Prompt 花费了大量 Token 来定义 JSON Schema 和复杂的业务规则。
    *   **冗余指标**: 5 个时间周期 x 20+ 个指标 = ~100 个数值字段。虽然不如原始 K 线大，但在高频调用下依然是负担。部分指标（如 `ret_1m`, `ret_3m`, `ret_5m`）之间存在高度相关性，可能冗余。
    *   **文本噪声**: YouTube 和 Alerts 的文本描述虽然经过截断，但相对于结构化数据，它们的信息密度较低，Token 消耗较高。

2.  **注意力稀释 (Attention Dilution)**:
    *   过多的输入字段可能导致 LLM “迷失”，即忽略关键信号而关注次要指标。
    *   复杂的指令（如“必须引用 path”）增加了推理负担，可能挤占了真正用于市场分析的“智力预算”。

---

## 3. 优化方案建议

针对上述问题，提出以下分阶段优化方案：

### 3.1 阶段一：Schema 与 Prompt 瘦身 (低成本，高收益)

*   **精简 JSON Schema**:
    *   移除 `anchors` 字段中过于繁琐的 `path` 要求。现在的 LLM (GPT-4o, Sonnet 3.5) 在遵循事实方面已经很强，可以改为只要求在 `reasoning` 中引用数值，由 Grounding Engine 进行模糊匹配验证，而不是强制 LLM 输出 JSON Path。
    *   简化 `evidence` 结构，不再要求 `metrics: {name: value}` 这种冗余结构，直接在文本中体现即可。
*   **压缩 System Prompt**:
    *   使用 TypeScript Interface 定义 Schema（比 JSON Schema 更省 Token）。
    *   移除部分显而易见的常识性规则。

### 3.2 阶段二：指标特征选择 (Feature Selection)

*   **移除低效指标**:
    *   保留 `RSI`, `MACD`, `Bollinger Bandwidth`, `ATR`, `Volume Z-Score`, `EMA Trend`。
    *   移除 `Stoch RSI` (与 RSI 高度相关)，移除部分 `Returns` (只保留 1m 和 1h)。
    *   仅在 `1h` 和 `4h` 周期保留完整指标，`15m` 和 `5m` 仅保留价格和核心动量指标，`1m` 仅保留价格。
*   **动态 Context**:
    *   如果 `Data Quality` 为 `POOR`，直接跳过大部分指标输入，只给价格，强迫 LLM 输出观望。

### 3.3 阶段三：分层分析架构 (架构级调整)

*   **两阶段分析 (CoT 分离)**:
    *   **Step 1 (Scanner)**: 使用轻量模型 (Flash/Haiku) 快速扫描 4h/1h 结构，判断是否值得交易。如果判断为“垃圾时间”，直接返回 HOLD，不进入 Step 2。
    *   **Step 2 (Deep Dive)**: 仅在 Step 1 认为有机会时，调用主力模型，传入完整上下文进行精细分析。
*   **RAG 动态加载**:
    *   不再一次性把 YouTube 观点全部塞入。只有当 LLM 在 Step 1 中对方向犹豫不决时，才在 Step 2 中引入外部观点作为辅助。

### 3.4 实施路线图

1.  **立即执行**: 修改 `prompts.py`，使用 TypeScript 风格简化 Schema 定义，移除 `anchors` 的强制 Path 要求（改为值匹配）。
2.  **短期执行**: 在 `market_context_builder.py` 中过滤掉 `1m` 和 `5m` 周期的大部分次要指标。
3.  **长期执行**: 实现 Scanner -> Deep Dive 的两阶段分析逻辑。
