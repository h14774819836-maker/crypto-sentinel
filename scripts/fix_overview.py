import os

path = r'h:\杂项\代码垃圾\trade\crypto_sentinel\app\web\templates\overview.html'

with open(path, 'rb') as f:
    byte_content = f.read()

# We can decode with 'replace'
content = byte_content.decode('utf-8', errors='replace')

replacements = [
    ('<span>LLM 调试\ufffd/span>', '<span data-i18n="nav.llm_debug">LLM 调试器</span>'),
    ('<span>LLM 调试?/span>', '<span data-i18n="nav.llm_debug">LLM 调试器</span>'),
    ('实时市场趋势、AI 交易信号与自动告警\ufffd/p>', '实时市场趋势、AI 交易信号与自动告警。</p>'),
    ('实时市场趋势、AI 交易信号与自动告警?/p>', '实时市场趋势、AI 交易信号与自动告警。</p>'),
    ('1分钟 \ufffd/th>', '1分钟 ▲</th>'),
    ('10分钟 \ufffd/th>', '10分钟 ▲</th>'),
    ('1分钟 ?/th>', '1分钟 ▲</th>'),
    ('10分钟 ?/th>', '10分钟 ▲</th>'),
    ('波动\ufffd(20)</th>', '波动率 (20)</th>'),
    ('波动?(20)</th>', '波动率 (20)</th>'),
    ('暂无市场数据，请稍后刷新\ufffd/span>', '暂无市场数据，请稍后刷新。</span>'),
    ('暂无市场数据，请稍后刷新?/span>', '暂无市场数据，请稍后刷新。</span>'),
    ('开始分\ufffd/span>', '开始分析</span>'),
    ('开始分?/span>', '开始分析</span>'),
    ('<span class="text-xs text-muted-foreground mb-1 block">入场\ufffd/span>', '<span class="text-xs text-muted-foreground mb-1 block" data-i18n="overview.table.entry">入场点</span>'),
    ('<span class="text-xs text-muted-foreground mb-1 block">入场?/span>', '<span class="text-xs text-muted-foreground mb-1 block" data-i18n="overview.table.entry">入场点</span>'),
    ('未生\ufffdAI 信号，请检\ufffdDEEPSEEK_ENABLED 配置\ufffd/span>', '未生成 AI 信号，请检查相关的 AI 配置。</span>'),
    ('未生?AI 信号，请检?DEEPSEEK_ENABLED 配置?/span>', '未生成 AI 信号，请检查相关的 AI 配置。</span>'),
    ('近期无告警\ufffd/p>', '近期无告警。</p>'),
    ('近期无告警?/p>', '近期无告警。</p>'),
    ('中\ufffd/span>', '中性</span>'),
    ('中?/span>', '中性</span>'),
    ('置信\ufffd/span>', '置信度</span>'),
    ('置信?/span>', '置信度</span>'),
    ('支撑\ufffd/div>', '支撑位</div>'),
    ('支撑?/div>', '支撑位</div>'),
    ('阻力\ufffd/div>', '阻力位</div>'),
    ('阻力?/div>', '阻力位</div>'),
    ('暂无共识数据\ufffd/p>', '暂无共识数据。</p>'),
    ('暂无共识数据?/p>', '暂无共识数据。</p>')
]

for old, new in replacements:
    content = content.replace(old, new)

with open(path, 'w', encoding='utf-8') as f:
    f.write(content)

print(f"Fixed overview.html")
