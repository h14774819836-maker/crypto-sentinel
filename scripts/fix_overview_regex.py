import re
import os

path = r'h:\杂项\代码垃圾\trade\crypto_sentinel\app\web\templates\overview.html'

with open(path, 'r', encoding='utf-8', errors='ignore') as f:
    content = f.read()

replacements = {
    r'LLM 调试.*?/span>': r'<span data-i18n="nav.llm_debug">LLM 调试器</span>',
    r'实时市场趋势、AI 交易信号与自动告警.*?/p>': r'实时市场趋势、AI 交易信号与自动告警。</p>',
    r'<th data-i18n="overview.table.symbol">交易.*?/th>': r'<th data-i18n="overview.table.symbol">交易对</th>',
    r'<th data-i18n="overview.table.1m">1分钟 .*?/th>': r'<th data-i18n="overview.table.1m">1分钟 ▲</th>',
    r'<th data-i18n="overview.table.10m">10分钟 .*?/th>': r'<th data-i18n="overview.table.10m">10分钟 ▲</th>',
    r'<th data-i18n="overview.table.vol">波动.*?(20)</th>': r'<th data-i18n="overview.table.vol">波动率 (20)</th>',
    r'暂无市场数据，请稍后刷新.*?/span>': r'暂无市场数据，请稍后刷新。</span>',
    r'开始分.*?/span>': r'开始分析</span>',
    r'<span class="text-xs text-muted-foreground mb-1 block">入场.*?/span>': r'<span class="text-xs text-muted-foreground mb-1 block" data-i18n="overview.table.entry">入场点</span>',
    r'未生.*AI 信号，请检.*DEEPSEEK_ENABLED 配置.*?/span>': r'未生成 AI 信号，请检查相关 AI 配置。</span>',
    r'近期无告警.*?/p>': r'近期无告警。</p>',
    r'中.*?/span>': r'中性</span>',
    r'置信.*?/span>': r'置信度</span>',
    r'支撑.*?/div>': r'支撑位</div>',
    r'阻力.*?/div>': r'阻力位</div>',
    r'暂无共识数据.*?/p>': r'暂无共识数据。</p>'
}

for pattern, repl in replacements.items():
    content = re.sub(pattern, repl, content, flags=re.DOTALL)

with open(path, 'w', encoding='utf-8') as f:
    f.write(content)

print("Regex fixes applied to overview.html")
