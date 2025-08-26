#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
. .venv/bin/activate
# 以7天窗口生成“上一个自然周”的周报
export TIME_WINDOW_DAYS=7
# 可选：若你想严格按“周一 00:00 ~ 周日 23:59”切分 → 周一 00:10 执行本脚本即可
unset NO_LLM   # 周报建议让模型认真总结
python -m pipelines.summarize
python -m pipelines.publish_weekly
python -m pipelines.index_search