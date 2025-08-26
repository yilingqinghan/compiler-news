#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
. .venv/bin/activate
# 最近1天的小步处理（不强制用大模型，节省资源）
export TIME_WINDOW_DAYS=1
export NO_LLM=${NO_LLM:-1}
python -m pipelines.ingest_rss
python -m pipelines.extract
python -m pipelines.dedupe_cluster
python -m pipelines.summarize
python -m pipelines.publish         # 生成当天HTML（非周报）