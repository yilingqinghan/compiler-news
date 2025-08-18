# ===== 控制开关 =====
NO_LLM ?= 0       # 1: 跳过 summarize（复用旧摘要）
NO_INGEST ?= 0    # 1: 跳过 ingest/extract/dedupe（复用旧库）

PY := . .venv/bin/activate && python -m

run:
	@if [ "$(NO_INGEST)" != "1" ]; then \
		$(PY) pipelines.ingest_rss; \
		$(PY) pipelines.extract; \
		$(PY) pipelines.dedupe_cluster; \
	else \
		echo "[skip] ingest/extract/dedupe"; \
	fi; \
	if [ "$(NO_LLM)" != "1" ]; then \
		$(PY) pipelines.summarize; \
	else \
		echo "[skip] summarize (reuse existing summaries)"; \
	fi; \
	$(PY) pipelines.publish; \
	$(PY) pipelines.publish_weekly; \
	$(PY) pipelines.index_search

# 仅重渲染页面/检索，完全复用数据库与旧摘要（最快）
republish:
	$(PY) pipelines.publish
	$(PY) pipelines.publish_weekly
	$(PY) pipelines.index_search

# 只做抓取+抽取+聚类（不调 LLM）
ingest-only:
	$(PY) pipelines.ingest_rss
	$(PY) pipelines.extract
	$(PY) pipelines.dedupe_cluster

# 仅 UI（当你只改了模板/样式）
ui:
	$(PY) pipelines.publish_weekly

.PHONY: run republish ingest-only ui