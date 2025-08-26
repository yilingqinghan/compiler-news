# -------- compiler-intel / Makefile --------

# 你也可以在命令前临时传入环境变量：
#   WINDOW_MODE=week_to_date NO_LLM=1 make run
#   WINDOW_MODE=last_week make weekly

PY      := python
VENV    := .venv
ACT     := . $(VENV)/bin/activate

export PYTHONUNBUFFERED = 1

.PHONY: help init deps preflight run weekly daily republish ui archive clean docker-up docker-down meili-up pg-up

help:
	@echo ""
	@echo "Targets:"
	@echo "  init         - 创建虚拟环境并安装依赖"
	@echo "  preflight    - 仅做一次性前置检查（Docker/DB/Meili/LLM）"
	@echo "  run          - 全流程（抓取->抽取->聚类->总结->发布->周报->索引）"
	@echo "  weekly       - 生成周报（脚本）+ 归档"
	@echo "  daily        - 每日小步抓取（脚本）"
	@echo "  republish    - 复用库中摘要，重新发布周报 + 索引（不抓取）"
	@echo "  ui           - 仅用模板重新渲染本周周报（最快）"
	@echo "  archive      - 扫描 web/dist 生成 index.html 归档页"
	@echo "  docker-up    - docker compose up -d（可选）"
	@echo "  docker-down  - docker compose down"
	@echo ""
	@echo "Env vars:"
	@echo "  WINDOW_MODE=rolling|week_to_date|last_week"
	@echo "  TIME_WINDOW_DAYS=N"
	@echo "  NO_LLM=1（不调用模型）"
	@echo ""

# ---- 安装依赖 ----
init: deps
deps:
	python3 -m venv $(VENV)
	$(ACT) && pip install -U pip wheel setuptools
	$(ACT) && pip install -r requirements.txt

# ---- 一次性前置检查（失败将直接退出，后续目标不会执行）----
preflight:
	$(ACT) && PYTHONTRACEBACK=0 $(PY) -m scripts.preflight

# ---- 全流程（只有在 preflight 通过的情况下才会继续）----
run: preflight
	$(ACT) && PYTHONTRACEBACK=0 $(PY) -m pipelines.ingest_rss && \
	$(PY) -m pipelines.extract && \
	$(PY) -m pipelines.dedupe_cluster && \
	$(PY) -m pipelines.summarize && \
	$(PY) -m pipelines.publish && \
	$(PY) -m pipelines.publish_weekly && \
	$(PY) -m pipelines.index_search

# ---- 每周/每日脚本（更快）----
weekly: preflight
	$(ACT) && bash scripts/run_weekly.sh && \
	$(MAKE) archive

daily: preflight
	$(ACT) && bash scripts/run_daily.sh

# ---- 仅重发页面（复用摘要与数据库，速度快）----
republish: preflight
	$(ACT) && $(PY) -m pipelines.publish && \
	$(PY) -m pipelines.publish_weekly && \
	$(PY) -m pipelines.index_search

# ---- 仅渲染周报 UI（模板改动后最快验证）----
ui: preflight
	$(ACT) && $(PY) -m pipelines.publish_weekly

# ---- 归档页（扫描 web/dist）----
archive:
	$(ACT) && $(PY) scripts/build_archive.py

# ---- 辅助 ----
docker-up:
	docker compose up -d

docker-down:
	docker compose down

clean:
	rm -rf web/dist/* __pycache__ .pytest_cache