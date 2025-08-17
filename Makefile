.PHONY: run all clean

run:
	. .venv/bin/activate && \
	python -m pipelines.ingest_rss && \
	python -m pipelines.extract && \
	python -m pipelines.dedupe_cluster && \
	python -m pipelines.summarize && \
	python -m pipelines.publish && \
	python -m pipelines.publish_weekly && \
	python -m pipelines.index_search
