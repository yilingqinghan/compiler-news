.PHONY: run all clean

run:
	python -m pipelines.ingest_rss && \
	python -m pipelines.extract && \
	python -m pipelines.dedupe_cluster && \
	python -m pipelines.summarize && \
	python -m pipelines.publish
