.PHONY: help install test bench-replay bench-check

help:
	@echo "install       install dependencies (uv)"
	@echo "test          run the spine test suite (needs Postgres, see README)"
	@echo "bench-replay  recompute bench/RESULTS.md from committed raw data (offline)"
	@echo "bench-check   fail if the committed bench/RESULTS.md is stale"

install:
	uv sync

test:
	uv run pytest -q

# Offline: reads only bench/data/raw/*.jsonl. No API key, no model calls.
bench-replay:
	uv run python -m bench.replay

bench-check:
	uv run python -m bench.replay --check
