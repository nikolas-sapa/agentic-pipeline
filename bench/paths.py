"""Where the benchmark's files live, resolved from this file rather than cwd."""

from pathlib import Path

BENCH_DIR = Path(__file__).resolve().parent
TASKS_DIR = BENCH_DIR / "data" / "tasks"
RAW_DIR = BENCH_DIR / "data" / "raw"
RESULTS_MD = BENCH_DIR / "RESULTS.md"
