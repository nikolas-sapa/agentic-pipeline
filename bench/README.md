# bench/

The measurement that decided whether this pipeline should route cheap requests
to a cheaper model. Results: [`RESULTS.md`](RESULTS.md), generated, never hand
edited.

```
make bench-replay    # recompute RESULTS.md from committed raw data, offline, ~0.1s
make bench-check     # fail if the committed RESULTS.md is stale
```

## Layout

```
prices.py             price table + the derive_usd function; the single place a dollar figure comes from
replay.py             offline recompute; generates RESULTS.md
paths.py              file locations, resolved from the package rather than cwd
data/tasks/           the three sampled n=50 task sets (committed)
data/raw/             per-call rows: tokens, derived $, parse/correct, stop reason, full model output (committed)
data/logs/            stderr from the three live runs, as they ran
live/runner.py        shared CLI invocation, retry/backoff, cost derivation
live/run_*.py         per-family prompt, output contract, grader
live/build_tasksets.py  rebuilds data/tasks/ from source corpora, sha256-checked
```

`replay.py` reads `data/raw/` and nothing else. It needs no network, no API key
and no model. It is stdlib only, so it also runs without uv at all:
`python3 -m bench.replay` from the repo root. Everything else in this directory is the live path, which spends
real quota and is not needed to verify a single number in `RESULTS.md`.

## The substrate, and what it costs the result

Every model call went through the **Claude Code CLI in headless mode, against
an interactive subscription**, not a metered API key. That was a hard
constraint of the project, and it is worth being blunt about the consequences:

- A subscription substrate is intended for interactive use. Running 300 scripted
  calls through it is not what it is for.
- The CLI's reported `total_cost_usd` is unusable as a benchmark figure. It
  includes auxiliary Claude Code calls the benchmark never requested --- a
  Sonnet-arm run repeatedly showed a Haiku entry in `modelUsage`, i.e. the
  substrate attributes Haiku overhead to the Sonnet arm, inverting the exact
  comparison being made. So the cost basis is `usage.input_tokens` /
  `usage.output_tokens` times a checked-in price table. It is recorded per row
  as `substrate_reported_cost_usd` anyway, so a reader can see the gap and see
  that the flattering number was not the one used.
- The CLI's own system prompt costs ~21,700 cache-creation input tokens per
  invocation and would have dominated the measurement 40x over.
  `--system-prompt` + `--setting-sources ""` + `--tools ""` suppress it down to
  ~177 input tokens. Without those flags the benchmark measures Claude Code,
  not the task.
- `--effort` is pinned to `low` on both arms. Unpinned, it moves Sonnet's output
  9.3x, which would make the arms incomparable.
- Thinking tokens are not broken out anywhere in the CLI's JSON. They are folded
  into `output_tokens` and cannot be separated.

**There is no metered-API path in this harness, and none is implemented.**
Porting `live/runner.py` to the Anthropic SDK is mechanically straightforward.
Validating it is the harder half: CLI and API token accounting are different
measurements, which is the whole reason the contamination note above exists.
Numbers from an API run are a new result, not an extension of this one, and
should be reported that way.

## Re-running live

Live runs overwrite `data/raw/`, spend quota, and take roughly an hour at
concurrency 2. They are not required for anything except producing new data.

```
uv run python -m bench.live.run_gsm8k
uv run python -m bench.live.run_mbpp
uv run python -m bench.live.run_mmlu_pro
make bench-replay
```

Rebuilding the task sets is only needed to verify the sampling or draw a new
one. The source corpora are not committed (4MB parquet, 750KB JSONL); fetch
them per the docstring in `live/build_tasksets.py`, which asserts their
sha256s and fails loudly on a different upstream revision. All three sampled
sets have been verified to rebuild byte-identically from the pinned corpora.
MMLU-Pro's parquet needs `pandas` + `pyarrow`, which the pipeline itself does
not depend on: `uv run --with pandas --with pyarrow python -m
bench.live.build_tasksets --corpora <dir>`.

## Two known grader gaps, deliberately not fixed

GSM8K's answer regex accepts only a bare number, so a correct
`ANSWER: $70,000` or `ANSWER: 160 minutes` fails to parse. MMLU-Pro's does not
strip markdown bold, so `**ANSWER: C**` fails. Both hit both arms.

They are left exactly as they ran, because fixing them would change what the
committed raw data means without a re-run. They are handled by reporting
accuracy conditional on parsing, and they do not touch the cost result: a call
that failed to parse still spent real, correctly counted tokens.

## Live MBPP safety

MBPP grading executes model-authored Python on the host. The separate
interpreter, socket monkeypatch and timeout do not sandbox filesystem, process
or network access. Run live grading only in a disposable sandbox without
sensitive files, credentials or host access. Offline replay does not execute
model-authored code.
