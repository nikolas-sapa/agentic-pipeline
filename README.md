# agentic-pipeline

[![License: MIT](https://img.shields.io/badge/license-MIT-F3F2EE?style=flat-square&labelColor=0B0B0D)](LICENSE)
[![Python >=3.12](https://img.shields.io/badge/python-3.12%2B-F3F2EE?style=flat-square&labelColor=0B0B0D)](pyproject.toml)
[![PRs welcome](https://img.shields.io/badge/PRs-welcome-F3F2EE?style=flat-square&labelColor=0B0B0D)](CONTRIBUTING.md)

An event-driven LLM request pipeline (Postgres-backed queue, idempotent
ingress, retry taxonomy, dead-letter, replay), and the benchmark that killed
its cost-routing feature.

## Quick start

```
make install                 # uv sync

# spine (needs Postgres; DATABASE_URL defaults to postgresql://spine:spine@localhost:5432/spine)
uv run uvicorn spine.ingress:app --reload
uv run python -m spine.worker
make test                    # requires disposable SPINE_TEST_DATABASE_URL

# the finding below, reproduced offline in ~0.1s, no API key or network
make bench-replay
```

See [`CONTRIBUTING.md`](CONTRIBUTING.md) for standing up Postgres locally.

## The finding

The pipeline was designed to route cheap requests to a cheaper model. Before
building that router, one gate had to pass: is the cheap model actually cheaper
per task? It is not.

Measured over 150 paired tasks, same prompt bytes, same flags, only `--model`
differing (figures below are as of the committed data; `bench/RESULTS.md` is
the generated source of truth):

| Family | Mean $/task, Sonnet 5 | Mean $/task, Haiku 4.5 | Ratio | Haiku cheaper on |
|---|---|---|---|---|
| GSM8K (arithmetic word problems) | 0.001285 | 0.002800 | **2.18x** | 1/50 |
| MBPP (Python function synthesis) | 0.001203 | 0.007321 | **6.08x** | 0/50 |
| MMLU-Pro (multiple choice) | 0.001642 | 0.011668 | **7.11x** | 0/50 |

Claude Haiku 4.5 lists at **half** Claude Sonnet 5's price per token in both
directions. It still cost 2 to 7 times more per task, and was cheaper on
**1 of 150** paired tasks. Quality was indistinguishable: every
conditional-accuracy confidence interval overlaps its counterpart, so this is
not a "you get what you pay for" result. It is a pricing-intuition result.
Cheap per token is not cheap per task, because the cheap model spends far more
tokens arriving at the same answer.

Full numbers, confidence intervals, and run integrity checks:
**[`bench/RESULTS.md`](bench/RESULTS.md)**, which is generated from the raw
per-call data and never hand edited.

### The mechanism

The obvious objection is that Haiku is only expensive because it emits verbose
output formats, and a tighter output contract would fix it. The data says
otherwise, because the two orderings invert.

Ranked by how short the required answer actually is:

```
MMLU-Pro (one letter)  <  GSM8K (a number)  <  MBPP (a function)
```

Ranked by Haiku's measured mean output tokens:

```
GSM8K (509.9)  <  MBPP (1414.2)  <  MMLU-Pro (2255.8)
```

The family demanding the **shortest** answer drew the **highest** spend, more
than the one that required writing code. Note this is not a clean reverse
trend either: GSM8K is mid-length by requirement and lowest by spend. The claim
is only the negative one, that required output length does not predict token
spend.

On MMLU-Pro the contract is about as strict as a contract gets ("Respond with
`ANSWER: <letter>` and nothing else"). Sonnet complies in a 74.1-token mean.
Haiku spends 2255.8. Three Haiku rows ran 6,620, 9,960 and 10,604 output tokens
before emitting a single letter. The excess is reasoning, not formatting, and
an output contract constrains the answer, not the reasoning that precedes it.
That is why tightening the prompt cannot close this gap.

### What this does not show

Stated up front rather than in a linked appendix, because the result is narrow:

- **n=50 per family, 150 pairs total.** The cost effect is large and paired, so
  it is robust; the accuracy point estimates are noise at this n.
- **One model pair.** Claude Sonnet 5 vs Claude Haiku 4.5. Nothing here
  transfers to another provider, another cheap model, or a future Haiku.
- **`--effort low` only.** Not tested at other effort levels.
- **One substrate.** Claude Code CLI, not the raw Anthropic API. See below.
- **Three short-answer, programmatically graded families**, all single-turn and
  single-call. No long-form generation, no multi-turn work, no tool-use loop.
- **Pricing is intro-rate sensitive.** Sonnet 5's $2/$10 runs through
  2026-08-31 and becomes $3/$15 on 2026-09-01. That change widens every ratio
  above in Haiku's favour; it does not close any of them.
- **Dollars are derived, not billed.** Token counts times a checked-in price
  table. No invoice was consulted.

## The substrate, stated plainly

Every model call ran through the **Claude Code CLI in headless mode against an
interactive subscription**, not a metered API key. That substrate is intended
for interactive use, and running 300 scripted calls through it is not what it
is for. It was a hard constraint of the project rather than a preference.

It also cost the measurement something specific. The CLI's own reported
`total_cost_usd` is unusable: it includes auxiliary Claude Code calls the
benchmark never requested, and was repeatedly observed attributing Haiku
overhead to the Sonnet arm, which inverts the exact comparison being made. So
the cost basis is `usage.input_tokens` / `usage.output_tokens` times the price
table in `bench/prices.py`. The substrate's own figure is still recorded per
row, so a reader can see the gap and see that the flattering number was not the
one used.

**There is no metered-API path in this harness, and none is implemented.**
Porting the runner to the Anthropic SDK is mechanically straightforward, but
that is not the same as validated: CLI and API token accounting are different
measurements, so a run through the API would have to be re-validated as a new
result rather than merged with these numbers. `bench/README.md` covers the substrate's other distortions (the CLI's
21,700-token system prompt, `--effort` moving Sonnet's output 9.3x, thinking
tokens not being separable) and the flags that suppress them.

## What the pipeline is

A durable job spine for LLM work, where the interesting failures are
duplicate execution and silent loss rather than throughput.

- **Idempotent ingress.** `POST /events` derives an idempotency key from
  `source` + provider event id, falling back to a payload hash. A unique index
  is the arbiter, not a read-then-write check. A duplicate POST returns the
  original `job_id` so the loser can see what the winner got.
- **Execution dedup at claim.** `FOR UPDATE SKIP LOCKED` with a lease. A
  distinct concern from ingress dedup, handled separately.
- **Failure taxonomy drives everything.** A `TerminalError` dead-letters
  immediately without burning attempts. Anything else retries with full-jitter
  backoff to `max_attempts`, then dead-letters with the last error preserved on
  the job row. An undeclared handler exception is treated as retryable, on the
  grounds that a handler bug should not be indistinguishable from bad input.
- **Lease reaper.** A crashed worker's job is reclaimed, its open attempt closed
  as `lease_expired`, and routed through the same retry-vs-dead-letter decision
  rather than a second code path. A reclaimed attempt cannot overwrite a newer
  attempt's result. Leases do not provide exactly-once external side effects:
  handlers must tolerate overlap if they run past the lease.
- **DLQ is a query, not a table.** `GET /jobs?state=dead_letter`.
- **Replay mints a new job** from the original event with `replay_of` lineage,
  predicated on the job being dead-lettered. The dead job is never resurrected.

This HTTP surface is for trusted local use. It has no authentication, request
limits or access controls; listing jobs exposes handler outputs and errors.
Do not expose it publicly without a separate access-control and resource-limit
design. The bundled observability viewer binds only to loopback.

The tests run against an explicitly configured disposable Postgres database
(`SPINE_TEST_DATABASE_URL`), and truncate its application tables. They never
fall back to the application's `DATABASE_URL`.

The tests run against real Postgres. Mocking `SKIP LOCKED` or `ON CONFLICT`
would test nothing.

### The router is a rejected policy, not missing work

Cost routing was designed, gated, measured, and dropped. The gate was written
before the data: mean derived $/task for the cheap arm must come in below the
expensive arm on the same task set, or the router has no premise and the
project reports that instead of a savings number. It came in 2 to 7 times
higher, so there is no router in `src/`.

Building it anyway would have shipped a component that made the system slower,
more complex, and more expensive, in exchange for a number that reads well.
The benchmark is the deliverable here; the absent router is the result of it.

## Running it

```
make install                 # uv sync

# spine (needs Postgres; DATABASE_URL defaults to postgresql://spine:spine@localhost:5432/spine)
uv run uvicorn spine.ingress:app --reload
uv run python -m spine.worker
make test                    # requires disposable SPINE_TEST_DATABASE_URL

# benchmark
make bench-replay            # recompute bench/RESULTS.md from committed raw data
make bench-check             # fail if RESULTS.md is stale
```

`make bench-replay` is offline. No API key, no network, no model calls, about
0.1 seconds. It reads only `bench/data/raw/*.jsonl` (300 rows carrying tokens,
parse and correctness flags, stop reason, and the full model output) and
regenerates every number in `RESULTS.md` from them. Re-deriving each row's
dollar figure from its token counts and comparing it against the value stored
at call time is part of the run, so a corrupted or reshuffled data file fails
loudly instead of quietly producing different headline numbers.

Re-running the benchmark live spends real quota and is documented in
[`bench/README.md`](bench/README.md), along with the sha256-checked task-set
builder. All three sampled task sets have been verified to rebuild
byte-identically from the pinned source corpora.

## Layout

```
src/spine/       ingress (FastAPI), queue, worker, handler registry, model
migrations/      plain .sql, applied in order
tests/           against real Postgres
bench/           the benchmark: replay.py, prices.py, RESULTS.md, live/, data/
```

## Contributing

Dev setup, running Postgres locally, tests, and reproducing the benchmark are
in [`CONTRIBUTING.md`](CONTRIBUTING.md). Bug reports and feature requests use
the issue templates; see [`SECURITY.md`](SECURITY.md) instead for
vulnerabilities.

## License

[MIT](LICENSE)
