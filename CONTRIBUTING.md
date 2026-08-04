# Contributing

## Setup

```
uv sync              # or: make install
```

Requires Python >=3.12 (developed on 3.14) and [uv](https://docs.astral.sh/uv/).

## Postgres

The spine needs a real Postgres instance; `SKIP LOCKED` and `ON CONFLICT`
behavior is not mockable, and the test suite runs against it directly.

```
docker run -d --name spine-pg -p 5432:5432 \
  -e POSTGRES_USER=spine -e POSTGRES_PASSWORD=spine -e POSTGRES_DB=spine \
  postgres:16

for f in migrations/*.sql; do
  psql postgresql://spine:spine@localhost:5432/spine -f "$f"
done
```

`DATABASE_URL` defaults to `postgresql://spine:spine@localhost:5432/spine`;
export it to point elsewhere. `docker-compose.yml` in the repo root brings up
only the OTLP trace viewer (Phoenix, port 6006), not Postgres.

## Running the spine

```
uv run uvicorn spine.ingress:app --reload
uv run python -m spine.worker
```

## Tests

```
make test             # pytest -q, against the Postgres above
```

## The benchmark

`bench/` holds the measurement that killed the cost-routing feature (see
README.md). To reproduce the published numbers:

```
make bench-replay     # recomputes bench/RESULTS.md from committed raw data, offline, ~0.1s
make bench-check      # fails if RESULTS.md is stale relative to the raw data
```

`bench-replay` needs no API key, no network, and no model calls; it reads only
`bench/data/raw/*.jsonl`. Re-running the live benchmark spends real API quota
and takes about an hour — see `bench/README.md` before doing that.

## Layout

```
src/spine/       ingress (FastAPI), queue, worker, handler registry, model
migrations/      plain .sql, applied in order
tests/           against real Postgres
bench/           the benchmark: replay.py, prices.py, RESULTS.md, live/, data/
```

## Pull requests

- Keep PRs scoped to one change. Explain the "why," not just the "what."
- Add or update tests for behavior changes; `make test` must pass.
- If a change touches `bench/`, do not hand-edit `RESULTS.md` — regenerate it
  with `make bench-replay` and commit the output.
- Match the existing style: plain SQL migrations, no ORM, real Postgres in
  tests, no invented abstractions ahead of a second use case.
