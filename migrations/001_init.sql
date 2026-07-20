-- Tracer bullet schema: events, jobs, attempts, llm_calls.
-- ponytail: no archival/partitioning of events/attempts (out of scope, plan §9).

CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE events (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source          TEXT NOT NULL,
    type            TEXT NOT NULL,
    payload         JSONB NOT NULL,
    idempotency_key TEXT NOT NULL UNIQUE,
    trace_id        TEXT NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE jobs (
    id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_id          UUID NOT NULL REFERENCES events(id),
    handler_name      TEXT NOT NULL,
    state             TEXT NOT NULL DEFAULT 'pending',
    attempt_count     INT NOT NULL DEFAULT 0,
    run_after         TIMESTAMPTZ NOT NULL DEFAULT now(),
    lease_expires_at  TIMESTAMPTZ,
    variant           TEXT,
    trace_id          TEXT NOT NULL,
    replay_of         UUID REFERENCES jobs(id),
    last_error_class  TEXT,
    last_error        TEXT,
    output            JSONB,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Hot path for the claim query (plan §1: only pending rows with run_after<=now()).
CREATE INDEX idx_jobs_claimable ON jobs (run_after) WHERE state = 'pending';
CREATE INDEX idx_jobs_state ON jobs (state);

CREATE TABLE attempts (
    id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id         UUID NOT NULL REFERENCES jobs(id),
    attempt_number INT NOT NULL,
    state          TEXT NOT NULL DEFAULT 'running',
    started_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    ended_at       TIMESTAMPTZ,
    duration_ms    INT,
    error_class    TEXT,
    error          TEXT,
    output         JSONB,
    -- Rollup columns over this attempt's llm_calls rows (plan §1, §6.3). Not
    -- independently written; the tracer-bullet handler makes no llm_calls, so
    -- these stay at their defaults.
    cost_micros    BIGINT NOT NULL DEFAULT 0,
    input_tokens   INT NOT NULL DEFAULT 0,
    output_tokens  INT NOT NULL DEFAULT 0
);

CREATE INDEX idx_attempts_job_id ON attempts (job_id);

CREATE TABLE llm_calls (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    attempt_id          UUID NOT NULL REFERENCES attempts(id),
    attempt_idx         INT NOT NULL,
    model               TEXT NOT NULL,
    escalated_from      TEXT,
    input_tokens        INT NOT NULL DEFAULT 0,
    output_tokens       INT NOT NULL DEFAULT 0,
    cache_read_tokens   INT NOT NULL DEFAULT 0,
    cache_write_tokens  INT NOT NULL DEFAULT 0,
    thinking_tokens     INT NOT NULL DEFAULT 0,
    cost_micros         BIGINT NOT NULL DEFAULT 0,
    gate_score          REAL,
    latency_ms          INT,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_llm_calls_attempt_id ON llm_calls (attempt_id);
