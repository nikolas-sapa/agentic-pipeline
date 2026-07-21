-- Observability (capability 2, plan §4/§5; 00-SEAMS D1): task_metrics has
-- no writer of its own — jobs/attempts/llm_calls already have exactly one
-- writer each, this view only reads. alerts is the one new table this
-- slice does write to.

CREATE VIEW task_metrics AS
SELECT
    j.id AS task_id,
    j.variant,
    j.handler_name AS task_type,
    j.state AS terminal_status,
    j.created_at AS started_at,
    a.ended_at AS finished_at,
    -- LAST attempt's duration, not end-to-end task time: a job that retried
    -- twice reports only its winning run, excluding failed attempts and the
    -- backoff between them. finished_at - started_at is the wall-clock figure.
    a.duration_ms,
    j.attempt_count,
    COALESCE(c.llm_call_count, 0) AS llm_call_count,
    COALESCE(c.input_tokens, 0) AS input_tokens,
    COALESCE(c.output_tokens, 0) AS output_tokens,
    COALESCE(c.cache_read_tokens, 0) AS cache_read_tokens,
    COALESCE(c.cache_write_tokens, 0) AS cache_write_tokens,
    COALESCE(c.cost_micros, 0) / 1000000.0 AS cost_usd,
    COALESCE(c.models_used, ARRAY[]::text[]) AS models_used
FROM jobs j
LEFT JOIN LATERAL (
    -- last attempt for the job
    SELECT ended_at, duration_ms
    FROM attempts
    WHERE job_id = j.id
    ORDER BY attempt_number DESC
    LIMIT 1
) a ON true
LEFT JOIN LATERAL (
    -- rollup over llm_calls for every attempt of this job
    SELECT
        count(*) AS llm_call_count,
        sum(input_tokens) AS input_tokens,
        sum(output_tokens) AS output_tokens,
        sum(cache_read_tokens) AS cache_read_tokens,
        sum(cache_write_tokens) AS cache_write_tokens,
        sum(cost_micros) AS cost_micros,
        array_agg(DISTINCT model) AS models_used
    FROM llm_calls
    WHERE attempt_id IN (SELECT id FROM attempts WHERE job_id = j.id)
) c ON true
WHERE j.state IN ('succeeded', 'dead_letter');

CREATE TABLE alerts (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    ts          TIMESTAMPTZ NOT NULL DEFAULT now(),
    kind        TEXT NOT NULL,
    task_id     UUID,
    detail_json JSONB NOT NULL,
    variant     TEXT
);

CREATE INDEX idx_alerts_kind ON alerts (kind);
