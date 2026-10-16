-- Existing IN_PROGRESS Saga remain untracked until explicitly reconciled; no invented deadline.
ALTER TABLE saga_orchestrator.saga_instances
    ADD COLUMN failure_reason VARCHAR(64),
    ADD COLUMN failed_at TIMESTAMPTZ;

CREATE TABLE saga_orchestrator.saga_step_attempts (
    id UUID PRIMARY KEY,
    saga_id UUID NOT NULL REFERENCES saga_orchestrator.saga_instances(saga_id) ON DELETE CASCADE,
    step VARCHAR(32) NOT NULL,
    attempt_number INTEGER NOT NULL,
    command_id UUID NOT NULL,
    outbox_event_id UUID NOT NULL UNIQUE,
    command_type VARCHAR(128) NOT NULL,
    topic VARCHAR(128) NOT NULL,
    payload TEXT NOT NULL,
    timeout_seconds BIGINT NOT NULL,
    backoff_seconds BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    published_at TIMESTAMPTZ,
    deadline_at TIMESTAMPTZ,
    retry_after_at TIMESTAMPTZ,
    CONSTRAINT uq_saga_step_attempt UNIQUE (saga_id, step, attempt_number),
    CONSTRAINT chk_saga_step_attempt_step CHECK (step IN ('SCORING', 'BUREAU', 'DECISION')),
    CONSTRAINT chk_saga_attempt_number CHECK (attempt_number >= 1),
    CONSTRAINT chk_saga_attempt_timeouts CHECK (timeout_seconds > 0 AND backoff_seconds >= 0),
    CONSTRAINT chk_saga_attempt_deadline CHECK (
       (published_at IS NULL AND deadline_at IS NULL AND retry_after_at IS NULL)
       OR (published_at IS NOT NULL AND deadline_at IS NOT NULL AND retry_after_at IS NOT NULL))
);

-- Scheduler scans due rows and checks the latest delivery using the unique key above.
CREATE INDEX idx_saga_attempt_deadline
    ON saga_orchestrator.saga_step_attempts(deadline_at, saga_id)
    WHERE published_at IS NOT NULL;
