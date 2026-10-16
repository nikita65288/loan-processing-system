CREATE TABLE saga_orchestrator.saga_instances
(
    saga_id        UUID PRIMARY KEY,
    application_id UUID NOT NULL CONSTRAINT uq_saga_instances_application_id UNIQUE,
    customer_id    UUID NOT NULL,
    amount         NUMERIC(19, 2) NOT NULL,
    term           INTEGER NOT NULL,
    currency       VARCHAR(3) NOT NULL,
    status         VARCHAR(32) NOT NULL,
    current_step   VARCHAR(32) NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL,
    updated_at     TIMESTAMPTZ NOT NULL,
    version        BIGINT NOT NULL DEFAULT 0,
    CONSTRAINT chk_saga_status CHECK (status IN ('IN_PROGRESS', 'COMPLETED', 'FAILED')),
    CONSTRAINT chk_saga_step CHECK (current_step IN ('SCORING', 'BUREAU', 'DECISION', 'FINISHED'))
);

CREATE TABLE saga_orchestrator.saga_outbox_events
(
    id             UUID PRIMARY KEY,
    saga_id        UUID NOT NULL REFERENCES saga_orchestrator.saga_instances (saga_id),
    application_id UUID NOT NULL,
    command_type   VARCHAR(128) NOT NULL,
    topic          VARCHAR(128) NOT NULL,
    payload        TEXT NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL,
    status         VARCHAR(32) NOT NULL,
    retry_count    INTEGER NOT NULL DEFAULT 0,
    claimed_at     TIMESTAMPTZ,
    published_at   TIMESTAMPTZ,
    last_error     TEXT,
    CONSTRAINT chk_saga_outbox_status CHECK (status IN ('NEW', 'CLAIMED', 'PUBLISHED', 'FAILED')),
    CONSTRAINT chk_saga_outbox_retries CHECK (retry_count >= 0),
    CONSTRAINT chk_saga_outbox_publication CHECK (
       (status = 'PUBLISHED' AND published_at IS NOT NULL)
       OR (status <> 'PUBLISHED' AND published_at IS NULL)
    ),
    CONSTRAINT chk_saga_outbox_claim CHECK (
       (status = 'CLAIMED' AND claimed_at IS NOT NULL)
       OR (status <> 'CLAIMED' AND claimed_at IS NULL)
    )
);

CREATE INDEX idx_saga_outbox_status_created_at
    ON saga_orchestrator.saga_outbox_events (status, created_at);
CREATE INDEX idx_saga_outbox_status_claimed_at
    ON saga_orchestrator.saga_outbox_events (status, claimed_at);
CREATE INDEX idx_saga_outbox_application_id
    ON saga_orchestrator.saga_outbox_events (application_id);
