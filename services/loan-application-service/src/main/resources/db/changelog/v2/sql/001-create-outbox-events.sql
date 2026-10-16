CREATE TABLE outbox_events
(
    id             UUID          NOT NULL,
    aggregate_type VARCHAR(64)  NOT NULL,
    aggregate_id   UUID          NOT NULL,
    event_type     VARCHAR(128) NOT NULL,
    payload        TEXT         NOT NULL,
    created_at     TIMESTAMPTZ  NOT NULL,
    status         VARCHAR(32)  NOT NULL,
    published_at   TIMESTAMPTZ,

    CONSTRAINT pk_outbox_events
        PRIMARY KEY (id),

    CONSTRAINT chk_outbox_event_status
        CHECK (status IN ('NEW', 'PUBLISHED')),

    CONSTRAINT chk_outbox_event_published_at
        CHECK (
            (status = 'NEW' AND published_at IS NULL)
            OR
            (status = 'PUBLISHED' AND published_at IS NOT NULL)
        )
);

CREATE INDEX idx_outbox_events_status_created_at
    ON outbox_events (status, created_at);
