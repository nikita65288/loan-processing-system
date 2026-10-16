ALTER TABLE outbox_events
    ADD COLUMN retry_count INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN last_error TEXT,
    ADD COLUMN claimed_at TIMESTAMPTZ;

ALTER TABLE outbox_events
    DROP CONSTRAINT chk_outbox_event_status;

ALTER TABLE outbox_events
    ADD CONSTRAINT chk_outbox_event_status
        CHECK (status IN ('NEW', 'CLAIMED', 'PUBLISHED', 'FAILED'));

ALTER TABLE outbox_events
    ADD CONSTRAINT chk_outbox_event_retry_count
        CHECK (retry_count >= 0);

ALTER TABLE outbox_events
    DROP CONSTRAINT chk_outbox_event_published_at;

ALTER TABLE outbox_events
    ADD CONSTRAINT chk_outbox_event_published_at
        CHECK (
            (status = 'PUBLISHED' AND published_at IS NOT NULL)
            OR
            (status IN ('NEW', 'CLAIMED', 'FAILED') AND published_at IS NULL)
        );

ALTER TABLE outbox_events
    ADD CONSTRAINT chk_outbox_event_claimed_at
        CHECK (
            (status = 'CLAIMED' AND claimed_at IS NOT NULL)
            OR
            (status IN ('NEW', 'PUBLISHED', 'FAILED') AND claimed_at IS NULL)
        );

CREATE INDEX idx_outbox_events_status_claimed_at
    ON outbox_events (status, claimed_at);

CREATE INDEX idx_outbox_events_status_published_at
    ON outbox_events (status, published_at);
