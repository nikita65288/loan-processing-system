CREATE TABLE notification.notification_deliveries (
 id UUID PRIMARY KEY,application_id UUID NOT NULL UNIQUE,
 command_id UUID NOT NULL,customer_id UUID NOT NULL,
 notification_type VARCHAR(64) NOT NULL,status VARCHAR(24) NOT NULL,
 created_at TIMESTAMPTZ NOT NULL,completed_at TIMESTAMPTZ NOT NULL,
 CONSTRAINT chk_delivery_status CHECK (status IN ('SENT_SIMULATED'))
);
CREATE TABLE notification.processed_commands (
 command_id UUID PRIMARY KEY,application_id UUID NOT NULL,request_fingerprint TEXT NOT NULL,created_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX idx_notification_commands_app ON notification.processed_commands(application_id);
CREATE TABLE notification.outbox_events (
 id UUID PRIMARY KEY,application_id UUID NOT NULL,topic VARCHAR(128) NOT NULL,
 payload TEXT NOT NULL,status VARCHAR(16) NOT NULL,created_at TIMESTAMPTZ NOT NULL,
 claimed_at TIMESTAMPTZ,published_at TIMESTAMPTZ,retry_count INT NOT NULL DEFAULT 0,last_error TEXT,
 CONSTRAINT chk_notification_outbox_status CHECK (status IN ('NEW','CLAIMED','PUBLISHED','FAILED')),
 CONSTRAINT chk_notification_outbox_claim CHECK ((status='CLAIMED') = (claimed_at IS NOT NULL)),
 CONSTRAINT chk_notification_outbox_publication CHECK ((status='PUBLISHED') = (published_at IS NOT NULL))
);
CREATE INDEX idx_notification_outbox_pending ON notification.outbox_events(status,created_at);
