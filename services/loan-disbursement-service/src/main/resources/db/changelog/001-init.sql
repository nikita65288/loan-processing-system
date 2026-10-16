CREATE TABLE loan_disbursement.disbursements (
 id UUID PRIMARY KEY, application_id UUID NOT NULL UNIQUE,
 command_id UUID NOT NULL, amount NUMERIC(19,2) NOT NULL,
 currency VARCHAR(3) NOT NULL, status VARCHAR(24) NOT NULL,
 created_at TIMESTAMPTZ NOT NULL, completed_at TIMESTAMPTZ NOT NULL,
 CONSTRAINT chk_disbursement_amount CHECK (amount > 0),
 CONSTRAINT chk_disbursement_status CHECK (status IN ('COMPLETED'))
);
CREATE TABLE loan_disbursement.processed_commands (
 command_id UUID PRIMARY KEY, application_id UUID NOT NULL,
 request_fingerprint TEXT NOT NULL, outcome VARCHAR(24) NOT NULL, created_at TIMESTAMPTZ NOT NULL,
 CONSTRAINT chk_disbursement_inbox_outcome CHECK (outcome IN ('PROCESSED','CONFLICT'))
);
CREATE INDEX idx_disbursement_commands_app ON loan_disbursement.processed_commands(application_id);
CREATE TABLE loan_disbursement.outbox_events (
 id UUID PRIMARY KEY, application_id UUID NOT NULL,topic VARCHAR(128) NOT NULL,
 payload TEXT NOT NULL,status VARCHAR(16) NOT NULL,created_at TIMESTAMPTZ NOT NULL,
 claimed_at TIMESTAMPTZ,published_at TIMESTAMPTZ,retry_count INT NOT NULL DEFAULT 0,last_error TEXT,
 CONSTRAINT chk_disbursement_outbox_status CHECK (status IN ('NEW','CLAIMED','PUBLISHED','FAILED')),
 CONSTRAINT chk_disbursement_outbox_claim CHECK ((status='CLAIMED') = (claimed_at IS NOT NULL)),
 CONSTRAINT chk_disbursement_outbox_publication CHECK ((status='PUBLISHED') = (published_at IS NOT NULL))
);
CREATE INDEX idx_disbursement_outbox_pending ON loan_disbursement.outbox_events(status,created_at);
