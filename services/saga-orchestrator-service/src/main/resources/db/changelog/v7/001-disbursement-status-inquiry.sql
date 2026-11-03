-- Durable attempt identity; multiple replica schedulers arbitrate on the Saga row lock.
CREATE TABLE saga_orchestrator.disbursement_inquiry_attempts (
    inquiry_id UUID PRIMARY KEY,
    saga_id UUID NOT NULL REFERENCES saga_orchestrator.saga_instances(saga_id) ON DELETE CASCADE,
    disbursement_command_id UUID NOT NULL,
    attempt_number INTEGER NOT NULL CHECK (attempt_number > 0),
    created_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT uq_disbursement_inquiry_attempt UNIQUE (saga_id, attempt_number)
);
CREATE INDEX idx_disbursement_inquiry_latest
    ON saga_orchestrator.disbursement_inquiry_attempts(saga_id, created_at DESC);
