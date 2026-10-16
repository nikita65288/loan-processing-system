-- Nullable on purpose: Stage 7 instances may still be waiting for scoring.
ALTER TABLE saga_orchestrator.saga_instances
    ADD COLUMN correlation_id UUID,
    ADD COLUMN credit_score INTEGER,
    ADD COLUMN bureau_score INTEGER,
    ADD COLUMN has_delinquencies BOOLEAN,
    ADD COLUMN final_decision VARCHAR(16),
    ADD COLUMN decision_reason VARCHAR(1024),
    ADD COLUMN completed_at TIMESTAMPTZ;

ALTER TABLE saga_orchestrator.saga_instances
    ADD CONSTRAINT chk_saga_final_decision
        CHECK (final_decision IS NULL OR final_decision IN ('APPROVED', 'REJECTED'));
