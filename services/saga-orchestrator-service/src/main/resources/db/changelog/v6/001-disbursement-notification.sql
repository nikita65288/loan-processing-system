-- In-place additive migration. Legacy COMPLETED Saga remain FINISHED and never get new commands.
ALTER TABLE saga_orchestrator.saga_instances
 ADD COLUMN disbursement_id UUID,
 ADD COLUMN disbursement_status VARCHAR(32),
 ADD COLUMN notification_status VARCHAR(32),
 ADD COLUMN notification_failure_code VARCHAR(64);
ALTER TABLE saga_orchestrator.saga_instances DROP CONSTRAINT chk_saga_status;
ALTER TABLE saga_orchestrator.saga_instances ADD CONSTRAINT chk_saga_status
 CHECK(status IN ('IN_PROGRESS','COMPLETED','FAILED','REQUIRES_RECONCILIATION'));
ALTER TABLE saga_orchestrator.saga_instances DROP CONSTRAINT chk_saga_step;
ALTER TABLE saga_orchestrator.saga_instances ADD CONSTRAINT chk_saga_step
 CHECK(current_step IN ('SCORING','BUREAU','DECISION','DISBURSEMENT','NOTIFICATION','FINISHED'));
ALTER TABLE saga_orchestrator.saga_step_attempts DROP CONSTRAINT chk_saga_step_attempt_step;
ALTER TABLE saga_orchestrator.saga_step_attempts ADD CONSTRAINT chk_saga_step_attempt_step
 CHECK(step IN ('SCORING','BUREAU','DECISION','DISBURSEMENT','NOTIFICATION'));
