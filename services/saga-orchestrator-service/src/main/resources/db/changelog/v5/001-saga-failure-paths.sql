-- Nullable for historical Stages 1-11; failure_reason and failed_at already exist.
ALTER TABLE saga_orchestrator.saga_instances
    ADD COLUMN last_error_code VARCHAR(64),
    ADD COLUMN last_error_at TIMESTAMPTZ,
    ADD COLUMN failed_command_id UUID;
