-- One logical consumer owns durable completion records for the entire Saga.
CREATE TABLE saga_orchestrator.processed_messages
(
    consumer_name VARCHAR(128) NOT NULL,
    message_id UUID NOT NULL,
    message_type VARCHAR(128) NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT pk_saga_processed_messages PRIMARY KEY (consumer_name, message_id)
);
