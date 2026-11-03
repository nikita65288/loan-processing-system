-- Unique logical inquiries, atomic with their response in the existing Outbox.
CREATE TABLE loan_disbursement.processed_status_inquiries (
    inquiry_id UUID PRIMARY KEY,
    application_id UUID NOT NULL,
    disbursement_command_id UUID NOT NULL,
    correlation_id UUID NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX idx_disbursement_inquiry_application
    ON loan_disbursement.processed_status_inquiries(application_id);
