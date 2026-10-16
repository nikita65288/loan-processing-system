CREATE TABLE loan_applications
(
    id          UUID             NOT NULL,
    customer_id UUID             NOT NULL,
    amount      NUMERIC(19, 2)   NOT NULL,
    term        INTEGER          NOT NULL,
    currency    VARCHAR(3)       NOT NULL,
    status      VARCHAR(32)      NOT NULL,
    created_at  TIMESTAMPTZ      NOT NULL,
    updated_at  TIMESTAMPTZ      NOT NULL,

    CONSTRAINT pk_loan_applications
        PRIMARY KEY (id),

    CONSTRAINT chk_loan_application_amount_positive
        CHECK (amount > 0),

    CONSTRAINT chk_loan_application_term_positive
        CHECK (term > 0),

    CONSTRAINT chk_loan_application_currency_format
        CHECK (currency ~ '^[A-Z]{3}$'),

    CONSTRAINT chk_loan_application_status
        CHECK (status = 'CREATED')
);