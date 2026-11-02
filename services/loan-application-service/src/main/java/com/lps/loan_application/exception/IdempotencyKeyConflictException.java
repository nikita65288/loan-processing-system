package com.lps.loan_application.exception;

/** Prevents the same idempotency key from being reused for a different creation request. */
public class IdempotencyKeyConflictException extends RuntimeException {
    public IdempotencyKeyConflictException() {
        super("Idempotency-Key was already used for a different request");
    }
}
