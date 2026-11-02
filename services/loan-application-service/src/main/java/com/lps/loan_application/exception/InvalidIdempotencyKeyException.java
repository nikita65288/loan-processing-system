package com.lps.loan_application.exception;

/** Indicates an ambiguous or malformed Idempotency-Key request header. */
public class InvalidIdempotencyKeyException extends RuntimeException {
    public InvalidIdempotencyKeyException() {
        super("Idempotency-Key must be one nonempty ASCII token (1-128 allowed characters)");
    }
}
