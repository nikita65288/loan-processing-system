package com.lps.loan_application.exception;

public class KafkaPublishException extends RuntimeException {

    public KafkaPublishException(
            String message,
            Throwable cause
    ) {
        super(message, cause);
    }
}
