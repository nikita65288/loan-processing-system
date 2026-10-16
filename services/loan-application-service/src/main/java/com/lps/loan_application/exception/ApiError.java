package com.lps.loan_application.exception;

import java.time.Instant;
import java.util.List;

public record ApiError(
        Instant timestamp,
        int status,
        String error,
        String message,
        String path,
        List<FieldError> fields
) {

    public record FieldError(
            String field,
            String message
    ) {}
}
