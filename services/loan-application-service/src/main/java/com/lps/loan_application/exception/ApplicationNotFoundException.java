package com.lps.loan_application.exception;

import java.util.UUID;

public class ApplicationNotFoundException extends RuntimeException {

    public ApplicationNotFoundException(UUID applicationId) {
        super("Loan application not found: " + applicationId);
    }
}
