package com.lps.loan_application.outbox;

public enum OutboxEventStatus {
    NEW,
    CLAIMED,
    PUBLISHED,
    FAILED
}
