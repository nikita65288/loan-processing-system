package com.lps.loan_application.service;

import com.lps.loan_application.dto.CreateLoanApplicationRequest;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class RequestFingerprintTest {
    private final RequestFingerprint fingerprint = new RequestFingerprint();
    private final UUID customer = UUID.fromString("11111111-1111-4111-8111-111111111111");

    @Test
    void equivalentDecimalScalesHaveTheSameFingerprint() {
        String first = fingerprint.forCreation(new CreateLoanApplicationRequest(customer, new BigDecimal("100.0"), 12, "EUR"));
        String replay = fingerprint.forCreation(new CreateLoanApplicationRequest(customer, new BigDecimal("100.00"), 12, "EUR"));
        assertEquals(first, replay);
        assertEquals(64, first.length());
        assertEquals(first, fingerprint.forCreation(new CreateLoanApplicationRequest(customer, new BigDecimal("100.0"), 12, "EUR")));
    }

    @Test
    void changesToEachBusinessFieldChangeFingerprint() {
        var original = new CreateLoanApplicationRequest(customer, new BigDecimal("100.00"), 12, "EUR");
        String hash = fingerprint.forCreation(original);
        assertNotEquals(hash, fingerprint.forCreation(new CreateLoanApplicationRequest(UUID.randomUUID(), original.amount(), 12, "EUR")));
        assertNotEquals(hash, fingerprint.forCreation(new CreateLoanApplicationRequest(customer, new BigDecimal("101.00"), 12, "EUR")));
        assertNotEquals(hash, fingerprint.forCreation(new CreateLoanApplicationRequest(customer, original.amount(), 13, "EUR")));
        assertNotEquals(hash, fingerprint.forCreation(new CreateLoanApplicationRequest(customer, original.amount(), 12, "USD")));
    }
}
