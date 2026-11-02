package com.lps.loan_application.service;

import com.lps.loan_application.dto.CreateLoanApplicationRequest;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.math.RoundingMode;
import java.util.HexFormat;

/** Stable, versioned SHA-256 identity for the validated logical create request. */
@Component
public class RequestFingerprint {

    public String forCreation(CreateLoanApplicationRequest request) {
        // All fields have validated, delimiter-free formats (UUID, number, ISO currency).
        String canonical = "lps.create-application.v1\n"
                + request.customerId() + "\n"
                + request.amount().setScale(2, RoundingMode.UNNECESSARY).toPlainString() + "\n"
                + request.term() + "\n"
                + request.currency();
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256")
                    .digest(canonical.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(digest);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("JVM does not provide SHA-256", e);
        }
    }
}
