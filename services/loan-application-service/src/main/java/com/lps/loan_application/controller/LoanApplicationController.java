package com.lps.loan_application.controller;

import com.lps.loan_application.dto.CreateLoanApplicationRequest;
import com.lps.loan_application.dto.LoanApplicationResponse;
import com.lps.loan_application.service.LoanApplicationService;
import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.http.HttpHeaders;
import com.lps.loan_application.exception.InvalidIdempotencyKeyException;
import com.lps.loan_application.service.IdempotentLoanApplicationCreator;
import java.util.List;
import java.util.regex.Pattern;
import org.springframework.web.bind.annotation.*;

import java.net.URI;
import java.util.UUID;

@RestController
@RequestMapping("/applications")
public class LoanApplicationController {

    private static final Pattern KEY_PATTERN = Pattern.compile("[A-Za-z0-9][A-Za-z0-9._:-]{0,127}");
    private final LoanApplicationService service;
    private final IdempotentLoanApplicationCreator creator;

    public LoanApplicationController(
            LoanApplicationService service,
            IdempotentLoanApplicationCreator creator
    ) {
        this.service = service;
        this.creator = creator;
    }

    @PostMapping
    public ResponseEntity<LoanApplicationResponse> create(
            @Valid @RequestBody CreateLoanApplicationRequest request,
            @RequestHeader HttpHeaders headers
    ) {
        LoanApplicationResponse response = creator.create(request, readKey(headers));

        return ResponseEntity
                .created(
                        URI.create("/applications/" + response.id())
                )
                .body(response);
    }

    /** Reject ambiguous or malformed client keys instead of silently changing their identity. */
    private String readKey(HttpHeaders headers) {
        List<String> values = headers.get("Idempotency-Key");
        if (values == null) {
            return null; // Legacy behavior; HTTP retries without a key are NOT idempotent.
        }
        if (values.size() != 1 || !KEY_PATTERN.matcher(values.get(0)).matches()) {
            throw new InvalidIdempotencyKeyException();
        }
        return values.get(0);
    }

    @GetMapping("/{applicationId}")
    public ResponseEntity<LoanApplicationResponse> getById(
            @PathVariable UUID applicationId
    ) {
        return ResponseEntity.ok(
                service.getById(applicationId)
        );
    }
}
