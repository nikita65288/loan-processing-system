package com.lps.loan_application.controller;

import com.lps.loan_application.dto.CreateLoanApplicationRequest;
import com.lps.loan_application.dto.LoanApplicationResponse;
import com.lps.loan_application.service.LoanApplicationService;
import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.net.URI;
import java.util.UUID;

@RestController
@RequestMapping("/applications")
public class LoanApplicationController {

    private final LoanApplicationService service;

    public LoanApplicationController(
            LoanApplicationService service
    ) {
        this.service = service;
    }

    @PostMapping
    public ResponseEntity<LoanApplicationResponse> create(
            @Valid @RequestBody CreateLoanApplicationRequest request
    ) {
        LoanApplicationResponse response = service.create(request);

        return ResponseEntity
                .created(
                        URI.create("/applications/" + response.id())
                )
                .body(response);
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
