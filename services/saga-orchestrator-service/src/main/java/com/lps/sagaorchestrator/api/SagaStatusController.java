package com.lps.sagaorchestrator.api;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

/** Diagnostic read-only Saga API. It never offers manual state transitions. */
@RestController
@RequestMapping("/sagas/applications")
public class SagaStatusController {
    private final SagaQueryService query;

    public SagaStatusController(SagaQueryService query) {
        this.query = query;
    }

    @GetMapping("/{applicationId}")
    public ResponseEntity<SagaStatusResponse> get(@PathVariable UUID applicationId) {
        return ResponseEntity.ok(query.getByApplicationId(applicationId));
    }
}
