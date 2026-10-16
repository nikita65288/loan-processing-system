package com.lps.sagaorchestrator.saga;

import org.springframework.stereotype.Component;

/** The current calculation/read steps have no reversible business effects. */
@Component
public class SagaCompensationPolicy {
    /** A new side-effecting step must be explicitly designed before this policy can change. */
    public boolean requiresCompensation(SagaInstance saga) {
        return switch (saga.getCurrentStep()) {
            // No reversible RESERVED/PENDING external effect exists in this simulated workflow.
            case SCORING, BUREAU, DECISION, DISBURSEMENT, NOTIFICATION, FINISHED -> false;
        };
    }
}
