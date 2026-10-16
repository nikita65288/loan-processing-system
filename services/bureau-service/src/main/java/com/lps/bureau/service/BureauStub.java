package com.lps.bureau.service;

import com.lps.contracts.command.PerformBureauCheckCommand;
import com.lps.contracts.event.BureauCheckCompletedEvent;
import com.lps.contracts.MessageIdentity;
import org.springframework.stereotype.Service;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import java.time.Instant;

/** Deterministic simulated bureau check. */
@Service
public class BureauStub {
    private final boolean simulateDelinquencies;
    /** Default behavior is unchanged; tests may enable REJECTED with a local-only setting. */
    public BureauStub() { this(false); }
    @Autowired
    public BureauStub(@Value("${lps.bureau.stub.simulate-delinquencies:false}") boolean simulateDelinquencies) {
        this.simulateDelinquencies = simulateDelinquencies;
    }

    public BureauCheckCompletedEvent check(PerformBureauCheckCommand command) {
        return new BureauCheckCompletedEvent(
                MessageIdentity.resultEventId(command.commandId(), "BureauCheckCompleted"),
                "BureauCheckCompleted",
                Instant.now(),
                command.correlationId(),
                command.applicationId(),
                720,
                simulateDelinquencies,
                true
        );
    }
}
