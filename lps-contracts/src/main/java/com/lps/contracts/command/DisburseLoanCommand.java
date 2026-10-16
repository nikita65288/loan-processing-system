package com.lps.contracts.command;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;
/** Logical payout request; retries preserve commandId and every business field. */
public record DisburseLoanCommand(UUID commandId, String commandType, Instant occurredAt,
                                 UUID correlationId, UUID applicationId, BigDecimal amount, String currency) {}
