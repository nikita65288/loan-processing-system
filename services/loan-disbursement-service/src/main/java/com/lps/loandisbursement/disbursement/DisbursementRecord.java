package com.lps.loandisbursement.disbursement;
import jakarta.persistence.*;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;
/** One durable completed simulated payout per application; this is not a money transfer. */
@Entity
@Table(name="disbursements",schema="loan_disbursement")
public class DisbursementRecord {
    @Id private UUID id;
    @Column(name="application_id",nullable=false,unique=true) private UUID applicationId;
    @Column(name="command_id",nullable=false) private UUID commandId;
    @Column(nullable=false,precision=19,scale=2) private BigDecimal amount;
    @Column(nullable=false,length=3) private String currency;
    @Column(nullable=false,length=24) private String status;
    @Column(name="created_at",nullable=false) private Instant createdAt;
    @Column(name="completed_at",nullable=false) private Instant completedAt;
    protected DisbursementRecord() {}
    public DisbursementRecord(UUID id, UUID applicationId, UUID commandId, BigDecimal amount,
                              String currency, Instant now) {
        this.id=id;this.applicationId=applicationId;this.commandId=commandId;
        this.amount=amount;this.currency=currency;this.status="COMPLETED";
        this.createdAt=now;this.completedAt=now;
    }
    public UUID getId() {return id;}
    public UUID getApplicationId() {return applicationId;}
    public UUID getCommandId() {return commandId;}
    public BigDecimal getAmount() {return amount;}
    public String getCurrency() {return currency;}
    public String getStatus() {return status;}
}
