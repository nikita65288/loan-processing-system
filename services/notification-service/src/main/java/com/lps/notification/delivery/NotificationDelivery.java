package com.lps.notification.delivery;
import jakarta.persistence.*;
import java.time.Instant;
import java.util.UUID;
import com.lps.contracts.model.NotificationType;
/** Durable simulated notification; SENT_SIMULATED does not imply an external message. */
@Entity
@Table(name="notification_deliveries",schema="notification")
public class NotificationDelivery {
    @Id private UUID id;
    @Column(name="application_id",nullable=false,unique=true) private UUID applicationId;
    @Column(name="command_id",nullable=false) private UUID commandId;
    @Column(name="customer_id",nullable=false) private UUID customerId;
    @Enumerated(EnumType.STRING)
    @Column(name="notification_type",nullable=false) private NotificationType notificationType;
    @Column(nullable=false) private String status;
    @Column(name="created_at",nullable=false) private Instant createdAt;
    @Column(name="completed_at",nullable=false) private Instant completedAt;
    protected NotificationDelivery() {}
    public NotificationDelivery(UUID id, UUID applicationId, UUID commandId, UUID customerId,
                                NotificationType type,Instant now) {
        this.id=id;this.applicationId=applicationId;this.commandId=commandId;
        this.customerId=customerId;this.notificationType=type;
        this.status="SENT_SIMULATED";this.createdAt=now;this.completedAt=now;
    }
    public String getStatus(){return status;}
    public NotificationType getNotificationType(){return notificationType;}
}
