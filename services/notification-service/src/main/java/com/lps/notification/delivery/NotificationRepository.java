package com.lps.notification.delivery;
import org.springframework.data.jpa.repository.JpaRepository;
import java.util.*;
/** One business notification per loan application in the current workflow. */
public interface NotificationRepository extends JpaRepository<NotificationDelivery,UUID> {
    Optional<NotificationDelivery> findByApplicationId(UUID applicationId);
}
