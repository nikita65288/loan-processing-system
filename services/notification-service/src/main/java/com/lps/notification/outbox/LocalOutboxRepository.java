package com.lps.notification.outbox;
import org.springframework.data.jpa.repository.*;
import org.springframework.data.repository.query.Param;
import java.time.Instant;
import java.util.*;

/** PostgreSQL row claiming and compare-and-set publication confirmation. */
public interface LocalOutboxRepository extends JpaRepository<LocalOutboxEvent, UUID> {
    @Query(value="""
        SELECT * FROM notification.outbox_events WHERE status = 'NEW'
        ORDER BY created_at, id LIMIT :limit FOR UPDATE SKIP LOCKED
        """, nativeQuery=true)
    List<LocalOutboxEvent> claimable(@Param("limit") int limit);

    @Modifying
    @Query(value="""
        UPDATE notification.outbox_events SET status='NEW', claimed_at=NULL
        WHERE status='CLAIMED' AND claimed_at < :before
        """, nativeQuery=true)
    int releaseStale(@Param("before") Instant before);

    @Modifying
    @Query(value="""
        UPDATE notification.outbox_events SET status='PUBLISHED', claimed_at=NULL,
        published_at=:now, last_error=NULL
        WHERE id=:id AND status='CLAIMED' AND claimed_at=:claimedAt
        """, nativeQuery=true)
    int publishIfOwned(@Param("id") UUID id, @Param("claimedAt") Instant claimedAt,
                       @Param("now") Instant now);

    @Modifying
    @Query(value="""
        UPDATE notification.outbox_events SET retry_count=retry_count+1,
        last_error=:error, claimed_at=NULL,
        status=CASE WHEN retry_count+1 >= :limit THEN 'FAILED' ELSE 'NEW' END
        WHERE id=:id AND status='CLAIMED' AND claimed_at=:claimedAt
        """, nativeQuery=true)
    int failIfOwned(@Param("id") UUID id, @Param("claimedAt") Instant claimedAt,
                    @Param("error") String error, @Param("limit") int limit);
}
