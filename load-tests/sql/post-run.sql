-- load testing: read-only, test-only verification against the existing local Kubernetes schemas.
-- IMPORTANT: Run all statements in ONE PostgreSQL session (IntelliJ Database console / psql).
-- Replace this example run prefix with the eight hexadecimal characters from k6 RUN_ID.
-- Never use this diagnostic cross-schema SQL as a runtime service dependency.
SELECT set_config('lps.run_prefix', 'abcdef12', false);

-- 1. Count all accepted application rows belonging to the isolated workload.
SELECT count(*) AS created_applications,
       min(created_at) AS first_created_at,
       max(created_at) AS last_created_at
FROM public.loan_applications
WHERE left(customer_id::text, 8) = current_setting('lps.run_prefix');

-- 2. Compare durable applications to Saga terminal states, including missing Saga rows.
WITH run_applications AS (
    SELECT id FROM public.loan_applications
    WHERE left(customer_id::text, 8) = current_setting('lps.run_prefix')
)
SELECT coalesce(s.status, 'MISSING_SAGA') AS saga_status, count(*) AS applications
FROM run_applications a
LEFT JOIN saga_orchestrator.saga_instances s ON s.application_id = a.id
GROUP BY coalesce(s.status, 'MISSING_SAGA')
ORDER BY 1;

-- 3. Financial invariant checks: no duplicate logical disbursement; no payout after rejection;
-- no approved completed Saga that claims payout completion without a durable matching record.
WITH run_applications AS (
    SELECT id FROM public.loan_applications
    WHERE left(customer_id::text, 8) = current_setting('lps.run_prefix')
)
SELECT
   (SELECT count(*) FROM (
       SELECT d.application_id FROM loan_disbursement.disbursements d
       JOIN run_applications a ON a.id=d.application_id
       GROUP BY d.application_id HAVING count(*) > 1
    ) duplicates) AS duplicate_disbursement_application_ids,
   (SELECT count(*) FROM run_applications a
       JOIN saga_orchestrator.saga_instances s ON s.application_id=a.id
       JOIN loan_disbursement.disbursements d ON d.application_id=a.id
       WHERE s.final_decision='REJECTED') AS rejected_with_disbursement,
   (SELECT count(*) FROM run_applications a
       JOIN saga_orchestrator.saga_instances s ON s.application_id=a.id
       LEFT JOIN loan_disbursement.disbursements d ON d.application_id=a.id
       WHERE s.status='COMPLETED' AND s.final_decision='APPROVED'
         AND s.disbursement_status='COMPLETED' AND d.id IS NULL)
       AS completed_approved_missing_disbursement;

-- 4. Saga p50/p95/p99 (duration measured from SAGA creation, not from HTTP request arrival).
-- Compute against COMPLETED Saga only; examine unfinished Saga separately (query 2).
SELECT count(*) AS completed_sagas,
       round(avg(extract(epoch FROM (s.completed_at - s.created_at)))::numeric, 3) AS avg_seconds,
       round((percentile_cont(0.50) within group (order by extract(epoch FROM (s.completed_at - s.created_at))))::numeric, 3) AS p50_seconds,
       round((percentile_cont(0.95) within group (order by extract(epoch FROM (s.completed_at - s.created_at))))::numeric, 3) AS p95_seconds,
       round((percentile_cont(0.99) within group (order by extract(epoch FROM (s.completed_at - s.created_at))))::numeric, 3) AS p99_seconds,
       round(max(extract(epoch FROM (s.completed_at - s.created_at)))::numeric, 3) AS max_seconds
FROM saga_orchestrator.saga_instances s
JOIN public.loan_applications a ON a.id=s.application_id
WHERE left(a.customer_id::text, 8) = current_setting('lps.run_prefix')
AND s.status='COMPLETED' AND s.completed_at IS NOT NULL;

-- 5. Pending ages, terminal completeness, and business decisions.
SELECT s.status,s.final_decision,count(*) AS count,
       round(max(extract(epoch FROM (now()-s.created_at)))::numeric,1) AS max_age_since_creation_seconds
FROM saga_orchestrator.saga_instances s
JOIN public.loan_applications a ON a.id=s.application_id
WHERE left(a.customer_id::text,8)=current_setting('lps.run_prefix')
GROUP BY s.status,s.final_decision ORDER BY 1,2;

-- 6. Outbox state by owner for THIS run. FAILED requires investigation, not deletion.
WITH run_applications AS (
  SELECT id FROM public.loan_applications WHERE left(customer_id::text,8)=current_setting('lps.run_prefix')
), states AS (
  SELECT 'loan-application' AS owner, o.status FROM public.outbox_events o
    JOIN run_applications a ON o.aggregate_id=a.id
  UNION ALL
  SELECT 'saga-orchestrator', o.status FROM saga_orchestrator.saga_outbox_events o
    JOIN run_applications a ON o.application_id=a.id
  UNION ALL
  SELECT 'loan-disbursement', o.status FROM loan_disbursement.outbox_events o
    JOIN run_applications a ON o.application_id=a.id
  UNION ALL
  SELECT 'notification', o.status FROM notification.outbox_events o
    JOIN run_applications a ON o.application_id=a.id
)
SELECT owner,status,count(*) AS events FROM states GROUP BY owner,status ORDER BY owner,status;

-- 7. Notification and payouts: a rejected outcome can still have notification delivery.
WITH run_applications AS (
 SELECT id FROM public.loan_applications WHERE left(customer_id::text,8)=current_setting('lps.run_prefix')
)
SELECT
 (SELECT count(*) FROM loan_disbursement.disbursements d JOIN run_applications a ON a.id=d.application_id) AS disbursement_rows,
 (SELECT count(*) FROM notification.notification_deliveries n JOIN run_applications a ON a.id=n.application_id) AS notification_rows,
 (SELECT count(*) FROM saga_orchestrator.saga_step_attempts st
   JOIN saga_orchestrator.saga_instances s ON s.saga_id=st.saga_id
   JOIN run_applications a ON a.id=s.application_id) AS saga_attempt_rows;

-- 8. Duplicate Saga check (schema UNIQUE(application_id) is primary protection).
WITH run_applications AS (
 SELECT id FROM public.loan_applications WHERE left(customer_id::text,8)=current_setting('lps.run_prefix')
)
SELECT s.application_id,count(*) AS sagas FROM saga_orchestrator.saga_instances s
JOIN run_applications a ON a.id=s.application_id
GROUP BY s.application_id HAVING count(*)>1;

-- 9. Global Inbox health; only a time-window approximation, NOT filtered by run ID:
-- processed_messages contains no application_id, so don't attribute these rows to the run.
SELECT count(*) AS total_inbox_messages FROM saga_orchestrator.processed_messages;
