-- load testing: one read-only JSON summary for automated reconciliation.
-- The runner replaces __RUN_ID__ ONLY after validating an eight-digit hex string.
-- Uses local Kubernetes PostgreSQL schemas. No updates, no advisory locks, no schema changes.
WITH apps AS (
  SELECT id, created_at FROM public.loan_applications
  WHERE left(customer_id::text, 8) = '__RUN_ID__'
), sagas AS (
  SELECT s.* FROM saga_orchestrator.saga_instances s JOIN apps a ON a.id = s.application_id
), outbox AS (
  SELECT 'loan-application' AS owner, o.status FROM public.outbox_events o JOIN apps a ON a.id = o.aggregate_id
  UNION ALL SELECT 'saga-orchestrator', o.status FROM saga_orchestrator.saga_outbox_events o JOIN apps a ON a.id = o.application_id
  UNION ALL SELECT 'loan-disbursement', o.status FROM loan_disbursement.outbox_events o JOIN apps a ON a.id = o.application_id
  UNION ALL SELECT 'notification', o.status FROM notification.outbox_events o JOIN apps a ON a.id = o.application_id
), saga_status_counts AS (
  SELECT status, count(*) AS count FROM sagas GROUP BY status
), outbox_status_counts AS (
  SELECT owner, status, count(*) AS count FROM outbox GROUP BY owner, status
)
SELECT jsonb_build_object(
  'run_id', '__RUN_ID__',
  'captured_at_utc', to_char(now() AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
  'created_applications', (SELECT count(*) FROM apps),
  'first_created_at', (SELECT min(created_at) FROM apps),
  'last_created_at', (SELECT max(created_at) FROM apps),
  'saga_count', (SELECT count(*) FROM sagas),
  'missing_sagas', (SELECT count(*) FROM apps a WHERE NOT EXISTS (SELECT 1 FROM sagas s WHERE s.application_id=a.id)),
  'duplicate_sagas', (SELECT count(*) FROM (SELECT application_id FROM sagas GROUP BY application_id HAVING count(*)>1) x),
  'saga_completed', (SELECT count(*) FROM sagas WHERE status='COMPLETED'),
  'saga_in_progress', (SELECT count(*) FROM sagas WHERE status='IN_PROGRESS'),
  'saga_failed', (SELECT count(*) FROM sagas WHERE status='FAILED'),
  'saga_requires_reconciliation', (SELECT count(*) FROM sagas WHERE status='REQUIRES_RECONCILIATION'),
  'saga_statuses', (SELECT coalesce(jsonb_object_agg(status,count),'{}'::jsonb) FROM saga_status_counts),
  'outbox_statuses', (SELECT coalesce(jsonb_agg(jsonb_build_object('owner',owner,'status',status,'events',count) ORDER BY owner,status),'[]'::jsonb) FROM outbox_status_counts),
  'outbox_pending', (SELECT count(*) FROM outbox WHERE status <> 'PUBLISHED'),
  'outbox_failed', (SELECT count(*) FROM outbox WHERE status='FAILED'),
  'duplicate_disbursement_application_ids', (
     SELECT count(*) FROM (
       SELECT d.application_id FROM loan_disbursement.disbursements d JOIN apps a ON a.id=d.application_id
       GROUP BY d.application_id HAVING count(*)>1
     ) x),
  'rejected_with_disbursement', (
     SELECT count(*) FROM sagas s JOIN loan_disbursement.disbursements d ON d.application_id=s.application_id
     WHERE s.final_decision='REJECTED'),
  'completed_approved_missing_disbursement', (
     SELECT count(*) FROM sagas s LEFT JOIN loan_disbursement.disbursements d ON d.application_id=s.application_id
     WHERE s.status='COMPLETED' AND s.final_decision='APPROVED'
       AND s.disbursement_status='COMPLETED' AND d.id IS NULL),
  'disbursement_rows', (SELECT count(*) FROM loan_disbursement.disbursements d JOIN apps a ON a.id=d.application_id),
  'notification_rows', (SELECT count(*) FROM notification.notification_deliveries n JOIN apps a ON a.id=n.application_id),
  'saga_attempt_rows', (SELECT count(*) FROM saga_orchestrator.saga_step_attempts st JOIN sagas s ON st.saga_id=s.saga_id),
  'saga_duration_seconds', (
     SELECT jsonb_build_object(
       'count',count(*),
       'average',round(avg(extract(epoch FROM (completed_at-created_at)))::numeric,3),
       'p50',round((percentile_cont(0.50) within group (order by extract(epoch FROM (completed_at-created_at))))::numeric,3),
       'p95',round((percentile_cont(0.95) within group (order by extract(epoch FROM (completed_at-created_at))))::numeric,3),
       'p99',round((percentile_cont(0.99) within group (order by extract(epoch FROM (completed_at-created_at))))::numeric,3),
       'maximum',round(max(extract(epoch FROM (completed_at-created_at)))::numeric,3))
     FROM sagas WHERE status='COMPLETED' AND completed_at IS NOT NULL),
  'global_inbox_messages_not_scoped_to_run', (SELECT count(*) FROM saga_orchestrator.processed_messages)
)::text;
