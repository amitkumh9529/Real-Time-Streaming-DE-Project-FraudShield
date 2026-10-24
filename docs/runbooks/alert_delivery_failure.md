# Runbook: Alert Delivery Failures & Dead-Letter Recovery

## 1. Overview
The alert notification engine writes to `fraudshield.gold.alert_outbox`. When external notification networks (SMTP/SendGrid) experience downtime, records transition into `FAILED` or `DEAD_LETTER` states.

---

## 2. Inspecting the Dead-Letter Queue (DLQ)

Run in Databricks SQL to view stuck or dead-lettered alerts:
```sql
SELECT
  alert_id,
  alert_type,
  recipient_email,
  retry_count,
  last_attempt_timestamp,
  failure_reason
FROM fraudshield.gold.alert_outbox
WHERE delivery_status = 'DEAD_LETTER'
ORDER BY last_attempt_timestamp DESC;
```

---

## 3. Remediating Common Failure Reasons

| Failure Reason Pattern | Root Cause | Remediation |
|---|---|---|
| `SMTPAuthenticationError` | Gmail App Password expired or revoked | Re-generate app password in Google Workspace and update `fraudshield-scope`. |
| `SMTPServerDisconnected` | Network egress firewall or transient timeout | Verify port 587 outbound rule in Databricks cluster network security group. |
| `ValueError: Invalid recipient` | Corrupted customer email in master profile | Update customer master profile in PostgreSQL and propagate via CDC. |

---

## 4. Re-queuing Dead-Letter Alerts

Once the root cause is resolved, safely reset dead-lettered alerts to `PENDING` with zero retry count:
```sql
UPDATE fraudshield.gold.alert_outbox
SET 
  delivery_status = 'PENDING',
  retry_count = 0,
  failure_reason = NULL
WHERE delivery_status = 'DEAD_LETTER'
  AND created_timestamp >= CURRENT_TIMESTAMP() - INTERVAL 7 DAYS;
```
The alert dispatcher will pick up these records on the next scheduled run and deliver them idempotently.
