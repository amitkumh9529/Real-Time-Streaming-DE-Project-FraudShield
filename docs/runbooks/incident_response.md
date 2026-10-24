# Runbook: Fraud Pipeline Incident Response & Triage

## 1. Severity Levels & Escalation Triggers

| Severity | Definition | Examples | Response SLA |
|---|---|---|---|
| **SEV-1 (Critical)** | Pipeline completely stalled; streaming consumer lag growing unbounded | Kafka SASL auth failure; cluster OOM; corrupt checkpoint. | < 15 minutes |
| **SEV-2 (High)** | Data quality degradation or partial alert sink failures | > 5% drop rate in DLT expectations; SMTP authentication revoked. | < 1 hour |
| **SEV-3 (Medium)** | Non-blocking schema drift or minor latency spikes | Unmapped JSON fields captured in `_rescued_data`. | < 4 hours |

---

## 2. SEV-1: Pipeline Stalled or Consumer Lag Spike

### Step 1: Check Databricks DLT Pipeline Event Log
Execute in Databricks SQL:
```sql
SELECT
  timestamp,
  message,
  level,
  details:flow_progress:metrics:num_output_rows as output_rows
FROM event_log("fraudshield_pipeline")
WHERE level IN ('ERROR', 'WARN')
ORDER BY timestamp DESC
LIMIT 50;
```

### Step 2: Diagnose Common Root Causes
1. **Confluent Cloud Credentials Expired / Revoked**:
   - Check error message for `SaslAuthenticationException` or `Invalid credentials`.
   - Rotate API key in Confluent Cloud Console and update `fraudshield-scope` secrets.
2. **State Store OOM on Stream-Stream Join**:
   - Check if watermarks are advancing properly.
   - Verify both transaction and watchlist streams have events with progressing timestamps.
3. **Corrupted Streaming Checkpoint**:
   - Never delete production checkpoints unless instructed by the lead architect.
   - Roll back pipeline definition to the last verified commit.

---

## 3. SEV-2: Data Quality & DLT Expectations Breached

When the drop rate exceeds acceptable thresholds:
```sql
SELECT
  dataset,
  expectation_name,
  SUM(passed_records) as total_passed,
  SUM(failed_records) as total_failed,
  ROUND(SUM(failed_records) / (SUM(passed_records) + SUM(failed_records)) * 100, 2) as failure_rate_pct
FROM event_log("fraudshield_pipeline")
WHERE event_type = 'expectation'
GROUP BY dataset, expectation_name
ORDER BY failure_rate_pct DESC;
```
If `valid_card_token` or `valid_transaction_id` failures spike, inspect upstream Kafka producers for schema regression.
