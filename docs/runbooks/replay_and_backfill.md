# Runbook: Historical Replay & Data Backfilling

## 1. Overview
This procedure details how to safely replay past Kafka transactions, backfill historical customer CDC events, and recompute downstream Gold metrics without causing duplicate customer alerts.

---

## 2. Kafka Stream Replay Procedure

### Scenario: Producer Error, Missing Hours, or Pipeline Bug
1. **Identify Required Offset / Timestamp**:
   Determine the target UTC timestamp or offset to rewind to using the Kafka consumer group offset tool:
   ```bash
   kafka-consumer-groups --bootstrap-server $BOOTSTRAP_SERVERS \
     --group fraudshield-dlt-consumer-group \
     --describe
   ```
2. **Rewind Consumer Group Offsets**:
   Reset offsets to the desired timestamp (e.g. 6 hours ago):
   ```bash
   kafka-consumer-groups --bootstrap-server $BOOTSTRAP_SERVERS \
     --group fraudshield-dlt-consumer-group \
     --topic credit_card_transactions \
     --reset-offsets --to-datetime 2026-10-24T00:00:00.000 \
     --execute
   ```
3. **Pipeline Checkpoint Reset (Isolated Staging/Backfill Pipeline)**:
   In Databricks DLT, do **not** blindly wipe the production checkpoint while running live! Instead:
   - Clone or trigger a dedicated backfill pipeline with a custom checkpoint location:
     `/Volumes/fraudshield/source/transactions/checkpoint_backfill_20261024/`.
   - Set `.option("startingOffsets", "earliest")` or specify the desired offset range.

---

## 3. Suppressing Duplicate Customer Alerts During Replay

To prevent re-sending hundreds of duplicate email notifications to customers during a replay:
1. **Outbox Idempotency Guarantee**:
   The `fraudshield.gold.alert_outbox` table uses deterministic `alert_id`s (`FRAUD-{transaction_id}-{watchlist_id}`).
2. **Backfill Mode Flag**:
   In the alert dispatcher or DLT pipeline settings, set:
   ```env
   ALERT_DISPATCH_ENABLED=false
   ```
   Or mark replayed alerts directly with `delivery_status = 'REPLAY_SUPPRESSED'`.

---

## 4. Verification & Validation
After completing the replay:
```sql
SELECT 
  DATE(transaction_timestamp) as txn_date,
  COUNT(*) as total_events,
  COUNT(DISTINCT transaction_id) as unique_transactions
FROM fraudshield.silver.transactions
GROUP BY DATE(transaction_timestamp)
ORDER BY txn_date DESC;
```
Verify `total_events == unique_transactions` to confirm the deduplication watermark held properly.
