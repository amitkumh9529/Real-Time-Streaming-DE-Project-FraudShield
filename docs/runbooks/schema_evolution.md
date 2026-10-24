# Runbook: Schema Evolution & Contract Migration

## 1. Overview
This runbook guides engineers through safely updating transaction schemas, adding fields, and modifying data contracts without downtime or stream pipeline crashes.

---

## 2. Ingestion Resilience (Bronze Layer)
- **Auto Loader**: Uses `cloudFiles.inferColumnTypes = true` with automatic `_rescued_data` column. Any unexpected or newly introduced JSON attributes are quarantined safely in `_rescued_data` rather than causing parser crashes.
- **Kafka Stream**: Stores raw `value` as binary/string. Schema changes do not break the Bronze ingestion table.

---

## 3. Safe Schema Evolution Workflow

### Step 1: Update Pydantic Contract at Producer Boundary
1. Open `kafka_producer/contracts.py`.
2. Add the new field as `Optional[<type>] = None` (backward compatible).
3. Run test suite:
   ```bash
   pytest tests/unit/test_producer_contracts.py -v
   ```

### Step 2: Update Silver Layer Schema Definition
1. Open `dlt_pipelines/silver/transactions_silver.py`.
2. Append the new column to `schema = StructType([...])`.
3. Add any corresponding quality rules via `@dp.expect(...)`.

### Step 3: Zero-Downtime Deployment
Deploy the updated pipeline using Databricks Asset Bundles:
```bash
databricks bundle deploy --target prod
```
DLT automatically handles non-breaking schema evolution for Delta tables without requiring historical table drops.
