# FraudShield Delta Live Tables (DLT) Streaming Pipeline

This directory contains the production Lakehouse pipelines built on **Databricks Delta Live Tables (DLT)** using Python and Structured Streaming.

```
       +-------------------------------------------------------------+
       |                        BRONZE LAYER                         |
       |  - transactions_bronze.py (Kafka SASL_SSL Streaming Source) |
       |  - fraud_watchlist_bronze.py (Auto Loader Volume Source)    |
       +------------------------------+------------------------------+
                                      |
                                      v
       +-------------------------------------------------------------+
       |                        SILVER LAYER                         |
       |  - transactions_silver.py (Schema parse, DLT expectations)  |
       |  - fraud_watchlist_silver.py (Uppercase normalization, time)|
       |  - customers_silver.py (PostgreSQL customer master hygiene) |
       +------------------------------+------------------------------+
                                      |
                                      v
       +-------------------------------------------------------------+
       |                         GOLD LAYER                          |
       |  - fraud_card_alert.py (Stream-Stream 5m watermarked join)  |
       |  - high_value_transactions_alert.py (Stream-static join)    |
       |  - transaction_count_by_minute.py (1-minute tumbling)      |
       |  - transaction_count_by_minute_sliding_window.py (5m slide) |
       +------------------------------+------------------------------+
                                      |
                                      v
       +-------------------------------------------------------------+
       |                        ALERTS LAYER                         |
       |  - fraud_card_alert_email_notifier.py (SMTP batch sink)     |
       |  - high_value_transaction_email_notifier.py (SMTP batch)   |
       +-------------------------------------------------------------+
```

## Layers

### `bronze/`
- `transactions_bronze.py`: Ingests raw binary/string messages from Kafka topic `credit_card_transactions`, capturing message offsets, partitions, timestamps, and pipeline ingestion timestamps.
- `fraud_watchlist_bronze.py`: Reads cloud storage JSON watch records via Databricks Auto Loader (`cloudFiles`), preserving `_rescued_data` and source file lineage.

### `silver/`
- `transactions_silver.py`: Parses JSON string payloads with strict Spark `StructType` schema and validates quality contracts:
  - `@dp.expect_or_drop("valid_transaction_id", "transaction_id IS NOT NULL")`
  - `@dp.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")`
  - `@dp.expect_or_drop("valid_card_number", "card_number IS NOT NULL")`
  - `@dp.expect_or_drop("valid_merchant_id", "merchant_id IS NOT NULL")`
  - `@dp.expect("valid_amount", "amount > 0")`
- `fraud_watchlist_silver.py`: Standardizes entity strings to uppercase and parses timestamps (`dd-MMM-yyyy HH:mm:ss`).
- `customers_silver.py`: Ingests customer master profiles, formats dates, and drops invalid records.

### `gold/`
- `fraud_card_alert.py`: Performs a **watermarked stream-to-stream inner join** between transactions (5-min watermark) and watchlist (5-min watermark) on `card_number == entity_id`, enriched with customer details via a stream-to-static left outer join.
- `high_value_transactions_alert.py`: Real-time stream-to-static join identifying transactions where `amount > transaction_limit`.
- `transaction_count_by_minute.py`: Aggregates transactions into 1-minute tumbling windows.
- `transaction_count_by_minute_sliding_window.py`: Aggregates transactions into 5-minute sliding windows (1-minute slide).

### `alerts/`
- `fraud_card_alert_email_notifier.py`: Micro-batch sink (`@dp.foreach_batch_sink`) that formats HTML security alerts with masked card numbers (`****1234`) and dispatches via SMTP.
- `high_value_transaction_email_notifier.py`: Micro-batch sink alerting customers when transactions exceed configured card thresholds.

## Deploying as a Delta Live Tables Pipeline
1. In your Databricks Workspace, go to **Delta Live Tables**.
2. Click **Create Pipeline**.
3. In **Source Code**, select the `dlt_pipelines` directory.
4. Set **Target Catalog** to `fraudshield` and **Target Schema** (Bronze, Silver, Gold).
5. Set Pipeline Mode to **Continuous** (or **Triggered** for development).
6. Click **Start**.
