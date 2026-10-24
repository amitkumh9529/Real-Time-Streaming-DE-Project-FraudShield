# FraudShield: Real-Time Fraud Detection & Transaction Intelligence Lakehouse

A production-grade, event-driven Data Engineering lakehouse architecture for detecting fraudulent credit card transactions, enforcing spending limits, managing customer profiles, and delivering idempotent security alerts.

Built on **Python**, **Confluent Cloud Kafka**, **Databricks Delta Live Tables (DLT)**, **PostgreSQL CDC**, **Auto Loader**, and **Databricks AI/BI Lakeview Dashboards**.

---

## 1. System Architecture

```
+-------------------------------------------------------------------------------------------------------+
|                                      DATA INGESTION BOUNDARY                                          |
+-------------------------------------------------------------------------------------------------------+
   |                                              |                                                 |
[PostgreSQL OLTP]                    [Confluent Cloud Kafka]                           [Cloud Storage Volumes]
Customer Master Profiles             Payment Event Stream (5-100 TPS)                  Watchlist File Drops (JSON)
(Historic Seeds + CDC)               (Normal, High-Value, Anomaly Attacks)             (Regulatory Sanctions)
   |                                              |                                                 |
   | Pydantic Schema Validation                   | Pydantic Contract Boundary                      | Schema Inference
   v                                              v                                                 v
Auto Loader / Lakeflow               Spark Structured Streaming                        Auto Loader (cloudFiles)
   |                                              |                                                 |
===|==============================================|=================================================|===
   v                                              v                                                 v
[fraudshield.bronze.customers]       [fraudshield.bronze.transactions]                 [fraudshield.bronze.fraud_watchlist]
Raw CDC Events + File Lineage        Raw Kafka Payload + Ingestion Metadata            Auto Loader + Rescued Column
===|==============================================|=================================================|===
   |                                              |                                                 |
   | DLT Quality Expectations                     | DLT Expectations (@dp.expect_or_drop)             | Canonical Normalization
   | Deterministic Card Tokenization              | Salted HMAC-SHA256 Tokenization                 | Uppercase Status & Codes
   | SCD Type 1 Conformed View                    | Watermarked Deduplication (10 min)              | Timestamp Parsing
   v                                              v                                                 v
[fraudshield.silver.customers]       [fraudshield.silver.transactions]                 [fraudshield.silver.fraud_watchlist]
Current Profile & Spending Limit     Clean Stream (No PAN, Preserved Fraud Score)      Active Filtered Watchlist
===|==============================================|=================================================|===
   |                                              |                                                 |
   +---------------------+       +----------------+---------------+                                 |
                         |       |                                |                                 |
                         v       v                                v                                 v
        +----------------------------------+            +-----------------------------------------------+
        |  High-Value Limit Alerts Stream  |            |     Fraud Watchlist Match Alerts Stream       |
        |  Stream-Static Join              |            |     Stream-to-Stream Watermarked Join         |
        |  Filter: amount > limit          |            |     card_token == entity_token                |
        +----------------------------------+            |     Interval: tx_time <= wl_time + 30 Days    |
                         |                              +-----------------------------------------------+
                         +------------------------------+                                   |
                                        |                                                   |
                                        v                                                   v
                         +-----------------------------------+             +----------------------------------+
                         |    fraudshield.gold.alert_outbox  |             | Windowed Velocity Aggregations   |
                         |    Transactional Outbox Table     |             | 1-Min Tumbling / 5-Min Sliding   |
                         |    (PENDING/SENT/DEAD_LETTER)     |             +----------------------------------+
                         +-----------------------------------+                              |
                                        |                                                   |
                                        v                                                   v
                         +-----------------------------------+             +----------------------------------+
                         | Decoupled Alert Dispatcher        |             | Databricks AI/BI Lakeview        |
                         | Bounded Retries & Idempotent Sinks|             | Operational Monitoring Dashboard |
                         | (100% Masked PII: ****1234)       |             | (14 Widgets, 12 SQL Datasets)    |
                         +-----------------------------------+             +----------------------------------+
```

---

## 2. Key Engineering Highlights & Upgrades

- **Strict PCI-DSS Compliance & PII Tokenization**: Raw 16-digit Primary Account Numbers (PANs) are salted and hashed using deterministic HMAC-SHA256 (`tok_<hex24>`) at the ingestion boundary. Downstream analytical layers and alerts retain strictly the token and masked last-4 (`****1234`).
- **Zero Plaintext Secrets**: Hardcoded tokens, API keys, passwords, and personal email addresses have been completely eradicated. Secret access is mediated locally via `.env` / environment variables and in Databricks via `fraudshield-scope` Secret Scopes.
- **Complete Customer Ingestion & SCD Type 1 Conformed View**: Implemented missing `customers_bronze` and conformed `customers_silver` pipeline ingesting PostgreSQL profiles and Change Data Capture (CDC) events to keep customer limits current.
- **Bounded Stream-to-Stream Joins**: Fixed streaming memory leaks in `fraud_card_alert` by introducing an explicit event-time boundary condition (`tx_time <= wl_time + INTERVAL 30 DAYS`) allowing Spark state store eviction, coupled with strict `status == 'ACTIVE'` filtering.
- **Transactional Outbox Pattern**: Replaced blocking synchronous SMTP network calls inside Spark streaming tasks with a durable `alert_outbox` Delta table and decoupled idempotent dispatcher supporting bounded exponential backoff and dead-letter queue (DLQ) state transitions.
- **Automated Test Suite**: 37 automated tests passing with 100% success rate on `pytest` without requiring live Spark, Kafka, or network connectivity.

---

## 3. Design Decisions & Technical Trade-offs

### Exactly-Once vs. At-Least-Once Delivery
- **Kafka & Producer**: Configured with `acks=all` and `enable.idempotence=True`. While Kafka provides at-least-once transport across network partitions, downstream duplicates are eliminated using structured streaming event deduplication (`dropDuplicates(["transaction_id"])` bounded by a 10-minute watermark).
- **Delta Lake**: All streaming writes commit via Delta Lake's ACID transaction log, ensuring exactly-once state persistence into Bronze, Silver, and Gold tables.

### Checkpointing & Stream Replay
- Every streaming pipeline maintains an isolated checkpoint directory on cloud storage (`/Volumes/fraudshield/source/.../checkpoint/`).
- For historical replays, offsets are rewound using Kafka consumer group tools, and backfills write into an isolated staging path with alert dispatching disabled to prevent duplicate customer notifications.

### Watermarks & Late-Arriving Data
- **Transaction Stream**: Configured with a **10-minute watermark** on `transaction_timestamp`. Accommodates network transit delay and mobile client clock skew while enabling timely state eviction.
- **Stream-Stream Join Constraint**: Joining transactions against watchlist events requires watermarks on both streams plus an interval condition (`tx_time >= effective_from AND tx_time <= effective_from + 30 days`). Without this interval constraint, Spark's state store memory grows indefinitely.

### Stream-Static vs. Stream-Stream Enrichment
- **Customer Limit Checks (Stream-Static)**: Joining transactions with the customer master is executed as a stream-to-static left outer join. Customer profiles represent slowly changing dimensions where transactions are enriched against the latest broadcasted snapshot without buffering overhead.
- **Watchlist Alerts (Stream-Stream)**: Regulatory watchlists arrive as a dynamic event feed; matching requires a stateful stream-stream join bounded by watermarks.

### PII Protections & Cryptographic Tokenization
- Hashing with HMAC-SHA256 ensures high-cardinality join integrity (`card_token == entity_token`) without ever storing or revealing cardholder numbers. Salts are kept in secret scopes and rotated periodically.

### Idempotency in Alert Delivery
- Every alert receives a deterministic primary key (`FRAUD-{transaction_id}-{watchlist_id}` or `ALERT-HV-{transaction_id}`).
- The `alert_outbox` table tracks delivery states (`PENDING`, `SENT`, `FAILED`, `DEAD_LETTER`). The dispatcher verifies whether an `alert_id` was already successfully dispatched before invoking SMTP, preventing duplicate notifications upon micro-batch restarts.

---

## 4. Repository Structure

```
FraudShield/
├── .github/workflows/ci.yml         # GitHub Actions CI (Ruff, secret checks, pytest)
├── .env.example                     # Environment template with safe placeholders
├── .gitignore                       # Git ignore rules for Python, secrets, cache
├── pyproject.toml                   # Project metadata, pinned dependencies, tool configs
├── requirements.txt                 # Pinned production dependencies
├── requirements-dev.txt             # Pinned development & testing dependencies
├── databricks.yml                   # Databricks Asset Bundle (DAB) for dev/prod deployment
├── SECURITY.md                      # Security policy, secret rotation & PCI-DSS guidelines
├── README.md                        # Portfolio documentation & architecture
│
├── core/                            # Core shared utilities
│   ├── __init__.py
│   └── security.py                  # Salted HMAC-SHA256 tokenization & card masking
│
├── services/                        # Production services
│   ├── __init__.py
│   └── alert_dispatcher.py          # Decoupled outbox alert dispatcher with retry & DLQ
│
├── dlt_pipelines/                   # Production Delta Live Tables (DLT) Pipeline
│   ├── README.md                    # DLT pipeline execution instructions
│   ├── bronze/                      # Raw ingestion layer
│   │   ├── transactions_bronze.py   # Kafka stream ingestion with message metadata
│   │   ├── fraud_watchlist_bronze.py# Auto Loader cloudFiles ingestion with rescued column
│   │   └── customers_bronze.py      # Customer profile & CDC ingestion
│   ├── silver/                      # Cleaned, conformed, tokenized layer
│   │   ├── transactions_silver.py   # Unpacks JSON, deduplicates, tokenizes PAN, DLT expectations
│   │   ├── fraud_watchlist_silver.py# Normalizes codes, tokenizes entity ID, parses dates
│   │   └── customers_silver.py      # Conformed customer profile (SCD Type 1) with tokenized card
│   ├── gold/                        # Business metrics & alert streams
│   │   ├── fraud_card_alert.py      # Bounded stream-stream join for watchlist matches
│   │   ├── high_value_transactions_alert.py # Stream-static join for limit violations
│   │   ├── alert_outbox.py          # Durable transactional outbox Delta table
│   │   ├── transaction_count_by_minute.py   # 1-minute tumbling window metrics
│   │   └── transaction_count_by_minute_sliding_window.py # 5-minute sliding window metrics
│   └── alerts/                      # Micro-batch alerting sinks
│       ├── fraud_card_alert_email_notifier.py # Micro-batch sink for watchlist alerts
│       └── high_value_transaction_email_notifier.py # Micro-batch sink for high-value alerts
│
├── kafka_producer/                  # Python transaction simulator & publisher
│   ├── README.md                    # Producer execution guide
│   ├── contracts.py                 # Pydantic data contracts and boundary validations
│   ├── config.py                    # Environment settings loader
│   ├── models.py                    # Dataclasses with event_id & fraud score preservation
│   ├── fraud_engine.py              # Behavioral anomaly engine (velocity, travel, testing)
│   ├── customer_generator.py        # Customer profile generator
│   ├── merchant_generator.py        # Merchant catalog generator
│   ├── transaction_generator.py     # Continuous transaction stream generator
│   ├── producer_normal.py           # Publishes legitimate transaction stream
│   ├── producer_fraud_card.py       # Simulates watchlist card probe attacks
│   ├── producer_fraud_transaction.py# Simulates high-value spending attacks
│   └── consumer.py                  # Topic validation consumer CLI
│
├── notebooks/                       # Databricks interactive notebooks & utilities
│   ├── README.md                    # Notebooks overview
│   ├── setup/
│   │   └── setup_secret_scope.py    # Secret scope initialization via REST API
│   ├── diagnostics/                 # Cluster smoke tests & sanity checks
│   │   ├── kafka_streaming_smoke.py # Kafka connectivity test
│   │   ├── autoloader_smoke.py      # Cloud storage Auto Loader test
│   │   └── email_notification_smoke.py # SMTP test
│   └── generators/
│       ├── fraud_watchlist_data_generator.py # Incremental JSON feed generator
│       └── fraud_watchlist.csv      # Seed watchlist catalog
│
├── sql/                             # PostgreSQL OLTP schemas & CDC seeds
│   ├── README.md                    # SQL instructions
│   ├── customers_historic.sql       # Baseline schema & 1,000 sanitized customer records
│   └── customers_incremental.sql    # Incremental update/insert simulation script
│
├── dashboards/                      # Visual analytics and BI
│   ├── README.md                    # Lakeview import and configuration guide
│   ├── fraudshield_fraud_monitoring.lvdash.json # Declarative Lakeview dashboard specification
│   └── preview_dashboard.html       # Standalone interactive browser preview
│
├── docs/runbooks/                   # Production operational runbooks
│   ├── replay_and_backfill.md       # Safe replay procedures without duplicate alerts
│   ├── incident_response.md         # SEV-1 to SEV-3 troubleshooting guide
│   ├── schema_evolution.md          # Contract migration & zero-downtime evolution
│   └── alert_delivery_failure.md    # Dead-letter queue remediation runbook
│
└── tests/                           # Automated Pytest Test Suite
    ├── conftest.py                  # Shared fixtures & test doubles
    ├── unit/                        # Unit tests (tokenization, contracts, fraud rules, outbox)
    ├── integration/                 # Integration tests (stream join logic, threshold rules)
    └── smoke/                       # Static pipeline syntax validation
```

---

## 5. How to Demo This Project in an Interview

Follow this structured script to present the project end-to-end to a technical interviewer or hiring manager in **5–7 minutes**:

### Phase 1: Set the Context (30 seconds)
> *"FraudShield is an enterprise real-time transaction monitoring Lakehouse. Financial institutions lose millions when fraud detection relies on nightly batch settlements. FraudShield ingests credit card streams from Kafka, correlates them in real time with customer limits from PostgreSQL and sanction watchlists from cloud storage, and detects attacks in under 5 seconds with zero raw PAN exposure."*

### Phase 2: Run the Automated Test Suite (1 minute)
Demonstrate test rigor and code quality:
```bash
# Run the 37 automated unit, integration, and smoke tests
pytest -v
```
**Talking Points**:
- Point out that tests run **100% offline** using Pydantic contracts and test doubles without requiring live cloud infrastructure.
- Highlight the unit tests covering PCI-DSS tokenization, outbox retry state transitions, and watermark interval constraints.

### Phase 3: Demonstrate PII Tokenization & Data Contracts (1 minute)
Show the contract boundary:
```bash
python -c "from core.security import tokenize_card_number, mask_card_number; pan='4321819600133898'; print('Raw PAN:  ', pan); print('Token:    ', tokenize_card_number(pan)); print('Masked:   ', mask_card_number(pan))"
```
**Talking Points**:
- Explain why deterministic tokenization (`tok_<hex24>`) is mandatory for stream-stream joins under PCI-DSS compliance.

### Phase 4: Demonstrate Real-Time Simulator & Alert Outbox (2 minutes)
Show the transaction simulator producing valid events with preserved fraud scores:
```bash
# Test contract validation on normal transactions
python kafka_producer/producer_normal.py
```
Then demonstrate the idempotent alert outbox processing:
```python
from services.alert_dispatcher import AlertDispatcher

dispatcher = AlertDispatcher(smtp_sender="alerts@example.com", smtp_password="test")
record = {
    "alert_id": "FRAUD-TXN999-WL01",
    "alert_type": "FRAUD_WATCHLIST_MATCH",
    "delivery_status": "PENDING",
    "customer_name": "Interview Demo User",
    "recipient_email": "demo@example.com",
    "payload_json": {"transaction_id": "TXN999", "amount": 150000.0, "card_last_four": "****3898"}
}
result = dispatcher.process_outbox_record(record)
print("Outbox State Transition:", result["delivery_status"])
```

### Phase 5: Show the Operational Lakeview Dashboard (1 minute)
Open the interactive preview dashboard in any browser:
```powershell
Start-Process "dashboards/preview_dashboard.html"
```
**Talking Points**:
- Point out the **14 visual widgets** and **12 SQL dataset queries** mapping directly to the underlying `fraudshield.silver.*` and `fraudshield.gold.*` Delta tables.
- Highlight the hourly alert trend line, watchlist vs. high-value pie breakdown, and country heatmap.

---

## 6. Quickstart & Deployment Guide

### Local Environment Setup
```bash
# 1. Create virtual environment
python -m venv .venv
source .venv/bin/activate  # Windows: .\.venv\Scripts\Activate.ps1

# 2. Install pinned dependencies
pip install -r requirements-dev.txt
pip install -e .

# 3. Configure environment
cp .env.example .env
# Edit .env with your credentials

# 4. Execute test suite
pytest
```

### Databricks Deployment via Asset Bundles (DAB)
```bash
# Validate bundle configuration
databricks bundle validate --target dev

# Deploy pipeline and workflows to Databricks
databricks bundle deploy --target dev

# Trigger pipeline run
databricks bundle run fraudshield_pipeline --target dev
```
