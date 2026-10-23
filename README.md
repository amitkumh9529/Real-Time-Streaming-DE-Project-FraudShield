# FraudShield: Real-Time Fraud Detection & Transaction Intelligence Lakehouse

A production-grade, event-driven Data Engineering lakehouse architecture for detecting fraudulent credit card transactions, enforcing spending limits, and streaming real-time security alerts.

---

## Architecture Overview

```
                      +--------------------------------------------------------------+
                      |                   HETEROGENEOUS DATA SOURCES                 |
                      +--------------------------------------------------------------+
                         |                           |                             |
                [PostgreSQL OLTP]          [Confluent Cloud Kafka]          [Cloud Storage Volumes]
               Customer Master Data       Real-Time Card Transactions         Watchlist JSON Drops
             (Historic + Incremental)    (Normal & Simulated Fraud)         (Regulatory / Sanctions)
                         |                           |                             |
                         v                           v                             v
                   Lakeflow / JDBC           Structured Streaming              Auto Loader
                         |                           |                             |
=========================|===========================|=============================|========================
                         v                           v                             v
              [fraudshield.bronze.customers]    [fraudshield.bronze.transactions]  [fraudshield.bronze.fraud_watchlist]
=========================|===========================|=============================|========================
                         | DLT Quality Checks        | DLT Schema Expectations     | Uppercase normalization
                         | Type conversions          | Valid ID/Card, Amount > 0   | Timestamp parsing
                         v                           v                             v
              [fraudshield.silver.customers]    [fraudshield.silver.transactions]  [fraudshield.silver.fraud_watchlist]
=========================|===========================|=============================|========================
                         |                           |                             |
                         +------------+       +------+-------+                     |
                                      |       |              |                     |
                                      v       v              v                     v
                        +---------------------------+  +---------------------------------+
                        |  High-Value Limit Alerts  |  |  Fraud Watchlist Matches Alert  |
                        |  Stream-Static Left Join  |  |  Stream-Stream 5m Watermark Join|
                        +---------------------------+  +---------------------------------+
                                      |                              |
                         +------------+------------------------------+
                         |
                         v
              +-------------------------------------+-------------------------------------+
              |                                     |                                     |
              v                                     v                                     v
  [dlt_pipelines/alerts/]                [Tumbling & Sliding Windows]                [dashboards/]
  SMTP Alert Dispatcher                   fraudshield.gold.transaction_count            Databricks AI/BI Lakeview
  Masked PII (Card: ****1234)             Real-Time Throughput / Minute              Operational Fraud Dashboard
```

---

## Repository Structure

```
FraudShield/
├── .env.example                     # Environment variables configuration template
├── .gitignore                       # Standard git ignore rules (secrets, venvs, cache)
├── README.md                        # Project technical documentation & architecture
│
├── dlt_pipelines/                   # Production Delta Live Tables (DLT) Streaming Pipeline
│   ├── README.md                    # DLT pipeline execution and deployment instructions
│   ├── bronze/                      # Raw ingestion from Kafka and Cloud Storage
│   │   ├── transactions_bronze.py   # Kafka stream ingestion with message metadata
│   │   └── fraud_watchlist_bronze.py# Auto Loader cloudFiles ingestion with rescued data
│   ├── silver/                      # Cleaned, validated, and conformed data
│   │   ├── customers_silver.py      # Customer profile hygiene and deduplication
│   │   ├── transactions_silver.py   # Transaction JSON parsing & DLT quality expectations
│   │   └── fraud_watchlist_silver.py# Standardized entity strings and timestamp parsing
│   ├── gold/                        # Business metrics, joins, and fraud detection logic
│   │   ├── fraud_card_alert.py      # Watermarked stream-stream join (Watchlist match)
│   │   ├── high_value_transactions_alert.py # Stream-static join (Limit violations)
│   │   ├── transaction_count_by_minute.py   # 1-minute tumbling window metrics
│   │   └── transaction_count_by_minute_sliding_window.py # 5-minute sliding window metrics
│   └── alerts/                      # Event-driven automated notifications
│       ├── fraud_card_alert_email_notifier.py # Micro-batch sink for fraud watchlist alerts
│       └── high_value_transaction_email_notifier.py # Micro-batch sink for high-value alerts
│
├── notebooks/                       # Databricks interactive notebooks & utilities
│   ├── README.md                    # Notebooks documentation
│   ├── setup/                       # Workspace & credential configuration
│   │   └── setup_secret_scope.py    # Databricks secret scope initialization
│   ├── tests/                       # Pipeline smoke tests & diagnostics
│   │   ├── test_kafka_streaming.py  # Kafka connectivity & ingestion validation
│   │   ├── test_autoloader.py       # Cloud storage auto-loader test
│   │   └── test_email_notification.py # Email dispatch verification
│   └── generators/                  # Cloud storage feed simulators
│       ├── fraud_watchlist_data_generator.py # Incremental JSON feed generator
│       └── fraud_watchlist.csv      # Seed watchlist entity catalog
│
├── kafka_producer/                  # Python credit card transaction simulator
│   ├── README.md                    # Producer execution guide
│   ├── requirements.txt             # Python dependencies
│   ├── config.py                    # Environment settings loader
│   ├── models.py                    # Domain dataclasses
│   ├── fraud_engine.py              # Behavioral & anomaly fraud evaluation engine
│   ├── customer_generator.py        # Realistic customer profile generator
│   ├── merchant_generator.py        # Merchant catalog generator
│   ├── transaction_generator.py     # Continuous transaction event stream generator
│   ├── producer_normal.py           # Produces legitimate transaction stream
│   ├── producer_fraud_card.py       # Probes known watchlist card attacks
│   ├── producer_fraud_transaction.py# Probes high-value & anomaly fraud attacks
│   ├── consumer.py                  # CLI consumer for topic validation
│   ├── utils.py                     # Shared helpers and validators
│   └── data/                        # Generated seed CSVs
│
├── sql/                             # PostgreSQL OLTP database scripts
│   ├── README.md                    # SQL deployment instructions
│   ├── customers_historic.sql       # Baseline schema and 1,000 customer records
│   └── customers_incremental.sql    # CDC incremental insert/update simulation
│
└── dashboards/                      # Visual analytics and BI
    ├── README.md                    # Lakeview import and configuration guide
    └── fraudshield_fraud_monitoring.lvdash.json # Declarative Databricks AI/BI Dashboard
```

---

## Quickstart Guide

### 1. Prerequisites
- Python 3.10+
- Databricks Workspace (Runtime 14.3+ LTS recommended, Unity Catalog enabled)
- Confluent Cloud Kafka cluster
- PostgreSQL instance (optional, for customer master OLTP simulation)

### 2. Configure Local Simulator
```bash
cd kafka_producer
python -m venv .venv

# Windows
.\.venv\Scripts\Activate.ps1
# Linux / macOS
source .venv/bin/activate

pip install -r requirements.txt
copy .env.example .env
# Edit .env with your Kafka credentials
```

### 3. Run Transaction Simulators
- **Normal Traffic**:
  ```bash
  python producer_normal.py
  ```
- **Simulate Watchlist Card Attack**:
  ```bash
  python producer_fraud_card.py
  ```
- **Simulate Anomaly & High-Value Attacks**:
  ```bash
  python producer_fraud_transaction.py
  ```

### 4. Deploy Databricks Secret Scope & Pipelines
1. Run `notebooks/setup/setup_secret_scope.py` in your Databricks Workspace to securely configure `fraudshield-scope`.
2. Create a new **Delta Live Tables Pipeline** pointing to `dlt_pipelines/`.
3. Set destination catalog to `fraudshield`.
4. Import `dashboards/fraudshield_fraud_monitoring.lvdash.json` into Databricks **Dashboards**.
