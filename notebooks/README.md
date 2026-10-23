# Databricks Notebooks & Exploratory Workflows

This directory contains standalone Databricks setup, diagnostic, and generator notebooks separate from the production Delta Live Tables (DLT) streaming pipeline code.

## Folder Organization

### 1. `setup/`
- `setup_secret_scope.py`: Configures the Databricks Secret Scope (`fraudshield-scope`) via Databricks REST API. Securely provisions:
  - `kafka_connection_details`: Confluent Cloud bootstrap servers, API key, and secret.
  - `gmail_api_key`: App password for the automated alert notification dispatcher.

### 2. `tests/`
- `test_kafka_streaming.py`: End-to-end smoke test validating Confluent Cloud Kafka SASL_SSL connectivity, batch schema parsing, and stream write to Delta.
- `test_autoloader.py`: Validates Databricks Auto Loader (`cloudFiles`) ingestion from Volume paths with automatic schema inference.
- `test_email_notification.py`: Validates SMTP TLS authentication and email delivery outside the streaming engine.

### 3. `generators/`
- `fraud_watchlist_data_generator.py`: Generates incremental, timestamped JSON watch files into `/Volumes/fraudshield/source/fraud_watchlist/source_data/` by tracking the latest processed `watchlist_id`.
- `fraud_watchlist.csv`: Seed catalog of high-risk entities and sanctions.
