# Credit Card Transaction Simulator

A production-oriented Python simulator that generates realistic credit card transactions and publishes them to a Kafka topic for downstream fraud detection workflows.

## Features

- Generates realistic customer and merchant datasets
- Simulates spending behavior that is consistent per customer
- Applies configurable fraud rules and scores
- Publishes JSON transactions to Confluent Cloud Kafka
- Logs delivery status, transaction metadata, and runtime stats
- Handles keyboard interrupts and graceful shutdown

## Installation

### 1. Create a virtual environment

```bash
python -m venv .venv
```

### 2. Activate the environment

On Windows:

```powershell
.\.venv\Scripts\Activate.ps1
```

On macOS/Linux:

```bash
source .venv/bin/activate
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

## Environment Configuration

Create a `.env` file from the example:

```bash
copy .env.example .env
```

Update the values for your Kafka cluster:

```env
BOOTSTRAP_SERVERS=your-bootstrap-servers:9092
API_KEY=your_api_key
API_SECRET=your_api_secret
TOPIC_NAME=credit_card_transactions
TRANSACTIONS_PER_SECOND=5
FRAUD_PERCENTAGE=0.08
TOTAL_CUSTOMERS=1000
TOTAL_MERCHANTS=200
RANDOM_SEED=42
```

## Running the Producer

Run one of the specialized simulator scripts depending on the test scenario:

- **Legitimate Transactions Stream**:
  ```bash
  python producer_normal.py
  ```
- **Simulate Watchlist Card Attacks**:
  ```bash
  python producer_fraud_card.py
  ```
- **Simulate Anomaly & High-Value Fraud Attacks**:
  ```bash
  python producer_fraud_transaction.py
  ```
- **Validate Ingested Stream (CLI Consumer)**:
  ```bash
  python consumer.py
  ```

The simulator will:
1. Generate customer and merchant datasets in `data/` if they do not already exist.
2. Produce continuous JSON transactions to the configured Kafka topic.
3. Apply behavioral fraud evaluations in real-time.

## Project Architecture

- [config.py](file:///d:/python%20dev/Fingaurd/kafka_producer/config.py) loads configuration values from `.env`
- [models.py](file:///d:/python%20dev/Fingaurd/kafka_producer/models.py) defines dataclasses for customers, merchants, transactions, and stats
- [customer_generator.py](file:///d:/python%20dev/Fingaurd/kafka_producer/customer_generator.py) creates realistic customer records
- [merchant_generator.py](file:///d:/python%20dev/Fingaurd/kafka_producer/merchant_generator.py) creates merchant records
- [transaction_generator.py](file:///d:/python%20dev/Fingaurd/kafka_producer/transaction_generator.py) simulates transaction behavior
- [fraud_engine.py](file:///d:/python%20dev/Fingaurd/kafka_producer/fraud_engine.py) evaluates fraud conditions and scores
- [producer_normal.py](file:///d:/python%20dev/Fingaurd/kafka_producer/producer_normal.py) publishes standard legitimate traffic
- [producer_fraud_card.py](file:///d:/python%20dev/Fingaurd/kafka_producer/producer_fraud_card.py) probes watchlist cards
- [producer_fraud_transaction.py](file:///d:/python%20dev/Fingaurd/kafka_producer/producer_fraud_transaction.py) probes high-value and anomaly attacks
- [consumer.py](file:///d:/python%20dev/Fingaurd/kafka_producer/consumer.py) reads topic messages for diagnostics
- [utils.py](file:///d:/python%20dev/Fingaurd/kafka_producer/utils.py) contains shared helpers

## Folder Explanation

- `data/` stores generated customer and merchant CSV files
- `.env` contains runtime secrets and Kafka configuration
- `requirements.txt` lists project dependencies

## Future Enhancements

- Add schema validation using JSON Schema or Pydantic
- Support multiple Kafka topics for fraud vs. clean traffic
- Persist transaction history for replay and testing
- Add Prometheus metrics for monitoring
