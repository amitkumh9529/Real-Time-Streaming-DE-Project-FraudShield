"""Pytest configuration and shared fixtures for FraudShield test suite.

Configures python path and provides mock data fixtures for transactions,
customers, watchlist records, and external service test doubles.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

# Add project root and kafka_producer to sys.path so tests can import modules directly
PROJECT_ROOT = Path(__file__).resolve().parent.parent
KAFKA_PRODUCER_DIR = PROJECT_ROOT / "kafka_producer"

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
if str(KAFKA_PRODUCER_DIR) not in sys.path:
    sys.path.insert(0, str(KAFKA_PRODUCER_DIR))


@pytest.fixture
def sample_transaction_dict():
    """Return a valid transaction payload dictionary."""
    return {
        "event_id": "EVT-TXN100001",
        "transaction_id": "TXN100001",
        "customer_id": "CUST000001",
        "card_number": "4321819600133898",
        "merchant_id": "MERCH0001",
        "merchant_name": "Amazon India",
        "merchant_category": "Electronics",
        "amount": 1450.50,
        "currency": "INR",
        "transaction_type": "Purchase",
        "payment_channel": "POS",
        "device_id": "DEV0001",
        "city": "Mumbai",
        "country": "India",
        "transaction_timestamp": "2026-10-24T10:00:00Z",
        "is_international": False,
        "fraud_score": 15,
        "fraud_reason": "NORMAL",
        "status": "APPROVED",
    }


@pytest.fixture
def sample_customer_dict():
    """Return a valid customer profile dictionary."""
    return {
        "customer_id": "CUST000001",
        "first_name": "Rahul",
        "last_name": "Sharma",
        "gender": "Male",
        "age": 32,
        "city": "Mumbai",
        "state": "Maharashtra",
        "country": "India",
        "annual_income": 1200000.0,
        "customer_segment": "Gold",
        "account_open_date": "2022-01-15",
        "risk_score": 25,
        "preferred_spending_min": 1000.0,
        "preferred_spending_max": 25000.0,
        "preferred_city": "Mumbai",
        "preferred_country": "India",
        "trusted_device_id": "DEV0001",
        "card_number": "4321819600133898",
        "card_type": "Visa",
        "email": "customer01@example.com",
        "transaction_limit": 50000.0,
        "update_timestamp": "2026-10-24T00:00:00Z",
    }


@pytest.fixture
def sample_watchlist_dict():
    """Return a valid watchlist entry dictionary."""
    return {
        "watchlist_id": "WL000001",
        "watch_type": "STOLEN_CARD",
        "entity_id": "4321819600133898",
        "risk_level": "CRITICAL",
        "action": "BLOCK",
        "reason_code": "RC_STOLEN",
        "reason_description": "Reported stolen by issuer bank",
        "status": "ACTIVE",
        "effective_from": "2026-10-01T00:00:00Z",
        "reported_by": "Interpol Fraud Unit",
        "reported_source": "Financial Cyber Division",
        "country": "India",
        "city": "Mumbai",
    }
