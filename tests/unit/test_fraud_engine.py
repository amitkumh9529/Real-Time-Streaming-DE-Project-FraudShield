"""Unit tests for the behavioral fraud evaluation engine."""

from datetime import datetime, timezone

import pytest
from fraud_engine import FraudEngine
from models import Customer, Merchant, Transaction


@pytest.fixture
def mock_customer():
    return Customer(
        customer_id="CUST0001",
        first_name="Test",
        last_name="User",
        gender="Male",
        age=30,
        city="Delhi",
        state="Delhi",
        country="India",
        annual_income=1000000.0,
        customer_segment="Regular",
        account_open_date="2022-01-01",
        risk_score=20.0,
        preferred_spending_min=100.0,
        preferred_spending_max=10000.0,
        preferred_city="Delhi",
        preferred_country="India",
        trusted_device_id="DEV0001",
        card_number="4321819600133898",
        card_type="Visa"
    )


@pytest.fixture
def mock_merchant():
    return Merchant(
        merchant_id="M001",
        merchant_name="Safe Store",
        merchant_category="Grocery",
        city="Delhi",
        country="India",
        merchant_risk="LOW",
        is_blacklisted=False
    )


def test_fraud_engine_normal_transaction(mock_customer, mock_merchant):
    """Verify normal compliant transactions produce zero or minimal score."""
    engine = FraudEngine(fraud_percentage=0.0, seed=42)
    txn = Transaction(
        transaction_id="TXN001",
        customer_id="CUST0001",
        card_number="4321819600133898",
        merchant_id="M001",
        merchant_name="Safe Store",
        merchant_category="Grocery",
        amount=500.0,
        currency="INR",
        transaction_type="Purchase",
        payment_channel="POS",
        device_id="DEV0001",
        city="Delhi",
        country="India",
        transaction_timestamp=datetime.now(timezone.utc).isoformat(),
        is_international=False,
        fraud_score=0,
        fraud_reason="NORMAL"
    )
    score, reason = engine.evaluate(txn, mock_customer, mock_merchant)
    assert score == 0
    assert reason == "NORMAL"


def test_fraud_engine_high_value_flag(mock_customer, mock_merchant):
    """Verify transactions exceeding 100,000 trigger HIGH_VALUE_TRANSACTION rule."""
    engine = FraudEngine(fraud_percentage=0.0, seed=42)
    txn = Transaction(
        transaction_id="TXN002",
        customer_id="CUST0001",
        card_number="4321819600133898",
        merchant_id="M001",
        merchant_name="Safe Store",
        merchant_category="Grocery",
        amount=150000.0,
        currency="INR",
        transaction_type="Purchase",
        payment_channel="POS",
        device_id="DEV0001",
        city="Delhi",
        country="India",
        transaction_timestamp=datetime.now(timezone.utc).isoformat(),
        is_international=False,
        fraud_score=0,
        fraud_reason="NORMAL"
    )
    score, reason = engine.evaluate(txn, mock_customer, mock_merchant)
    assert score >= 40
    assert "HIGH_VALUE_TRANSACTION" in reason


def test_fraud_engine_blacklisted_merchant(mock_customer, mock_merchant):
    """Verify transactions at blacklisted merchants trigger rule and assign heavy penalty."""
    mock_merchant.is_blacklisted = True
    engine = FraudEngine(fraud_percentage=0.0, seed=42)
    txn = Transaction(
        transaction_id="TXN003",
        customer_id="CUST0001",
        card_number="4321819600133898",
        merchant_id="M001",
        merchant_name="Shady Merchant",
        merchant_category="Electronics",
        amount=250.0,
        currency="INR",
        transaction_type="Purchase",
        payment_channel="Online",
        device_id="DEV0001",
        city="Delhi",
        country="India",
        transaction_timestamp=datetime.now(timezone.utc).isoformat(),
        is_international=False,
        fraud_score=0,
        fraud_reason="NORMAL"
    )
    score, reason = engine.evaluate(txn, mock_customer, mock_merchant)
    assert score >= 60
    assert "BLACKLISTED_MERCHANT" in reason
