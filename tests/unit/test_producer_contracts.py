"""Unit tests for Pydantic producer data contracts and boundary validations."""

import pytest
from contracts import CustomerProfile, TransactionEvent, WatchlistRecord
from pydantic import ValidationError


def test_transaction_contract_valid(sample_transaction_dict):
    """Verify standard valid transaction succeeds schema validation."""
    event = TransactionEvent(**sample_transaction_dict)
    assert event.transaction_id == "TXN100001"
    assert event.amount == 1450.50
    assert event.fraud_score == 15
    assert event.fraud_reason == "NORMAL"


def test_transaction_contract_rejects_negative_or_zero_amount(sample_transaction_dict):
    """Verify amounts <= 0 are rejected by the data contract."""
    bad_payload = {**sample_transaction_dict, "amount": -10.0}
    with pytest.raises(ValidationError):
        TransactionEvent(**bad_payload)

    zero_payload = {**sample_transaction_dict, "amount": 0.0}
    with pytest.raises(ValidationError):
        TransactionEvent(**zero_payload)


def test_transaction_contract_rejects_invalid_id_format(sample_transaction_dict):
    """Verify malformed transaction IDs (missing prefix) are rejected."""
    bad_payload = {**sample_transaction_dict, "transaction_id": "BAD_ID_123"}
    with pytest.raises(ValidationError):
        TransactionEvent(**bad_payload)


def test_transaction_contract_rejects_invalid_timestamp(sample_transaction_dict):
    """Verify invalid date strings fail parsing."""
    bad_payload = {**sample_transaction_dict, "transaction_timestamp": "not-a-timestamp"}
    with pytest.raises(ValidationError):
        TransactionEvent(**bad_payload)


def test_transaction_contract_fraud_score_bounds(sample_transaction_dict):
    """Verify fraud score must be within 0 to 100."""
    with pytest.raises(ValidationError):
        TransactionEvent(**{**sample_transaction_dict, "fraud_score": 105})
    with pytest.raises(ValidationError):
        TransactionEvent(**{**sample_transaction_dict, "fraud_score": -5})


def test_customer_contract_valid(sample_customer_dict):
    """Verify valid customer profile record succeeds validation."""
    customer = CustomerProfile(**sample_customer_dict)
    assert customer.customer_id == "CUST000001"
    assert customer.transaction_limit == 50000.0


def test_watchlist_contract_valid(sample_watchlist_dict):
    """Verify valid watchlist entry succeeds validation."""
    record = WatchlistRecord(**sample_watchlist_dict)
    assert record.watchlist_id == "WL000001"
    assert record.status == "ACTIVE"
