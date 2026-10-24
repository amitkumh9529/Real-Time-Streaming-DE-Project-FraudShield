"""Unit tests for reliable, idempotent alert delivery and retry state transitions."""

from unittest.mock import MagicMock

import pytest

from services.alert_dispatcher import AlertDispatcher


@pytest.fixture
def mock_smtp_factory():
    """Create a mock SMTP server client."""
    mock_server = MagicMock()
    return MagicMock(return_value=mock_server), mock_server


def test_alert_dispatcher_successful_delivery(mock_smtp_factory):
    """Verify pending alert is sent and transitioned to SENT status."""
    factory, mock_server = mock_smtp_factory
    dispatcher = AlertDispatcher(
        smtp_sender="alerts@test.com",
        smtp_password="test-password",
        smtp_client_factory=factory
    )

    record = {
        "alert_id": "FRAUD-TXN100-WL01",
        "alert_type": "FRAUD_WATCHLIST_MATCH",
        "customer_name": "Test User",
        "recipient_email": "user@example.com",
        "delivery_status": "PENDING",
        "retry_count": 0,
        "max_retries": 3,
        "payload_json": {
            "transaction_id": "TXN100",
            "card_last_four": "****1234",
            "amount": 9500.0,
            "currency": "INR",
            "fraud_score": 85
        }
    }

    result = dispatcher.process_outbox_record(record)
    assert result["delivery_status"] == "SENT"
    assert result["failure_reason"] is None
    assert mock_server.send_message.called


def test_alert_dispatcher_idempotency_prevents_duplicate_sends(mock_smtp_factory):
    """Verify that retrying a previously sent alert ID skips network dispatch."""
    factory, mock_server = mock_smtp_factory
    dispatcher = AlertDispatcher(
        smtp_sender="alerts@test.com",
        smtp_password="test-password",
        smtp_client_factory=factory
    )

    record = {
        "alert_id": "FRAUD-TXN200-WL02",
        "alert_type": "FRAUD_WATCHLIST_MATCH",
        "customer_name": "Test User",
        "recipient_email": "user@example.com",
        "delivery_status": "PENDING",
        "retry_count": 0,
        "max_retries": 3,
        "payload_json": {"transaction_id": "TXN200", "amount": 100.0}
    }

    # First attempt: succeeds
    res1 = dispatcher.process_outbox_record(record)
    assert res1["delivery_status"] == "SENT"
    assert mock_server.send_message.call_count == 1

    # Second attempt with same alert ID (pipeline retry): must be a no-op!
    res2 = dispatcher.process_outbox_record(record)
    assert res2["delivery_status"] == "SENT"
    assert mock_server.send_message.call_count == 1  # Not called again!


def test_alert_dispatcher_retry_failure_increments_count():
    """Verify network exceptions increment retry count and set FAILED status."""
    failing_factory = MagicMock(side_effect=ConnectionError("SMTP connection timed out"))
    dispatcher = AlertDispatcher(
        smtp_sender="alerts@test.com",
        smtp_password="test-password",
        smtp_client_factory=failing_factory,
        max_retries=3
    )

    record = {
        "alert_id": "FRAUD-TXN300-WL03",
        "delivery_status": "PENDING",
        "retry_count": 0,
        "max_retries": 3,
        "payload_json": {}
    }

    result = dispatcher.process_outbox_record(record)
    assert result["delivery_status"] == "FAILED"
    assert result["retry_count"] == 1
    assert "ConnectionError" in result["failure_reason"]


def test_alert_dispatcher_transitions_to_dead_letter_queue():
    """Verify record transitions to DEAD_LETTER when max retries are exceeded."""
    failing_factory = MagicMock(side_effect=Exception("Permanent authentication failure"))
    dispatcher = AlertDispatcher(
        smtp_sender="alerts@test.com",
        smtp_password="test-password",
        smtp_client_factory=failing_factory,
        max_retries=3
    )

    record = {
        "alert_id": "FRAUD-TXN400-WL04",
        "delivery_status": "FAILED",
        "retry_count": 2,  # On next failure will hit 3 (max_retries)
        "max_retries": 3,
        "payload_json": {}
    }

    result = dispatcher.process_outbox_record(record)
    assert result["delivery_status"] == "DEAD_LETTER"
    assert result["retry_count"] == 3
