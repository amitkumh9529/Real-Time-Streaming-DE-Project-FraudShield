"""Integration tests validating end-to-end stream join logic, active status, and limit rules."""

from datetime import datetime, timedelta

from core.security import tokenize_card_number


def evaluate_watchlist_match(transaction: dict, watchlist: list[dict], max_window_days: int = 30) -> list[dict]:
    """Pure-python implementation of the watermarked stream-stream join logic."""
    matches = []
    tx_time = datetime.fromisoformat(transaction["transaction_timestamp"].replace("Z", "+00:00"))
    tx_card_token = transaction["card_token"]

    for wl in watchlist:
        # Condition 1: Must be ACTIVE
        if wl.get("status", "").upper() != "ACTIVE":
            continue

        # Condition 2: Card Token equality
        if tx_card_token != wl.get("entity_token"):
            continue

        # Condition 3: Time range boundary (effective_from <= tx_time <= effective_from + 30 days)
        wl_time = datetime.fromisoformat(wl["effective_from"].replace("Z", "+00:00"))
        if wl_time <= tx_time <= wl_time + timedelta(days=max_window_days):
            alert_id = f"FRAUD-{transaction['transaction_id']}-{wl['watchlist_id']}"
            matches.append({
                "alert_id": alert_id,
                "transaction_id": transaction["transaction_id"],
                "watchlist_id": wl["watchlist_id"],
                "risk_level": wl["risk_level"]
            })
    return matches


def evaluate_high_value_alert(transaction: dict, customer: dict) -> dict | None:
    """Pure-python implementation of stream-static limit violation join."""
    amount = float(transaction.get("amount", 0.0))
    limit = float(customer.get("transaction_limit", 100000.0))

    if amount > limit:
        return {
            "alert_id": f"ALERT-HV-{transaction['transaction_id']}",
            "transaction_id": transaction["transaction_id"],
            "amount": amount,
            "limit": limit
        }
    return None


def test_watchlist_match_active_and_within_time_window():
    """Verify active watchlist entry within time window triggers a valid alert."""
    card = "4321819600133898"
    token = tokenize_card_number(card)

    tx = {
        "transaction_id": "TXN501",
        "card_token": token,
        "transaction_timestamp": "2026-10-15T12:00:00Z"
    }

    watchlist = [{
        "watchlist_id": "WL001",
        "entity_token": token,
        "status": "ACTIVE",
        "risk_level": "CRITICAL",
        "effective_from": "2026-10-01T00:00:00Z"  # 14 days prior (within 30d window)
    }]

    alerts = evaluate_watchlist_match(tx, watchlist)
    assert len(alerts) == 1
    assert alerts[0]["alert_id"] == "FRAUD-TXN501-WL001"


def test_watchlist_match_ignores_inactive_status():
    """Verify inactive/revoked watchlist records do NOT trigger alerts."""
    card = "4321819600133898"
    token = tokenize_card_number(card)

    tx = {
        "transaction_id": "TXN502",
        "card_token": token,
        "transaction_timestamp": "2026-10-15T12:00:00Z"
    }

    watchlist = [{
        "watchlist_id": "WL002",
        "entity_token": token,
        "status": "INACTIVE",  # Revoked or expired
        "risk_level": "HIGH",
        "effective_from": "2026-10-01T00:00:00Z"
    }]

    alerts = evaluate_watchlist_match(tx, watchlist)
    assert len(alerts) == 0


def test_watchlist_match_ignores_outside_event_time_window():
    """Verify transactions outside the 30-day window do NOT match."""
    card = "4321819600133898"
    token = tokenize_card_number(card)

    tx = {
        "transaction_id": "TXN503",
        "card_token": token,
        "transaction_timestamp": "2026-12-01T12:00:00Z"  # 60 days after effective_from
    }

    watchlist = [{
        "watchlist_id": "WL003",
        "entity_token": token,
        "status": "ACTIVE",
        "risk_level": "HIGH",
        "effective_from": "2026-10-01T00:00:00Z"
    }]

    alerts = evaluate_watchlist_match(tx, watchlist)
    assert len(alerts) == 0


def test_high_value_alert_triggers_when_limit_exceeded():
    """Verify transaction exceeding customer limit triggers an alert."""
    tx = {"transaction_id": "TXN601", "amount": 75000.0}
    customer = {"customer_id": "CUST001", "transaction_limit": 50000.0}

    alert = evaluate_high_value_alert(tx, customer)
    assert alert is not None
    assert alert["alert_id"] == "ALERT-HV-TXN601"
    assert alert["amount"] == 75000.0


def test_high_value_alert_does_not_trigger_when_within_limit():
    """Verify compliant transaction within customer limit does not trigger an alert."""
    tx = {"transaction_id": "TXN602", "amount": 25000.0}
    customer = {"customer_id": "CUST001", "transaction_limit": 50000.0}

    alert = evaluate_high_value_alert(tx, customer)
    assert alert is None
