"""Unit tests for deterministic transaction deduplication logic."""



def deduplicate_transactions(transactions: list[dict], key: str = "transaction_id") -> list[dict]:
    """Pure-python simulation of streaming event deduplication by key."""
    seen = set()
    deduped = []
    for txn in transactions:
        val = txn.get(key)
        if val not in seen:
            seen.add(val)
            deduped.append(txn)
    return deduped


def test_deduplication_filters_identical_ids():
    """Verify duplicate records are filtered out while preserving the first occurrence."""
    raw_events = [
        {"transaction_id": "TXN001", "amount": 100.0, "offset": 1},
        {"transaction_id": "TXN002", "amount": 200.0, "offset": 2},
        {"transaction_id": "TXN001", "amount": 100.0, "offset": 3},  # Duplicate re-transmission
        {"transaction_id": "TXN003", "amount": 300.0, "offset": 4},
        {"transaction_id": "TXN002", "amount": 200.0, "offset": 5},  # Duplicate
    ]

    result = deduplicate_transactions(raw_events, "transaction_id")
    assert len(result) == 3
    assert [r["transaction_id"] for r in result] == ["TXN001", "TXN002", "TXN003"]
    assert result[0]["offset"] == 1  # Retained original arrival
