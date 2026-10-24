"""Smoke tests for pipeline code structure and static syntax validity."""

import ast
from pathlib import Path

import pytest

PIPELINES_DIR = Path(__file__).resolve().parent.parent.parent / "dlt_pipelines"


@pytest.mark.parametrize("pipeline_file", [
    "bronze/transactions_bronze.py",
    "bronze/fraud_watchlist_bronze.py",
    "bronze/customers_bronze.py",
    "silver/transactions_silver.py",
    "silver/fraud_watchlist_silver.py",
    "silver/customers_silver.py",
    "gold/fraud_card_alert.py",
    "gold/high_value_transactions_alert.py",
    "gold/alert_outbox.py",
    "gold/transaction_count_by_minute.py",
    "gold/transaction_count_by_minute_sliding_window.py",
])
def test_pipeline_file_syntax(pipeline_file):
    """Verify all DLT pipeline files are syntactically valid Python code."""
    file_path = PIPELINES_DIR / pipeline_file
    assert file_path.exists(), f"Pipeline file {pipeline_file} does not exist"

    with open(file_path, "r", encoding="utf-8") as f:
        source = f.read()

    # Parse AST to ensure no syntax errors exist
    tree = ast.parse(source, filename=str(file_path))
    assert tree is not None
