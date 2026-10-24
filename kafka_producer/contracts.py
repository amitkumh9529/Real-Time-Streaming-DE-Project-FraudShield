"""Data Contracts and Schema Validation for Producer Boundaries.

Defines Pydantic schemas validating transactions, customers, and watchlist
events before publishing to Kafka or dropping into storage volumes.
"""

from __future__ import annotations

import re
from datetime import datetime
from typing import Optional

try:
    from pydantic import BaseModel, Field, field_validator
except ImportError:
    # Fallback compatibility if pydantic v1 is installed
    from pydantic import BaseModel, Field
    from pydantic import validator as field_validator


class TransactionEvent(BaseModel):
    """Schema contract for real-time credit card transactions published to Kafka."""

    event_id: str = Field(description="Deterministic or UUID event identifier")
    transaction_id: str = Field(description="Unique business transaction ID")
    customer_id: str = Field(description="Customer account ID")
    card_number: str = Field(description="Primary card number for transaction")
    merchant_id: str = Field(description="Merchant identifier")
    merchant_name: str = Field(description="Merchant business name")
    merchant_category: str = Field(description="Merchant category (e.g., Electronics, Dining)")
    amount: float = Field(gt=0, description="Transaction amount in local currency (must be > 0)")
    currency: str = Field(default="INR", description="ISO 4217 currency code")
    transaction_type: str = Field(default="Purchase", description="Type of transaction (Purchase, Withdrawal)")
    payment_channel: str = Field(default="POS", description="Payment channel (POS, Online, Mobile, ATM)")
    device_id: str = Field(description="Terminal or mobile device ID")
    city: str = Field(description="Transaction location city")
    country: str = Field(description="Transaction location country")
    transaction_timestamp: str = Field(description="ISO-8601 transaction timestamp")
    is_international: bool = Field(default=False, description="Flag indicating cross-border transaction")
    fraud_score: int = Field(default=0, ge=0, le=100, description="Risk assessment score (0-100)")
    fraud_reason: str = Field(default="NORMAL", description="Primary rule or model classification reason")
    status: str = Field(default="APPROVED", description="Transaction authorization status")

    @field_validator("transaction_id")
    def validate_transaction_id(cls, v: str) -> str:
        if not v or not re.match(r"^TX[NF]?\d+$", v):
            raise ValueError(f"Invalid transaction_id format: '{v}'")
        return v

    @field_validator("amount")
    def validate_amount(cls, v: float) -> float:
        if v <= 0:
            raise ValueError(f"Amount must be strictly positive, got: {v}")
        return round(v, 2)

    @field_validator("transaction_timestamp")
    def validate_timestamp(cls, v: str) -> str:
        try:
            # Validate ISO format
            datetime.fromisoformat(v.replace("Z", "+00:00"))
        except Exception as e:
            raise ValueError(f"Invalid ISO-8601 timestamp '{v}': {e}") from e
        return v


class CustomerProfile(BaseModel):
    """Schema contract for customer master dimension records."""

    customer_id: str
    first_name: str
    last_name: str
    gender: str
    age: int = Field(ge=18, le=120)
    city: str
    state: str
    country: str
    annual_income: float = Field(ge=0)
    customer_segment: str
    account_open_date: str
    risk_score: int = Field(ge=0, le=100)
    preferred_spending_min: float = Field(ge=0)
    preferred_spending_max: float = Field(ge=0)
    preferred_city: str
    preferred_country: str
    trusted_device_id: str
    card_number: str
    card_type: str
    email: str = Field(description="Customer email for alerts")
    transaction_limit: float = Field(gt=0, description="Configured single transaction limit")
    update_timestamp: str


class WatchlistRecord(BaseModel):
    """Schema contract for regulatory and fraud watchlist feeds."""

    watchlist_id: str
    watch_type: str
    entity_id: str
    risk_level: str
    action: str
    reason_code: str
    reason_description: str
    status: str = Field(description="Status: ACTIVE or INACTIVE")
    effective_from: str
    reported_by: str
    reported_source: str
    country: str
    city: Optional[str] = None
