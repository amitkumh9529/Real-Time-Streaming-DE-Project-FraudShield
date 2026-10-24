"""Security and cryptographic tokenization utilities for FraudShield.

Ensures compliance with PCI-DSS by tokenizing primary account numbers (PAN)
and generating non-reversible deterministic tokens and presentation masks.
"""

from __future__ import annotations

import hashlib
import hmac
import os
import re

# Standard salt for deterministic HMAC tokenization.
# In production, this must be injected via Databricks Secret Scopes or KMS.
DEFAULT_SALT = "fraudshield-pci-token-salt-v1"


def get_token_salt() -> str:
    """Retrieve token salt from environment or return default."""
    return os.getenv("CARD_TOKEN_SALT", DEFAULT_SALT)


def tokenize_card_number(card_number: str | None, salt: str | None = None) -> str:
    """Generate a deterministic HMAC-SHA256 token for a card number.

    Allows join correlation across tables without storing the raw PAN.
    """
    if not card_number:
        return "TOKEN_EMPTY"

    # Strip spaces and hyphens
    sanitized = re.sub(r"[\s\-]", "", str(card_number))
    if not sanitized:
        return "TOKEN_EMPTY"

    active_salt = salt or get_token_salt()
    token = hmac.new(
        active_salt.encode("utf-8"),
        sanitized.encode("utf-8"),
        hashlib.sha256
    ).hexdigest()
    return f"tok_{token[:24]}"


def mask_card_number(card_number: str | None) -> str:
    """Mask card number showing only the last 4 digits (PCI-DSS compliant)."""
    if not card_number:
        return "****"
    sanitized = re.sub(r"[\s\-]", "", str(card_number))
    if len(sanitized) < 4:
        return "****"
    return f"****{sanitized[-4:]}"


def mask_email(email: str | None) -> str:
    """Mask email for privacy in logs and analytical outputs."""
    if not email or "@" not in email:
        return "user***@example.com"
    parts = email.split("@", 1)
    username = parts[0]
    domain = parts[1]
    if len(username) <= 2:
        masked_user = username[0] + "***"
    else:
        masked_user = username[0] + "***" + username[-1]
    return f"{masked_user}@{domain}"
