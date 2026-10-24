"""Unit tests for PCI-DSS cryptographic tokenization and data masking."""

from core.security import mask_card_number, mask_email, tokenize_card_number


def test_tokenize_card_number_deterministic():
    """Verify that identical card numbers produce identical tokens with the same salt."""
    card = "4321819600133898"
    token1 = tokenize_card_number(card)
    token2 = tokenize_card_number(card)
    assert token1 == token2
    assert token1.startswith("tok_")
    assert len(token1) == 28  # "tok_" (4) + 24 hex characters


def test_tokenize_card_number_different_inputs():
    """Verify distinct cards generate distinct non-colliding tokens."""
    card1 = "4321819600133898"
    card2 = "5026542351161551"
    assert tokenize_card_number(card1) != tokenize_card_number(card2)


def test_tokenize_card_number_handles_whitespace_and_hyphens():
    """Verify formatting characters (spaces, dashes) do not affect token output."""
    raw = "4321819600133898"
    formatted = "4321-8196-0013-3898"
    spaced = "4321 8196 0013 3898"
    assert tokenize_card_number(raw) == tokenize_card_number(formatted)
    assert tokenize_card_number(raw) == tokenize_card_number(spaced)


def test_tokenize_card_empty_or_none():
    """Verify empty or None values return fallback TOKEN_EMPTY."""
    assert tokenize_card_number(None) == "TOKEN_EMPTY"
    assert tokenize_card_number("") == "TOKEN_EMPTY"
    assert tokenize_card_number("   ") == "TOKEN_EMPTY"


def test_mask_card_number_pci_compliant():
    """Verify PAN masking reveals only the final 4 digits."""
    assert mask_card_number("4321819600133898") == "****3898"
    assert mask_card_number("5026-5423-5116-1551") == "****1551"
    assert mask_card_number(None) == "****"
    assert mask_card_number("12") == "****"


def test_mask_email():
    """Verify email address is masked for operational privacy."""
    assert mask_email("johndoe@example.com") == "j***e@example.com"
    assert mask_email("ab@company.com") == "a***@company.com"
    assert mask_email(None) == "user***@example.com"
    assert mask_email("invalid-email") == "user***@example.com"
