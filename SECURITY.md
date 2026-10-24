# Security Policy & Secret Management Guidelines

This document outlines the security architecture, PCI-DSS compliance practices, secret management policies, and automated scanning protocols for the **FraudShield** project.

---

## 1. Zero Plaintext Secrets Policy

- **No Secrets in Source Control**: API keys, passwords, bearer tokens, connection strings, private certificates, or customer emails must never be committed to Git.
- **Local Development**: All credentials must be loaded via local `.env` files (which are strictly ignored by `.gitignore`) or explicit environment variables (`os.getenv`).
- **Databricks Workspaces**: All production credentials (Kafka SASL passwords, API tokens, database passwords, SMTP keys) must be fetched at runtime exclusively via **Databricks Secret Scopes** using `dbutils.secrets.get(scope, key)`.
- **No Token/Credential Logging**: Never use `print()` or loggers to output raw credential strings, access tokens, or connection configurations.

---

## 2. PCI-DSS Compliance & Cardholder Data Protection

In strict adherence to the **Payment Card Industry Data Security Standard (PCI-DSS)**:
1. **No Primary Account Number (PAN) Storage**: Raw 16-digit card numbers must never be persisted in plain text in Delta Lake (Bronze, Silver, Gold), event logs, or alerting tables.
2. **Deterministic Tokenization**: At the ingestion boundary, incoming card numbers are converted into non-reversible, salted HMAC-SHA256 tokens (`tok_<hex24>`). This preserves relational correlation across tables (e.g., watchlist joins) without exposing raw cardholder data.
3. **Presentation Masking**: Any customer-facing notification or dashboard visualization retains strictly the last four digits of the card number (e.g., `****1234`).
4. **Token Salt Rotation**: The salt used for HMAC tokenization (`CARD_TOKEN_SALT`) must be stored in an enterprise Key Management Service (AWS KMS, Azure Key Vault, or GCP KMS) and rotated quarterly.

---

## 3. Secret Scope Hierarchy & Rotation Procedure

### Databricks Secret Scope: `fraudshield-scope`

| Secret Key | Description | Rotation Frequency | Rotation Procedure |
|---|---|---|---|
| `kafka_connection_details` | Confluent Cloud bootstrap, topic, and SASL API key/secret | 90 days | Generate new key pair in Confluent Console, update scope via REST API/CLI, verify consumer groups, deprecate old key. |
| `gmail_api_key` | SMTP application password for alert delivery | 90 days | Revoke old App Password in Google Admin/Security, generate new 16-character token, update scope. |
| `pci_token_salt` | Salt string for card number HMAC hashing | 180 days | Update KMS parameter; run backfill tokenization script for active records. |

### Updating Secrets via Databricks CLI
```bash
# Store or update secret in scope
databricks secrets put-secret fraudshield-scope kafka_connection_details --string-value '{"bootstrap_servers":"...","api_key":"...","api_secret":"..."}'

# Verify secret keys (metadata only, values are never returned)
databricks secrets list-secrets fraudshield-scope
```

---

## 4. Secret Scanning & CI Safeguards

Automated secret scanning is enforced on every commit and pull request via GitHub Actions:
- **TruffleHog / Gitleaks**: Automated scanning verifies no high-entropy strings, AWS/GCP/Azure keys, or private tokens are present in any commit diff.
- **Pre-commit Hooks**: Developers are encouraged to install `detect-secrets` or `pre-commit`:
  ```bash
  pip install detect-secrets
  detect-secrets scan > .secrets.baseline
  ```

---

## 5. Vulnerability Reporting

If you identify a security vulnerability, credential leak, or PCI-DSS violation in this repository:
1. Do **not** open a public GitHub issue.
2. Immediately contact the security operations team at `security@fraudshield.internal`.
3. Provide details of the finding and potential impact for expedited review and remediation.
