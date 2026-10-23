# PostgreSQL OLTP Database Scripts

This directory contains the database definition and data ingestion scripts for the **FraudShield** customer master dimension.

## Files
- `customers_historic.sql`: DDL schema definition and initial historical seed data (1,000 verified customer records with account parameters, spending limits, risk scores, and trusted devices).
- `customers_incremental.sql`: Transactional script simulating Change Data Capture (CDC) events:
  - New customer onboarding (`INSERT` operations).
  - Profile attribute changes (`UPDATE` operations on income, risk score, location, and `update_timestamp`).

## Deployment
Execute against your PostgreSQL instance:
```bash
psql -h <host> -U <user> -d <database> -f sql/customers_historic.sql
```

To simulate incremental CDC updates:
```bash
psql -h <host> -U <user> -d <database> -f sql/customers_incremental.sql
```
