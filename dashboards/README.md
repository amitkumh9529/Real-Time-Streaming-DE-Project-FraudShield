# FraudShield Lakeview Fraud Detection Dashboard

This directory contains the declarative **Databricks AI/BI (Lakeview)** dashboard definition for real-time transaction monitoring and fraud analytics.

## Dashboard File
- `fraudshield_fraud_monitoring.lvdash.json`: Complete serialized Lakeview dashboard layout, dataset definitions, visual widgets, and parameterized filters.

## Visualized Metrics & KPIs
1. **Total Alerts Today**: Union of real-time watchlist match alerts and high-value threshold alerts.
2. **Total Transactions Today**: Daily transaction volume.
3. **Average Transaction Amount**: Real-time spending baseline.
4. **High Risk Customers Count**: Active accounts with a risk score > 70.
5. **Alert Trends (24h Lookback)**: Hourly breakdown of fraud alerts.
6. **Alert Type Breakdown**: Relative volume of `FRAUD_WATCHLIST_MATCH` vs `HIGH_VALUE_TRANSACTION`.
7. **Transaction Volume by Minute**: Intraday streaming throughput.
8. **Domestic vs. International**: Percentage distribution of cross-border card usage.
9. **Top Merchants by Volume**: Highest traffic merchant partners.
10. **Merchant Category Spend**: Aggregated expenditure by vertical (Electronics, Dining, Travel, etc.).
11. **Payment Channel Breakdown**: Traffic across POS, Online, Mobile, and ATM channels.
12. **Geographical Transaction Heatmap**: Global transaction distribution by country.
13. **Watchlist Risk Distribution**: Active watchlist profiles classified by risk severity (`LOW`, `MEDIUM`, `HIGH`, `CRITICAL`).

## How to Import into Databricks
1. Log in to your **Databricks Workspace**.
2. Navigate to **Dashboards** in the left sidebar.
3. Click **Create Dashboard** -> Select **Import from file**.
4. Upload `fraudshield_fraud_monitoring.lvdash.json`.
5. Ensure your SQL Warehouse has read access to the `fraudshield` catalog (`fraudshield.silver.*` and `fraudshield.gold.*`).
6. Click **Publish** to share with risk officers, compliance analysts, and executive stakeholders.
