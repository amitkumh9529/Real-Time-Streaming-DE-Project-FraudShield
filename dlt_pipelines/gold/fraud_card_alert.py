"""Gold Layer: Real-Time Fraud Watchlist Alert Correlation.

Executes a watermarked stream-to-stream join between incoming transactions
and regulatory watchlists with an explicit event-time constraint to prevent
state store memory explosion. Enriches matches with customer master data.
"""

from __future__ import annotations

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame


@dp.table(
    name="fraudshield.gold.fraud_card_alert",
    comment="Real-time alerts triggered by matching transactions against active fraud watchlist records"
)
def fraud_card_alert() -> DataFrame:
    """Join watermarked transaction and watchlist streams on card_token with time-range boundary."""
    transactions = spark.readStream.table("fraudshield.silver.transactions")
    fraud_watchlist = spark.readStream.table("fraudshield.silver.fraud_watchlist")
    customers = spark.read.table("fraudshield.silver.customers")

    # Filter strictly for active watchlist entries
    active_watchlist = fraud_watchlist.filter(F.col("status") == "ACTIVE")

    # Define watermarks on both event streams
    tx_watermarked = transactions.withWatermark("transaction_timestamp", "10 minutes")
    wl_watermarked = active_watchlist.withWatermark("effective_from", "10 minutes")

    # Explicit event-time interval condition ensures Spark state store eviction
    join_conditions = [
        tx_watermarked.card_token == wl_watermarked.entity_token,
        tx_watermarked.transaction_timestamp >= wl_watermarked.effective_from,
        tx_watermarked.transaction_timestamp <= wl_watermarked.effective_from + F.expr("INTERVAL 30 DAYS")
    ]

    fraud_detected = (
        tx_watermarked.join(
            wl_watermarked,
            join_conditions,
            "inner"
        ).join(
            customers,
            tx_watermarked.customer_id == customers.customer_id,
            "left"
        ).select(
            # Alert identification
            F.concat_ws("-", F.lit("FRAUD"), tx_watermarked.transaction_id, wl_watermarked.watchlist_id).alias("alert_id"),
            F.lit("FRAUD_WATCHLIST_MATCH").alias("alert_type"),
            F.current_timestamp().alias("alert_timestamp"),

            # Transaction details (PCI-DSS compliant: tokenized & masked)
            tx_watermarked.event_id,
            tx_watermarked.transaction_id,
            tx_watermarked.customer_id,
            customers.email.alias("customer_email"),
            F.concat_ws(" ", customers.first_name, customers.last_name).alias("customer_name"),
            tx_watermarked.card_token,
            tx_watermarked.card_last_four,
            tx_watermarked.amount,
            tx_watermarked.currency,
            tx_watermarked.merchant_id,
            tx_watermarked.merchant_name,
            tx_watermarked.merchant_category,
            tx_watermarked.transaction_type,
            tx_watermarked.payment_channel,
            tx_watermarked.device_id,
            tx_watermarked.city.alias("transaction_city"),
            tx_watermarked.country.alias("transaction_country"),
            tx_watermarked.transaction_timestamp,
            tx_watermarked.is_international,
            tx_watermarked.fraud_score,
            tx_watermarked.fraud_reason,
            tx_watermarked.status.alias("transaction_status"),

            # Fraud watchlist match details
            wl_watermarked.watchlist_id,
            wl_watermarked.watch_type,
            wl_watermarked.risk_level,
            wl_watermarked.action,
            wl_watermarked.reason_code,
            wl_watermarked.reason_description,
            wl_watermarked.effective_from.alias("watchlist_effective_from"),
            wl_watermarked.reported_by,
            wl_watermarked.reported_source,
            wl_watermarked.city.alias("watchlist_city"),
            wl_watermarked.country.alias("watchlist_country")
        )
    )

    return fraud_detected
