"""Gold Layer: Durable Alert Outbox Delta Table.

Implements the Transactional Outbox Pattern for fraud notifications. Alerts are
appended as durable records into this table by the streaming pipeline, ensuring
decoupling from external notification APIs and zero risk of pipeline backpressure.
"""

from __future__ import annotations

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame


@dp.table(
    name="fraudshield.gold.alert_outbox",
    comment="Durable transactional outbox table for reliable, idempotent alert delivery"
)
def alert_outbox() -> DataFrame:
    """Consolidate fraud watchlist matches and high-value alerts into a durable outbox."""
    fraud_alerts = spark.readStream.table("fraudshield.gold.fraud_card_alert")
    hv_alerts = spark.readStream.table("fraudshield.gold.high_value_transactions_alert")

    # Standardize fraud alerts into outbox format
    fraud_outbox = fraud_alerts.select(
        F.col("alert_id"),
        F.col("alert_type"),
        F.col("customer_id"),
        F.col("customer_name"),
        F.coalesce(F.col("customer_email"), F.lit("alerts-triage@example.com")).alias("recipient_email"),
        F.to_json(F.struct(
            F.col("alert_id"),
            F.col("alert_type"),
            F.col("transaction_id"),
            F.col("card_last_four"),
            F.col("amount"),
            F.col("currency"),
            F.col("merchant_name"),
            F.col("merchant_category"),
            F.col("transaction_city"),
            F.col("transaction_country"),
            F.col("transaction_timestamp"),
            F.col("fraud_score"),
            F.col("fraud_reason"),
            F.col("watchlist_id"),
            F.col("risk_level"),
            F.col("action"),
            F.col("reason_description")
        )).alias("payload_json"),
        F.lit("PENDING").alias("delivery_status"),
        F.lit(0).alias("retry_count"),
        F.lit(3).alias("max_retries"),
        F.current_timestamp().alias("created_timestamp"),
        F.cast(F.lit(None), "timestamp").alias("last_attempt_timestamp"),
        F.cast(F.lit(None), "timestamp").alias("delivered_timestamp"),
        F.cast(F.lit(None), "string").alias("failure_reason")
    )

    # Standardize high-value alerts into outbox format
    hv_outbox = hv_alerts.select(
        F.col("alert_id"),
        F.col("alert_type"),
        F.col("customer_id"),
        F.col("customer_name"),
        F.coalesce(F.col("customer_email"), F.lit("alerts-triage@example.com")).alias("recipient_email"),
        F.to_json(F.struct(
            F.col("alert_id"),
            F.col("alert_type"),
            F.col("transaction_id"),
            F.col("card_last_four"),
            F.col("transaction_amount").alias("amount"),
            F.col("transaction_limit"),
            F.col("currency"),
            F.col("merchant_name"),
            F.col("merchant_category"),
            F.col("city").alias("transaction_city"),
            F.col("country").alias("transaction_country"),
            F.col("transaction_timestamp"),
            F.col("fraud_score"),
            F.col("fraud_reason")
        )).alias("payload_json"),
        F.lit("PENDING").alias("delivery_status"),
        F.lit(0).alias("retry_count"),
        F.lit(3).alias("max_retries"),
        F.current_timestamp().alias("created_timestamp"),
        F.cast(F.lit(None), "timestamp").alias("last_attempt_timestamp"),
        F.cast(F.lit(None), "timestamp").alias("delivered_timestamp"),
        F.cast(F.lit(None), "string").alias("failure_reason")
    )

    return fraud_outbox.unionByName(hv_outbox)
