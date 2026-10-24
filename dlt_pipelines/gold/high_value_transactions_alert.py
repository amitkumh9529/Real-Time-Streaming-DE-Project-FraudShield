"""Gold Layer: High-Value Transaction Alert Stream.

Performs a stream-to-static enrichment join between transactions and conformed
customer profiles to detect transactions violating individual customer spending limits.
"""

from __future__ import annotations

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame


@dp.table(
    name="fraudshield.gold.high_value_transactions_alert",
    comment="Real-time alerts for transactions exceeding customer-configured transaction limits"
)
def high_value_transactions_alert() -> DataFrame:
    """Stream transactions and filter on amount exceeding active customer limits."""
    transactions = spark.readStream.table("fraudshield.silver.transactions")
    customers = spark.read.table("fraudshield.silver.customers")

    joined_df = (
        transactions.join(
            customers,
            transactions.customer_id == customers.customer_id,
            "left"
        )
        .filter(F.col("amount") > F.coalesce(customers.transaction_limit, F.lit(100000.0)))
        .select(
            F.concat_ws("-", F.lit("ALERT-HV"), transactions.transaction_id).alias("alert_id"),
            F.lit("HIGH_VALUE_TRANSACTION").alias("alert_type"),
            F.current_timestamp().alias("alert_timestamp"),

            transactions.event_id,
            transactions.transaction_id,
            transactions.customer_id,
            customers.email.alias("customer_email"),
            F.concat_ws(" ", customers.first_name, customers.last_name).alias("customer_name"),
            transactions.card_token,
            transactions.card_last_four,
            transactions.amount.alias("transaction_amount"),
            customers.transaction_limit,
            transactions.currency,
            transactions.merchant_name,
            transactions.merchant_category,
            transactions.transaction_type,
            transactions.payment_channel,
            transactions.city,
            transactions.country,
            transactions.is_international,
            transactions.fraud_score,
            transactions.fraud_reason,
            transactions.transaction_timestamp,
            transactions.status
        )
    )

    return joined_df
