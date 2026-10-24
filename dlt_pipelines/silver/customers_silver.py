"""Silver Layer: Customer Master Dimension (SCD Type 1 Conformed View).

Applies data quality expectations, tokenizes card numbers (PCI-DSS),
and maintains the current conformed customer profile for stream enrichment.
"""

from __future__ import annotations

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame


@dp.table(
    name="fraudshield.silver.customers",
    comment="Cleaned, conformed customer dimension (SCD Type 1) with tokenized card numbers"
)
@dp.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
@dp.expect("valid_age", "age >= 18 AND age <= 100")
@dp.expect("valid_transaction_limit", "transaction_limit > 0")
def customers_silver() -> DataFrame:
    """Read bronze customer stream, tokenize PII, and maintain latest profile per customer."""
    bronze_df = spark.readStream.table("fraudshield.bronze.customers")

    # Clean, cast, and secure fields
    transformed_df = bronze_df.select(
        F.col("customer_id"),
        F.col("first_name"),
        F.col("last_name"),
        F.col("gender"),
        F.col("age").cast("integer"),
        F.col("city"),
        F.col("state"),
        F.col("country"),
        F.col("annual_income").cast("double"),
        F.col("customer_segment"),
        F.to_date(F.col("account_open_date"), "yyyy-MM-dd").alias("account_open_date"),
        F.col("risk_score").cast("integer"),
        F.col("preferred_spending_min").cast("double"),
        F.col("preferred_spending_max").cast("double"),
        F.col("preferred_city"),
        F.col("preferred_country"),
        F.col("trusted_device_id"),
        # PCI-DSS: Tokenize card number with SHA-256 and isolate last 4 digits
        F.concat(F.lit("tok_"), F.substring(F.sha2(F.col("card_number"), 256), 1, 24)).alias("card_token"),
        F.concat(F.lit("****"), F.substring(F.col("card_number"), -4, 4)).alias("card_last_four"),
        F.col("card_type"),
        F.col("email"),
        F.col("transaction_limit").cast("double"),
        F.to_timestamp(F.col("update_timestamp")).alias("profile_updated_at"),
        F.current_timestamp().alias("silver_ingestion_timestamp")
    )

    return transformed_df
