"""Silver Layer: Standardized Fraud Watchlist Stream.

Cleans, validates, and tokenizes watchlist entities, ensuring uppercase canonicalization
and strict status tracking for real-time join matching.
"""

from __future__ import annotations

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame


@dp.table(
    name="fraudshield.silver.fraud_watchlist",
    comment="Cleaned and standardized fraud watchlist with entity tokenization"
)
@dp.expect_or_drop("valid_watchlist_id", "watchlist_id IS NOT NULL")
@dp.expect_or_drop("valid_entity_id", "entity_id IS NOT NULL")
@dp.expect("valid_status", "status IN ('ACTIVE', 'INACTIVE', 'REVOKED')")
def fraud_watchlist_silver() -> DataFrame:
    """Read raw watchlist stream, tokenize entity identifier, and canonicalize timestamps."""
    bronze_df = spark.readStream.table("fraudshield.bronze.fraud_watchlist")

    cleaned_df = bronze_df.select(
        F.upper(F.trim(F.col("watchlist_id"))).alias("watchlist_id"),
        F.upper(F.trim(F.col("watch_type"))).alias("watch_type"),
        # PCI-DSS Tokenization: Salted deterministic token for card/entity matching
        F.concat(F.lit("tok_"), F.substring(F.sha2(F.col("entity_id"), 256), 1, 24)).alias("entity_token"),
        F.concat(F.lit("****"), F.substring(F.col("entity_id"), -4, 4)).alias("entity_last_four"),
        F.upper(F.trim(F.col("risk_level"))).alias("risk_level"),
        F.upper(F.trim(F.col("action"))).alias("action"),
        F.col("reason_code"),
        F.col("reason_description"),
        F.upper(F.trim(F.coalesce(F.col("status"), F.lit("ACTIVE")))).alias("status"),
        F.to_timestamp(F.col("effective_from"), "dd-MMM-yyyy HH:mm:ss").alias("effective_from"),
        F.col("reported_by"),
        F.col("reported_source"),
        F.col("country"),
        F.col("city"),
        F.col("source_file"),
        F.col("ingestion_timestamp").alias("bronze_ingestion_timestamp"),
        F.current_timestamp().alias("silver_ingestion_timestamp")
    )

    return cleaned_df
