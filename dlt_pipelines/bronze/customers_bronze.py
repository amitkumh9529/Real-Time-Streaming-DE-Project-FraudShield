"""Bronze Layer Ingestion for Customer Master & CDC Feed.

Ingests raw customer profile records and Change Data Capture (CDC) events
originating from PostgreSQL OLTP into the Bronze Delta Lake table.
"""

from __future__ import annotations

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame


@dp.table(
    name="fraudshield.bronze.customers",
    comment="Raw customer profile events and CDC updates ingested from PostgreSQL / Storage Volumes"
)
def customers_bronze() -> DataFrame:
    """Ingest raw customer records using Auto Loader for incremental file drops.

    In production Databricks environments, this reads from the Unity Catalog
    Volume where Lakeflow Connect or Debezium CDC drops incremental batch records.
    """
    source_volume = "/Volumes/fraudshield/source/customers/source_data/"

    # Read incremental CDC / batch drops via cloudFiles
    streaming_df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaLocation", "/Volumes/fraudshield/source/customers/schema/")
        .load(source_volume)
    )

    parsed_df = streaming_df.select(
        F.col("customer_id"),
        F.col("first_name"),
        F.col("last_name"),
        F.col("gender"),
        F.col("age"),
        F.col("city"),
        F.col("state"),
        F.col("country"),
        F.col("annual_income"),
        F.col("customer_segment"),
        F.col("account_open_date"),
        F.col("risk_score"),
        F.col("preferred_spending_min"),
        F.col("preferred_spending_max"),
        F.col("preferred_city"),
        F.col("preferred_country"),
        F.col("trusted_device_id"),
        F.col("card_number"),
        F.col("card_type"),
        F.col("email"),
        F.col("transaction_limit"),
        F.col("update_timestamp"),
        # CDC metadata (default to 'U' if not provided)
        F.coalesce(F.col("op_type"), F.lit("U")).alias("cdc_operation"),
        F.col("_metadata.file_path").alias("source_file"),
        F.current_timestamp().alias("ingestion_timestamp")
    )

    return parsed_df
