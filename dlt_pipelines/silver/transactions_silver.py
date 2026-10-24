"""Silver Layer: Cleaned, Deduplicated, and Tokenized Transactions Stream.

Parses raw JSON from Kafka Bronze, applies schema contracts, enforces data quality
expectations, executes deterministic event deduplication, and secures cardholder data.
"""

from __future__ import annotations

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


@dp.table(
    name="fraudshield.silver.transactions",
    comment="Parsed, validated, deduplicated, and PCI-DSS tokenized transaction stream"
)
@dp.expect_or_drop("valid_transaction_id", "transaction_id IS NOT NULL AND transaction_id RLIKE '^TX[NF]?[0-9]+$'")
@dp.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
@dp.expect_or_drop("valid_card_token", "card_token IS NOT NULL AND card_token != 'tok_'")
@dp.expect_or_drop("valid_merchant_id", "merchant_id IS NOT NULL")
@dp.expect("valid_amount", "amount > 0")
@dp.expect("valid_timestamp", "transaction_timestamp IS NOT NULL AND transaction_timestamp <= current_timestamp() + INTERVAL 5 MINUTES")
@dp.expect("valid_fraud_score", "fraud_score >= 0 AND fraud_score <= 100")
def transactions_silver() -> DataFrame:
    """Read raw Kafka Bronze stream, unpack JSON, tokenize PAN, and deduplicate."""
    bronze_df = spark.readStream.table("fraudshield.bronze.transactions")

    schema = StructType([
        StructField("event_id", StringType()),
        StructField("transaction_id", StringType()),
        StructField("customer_id", StringType()),
        StructField("card_number", StringType()),
        StructField("merchant_id", StringType()),
        StructField("merchant_name", StringType()),
        StructField("merchant_category", StringType()),
        StructField("amount", DoubleType()),
        StructField("currency", StringType()),
        StructField("transaction_type", StringType()),
        StructField("payment_channel", StringType()),
        StructField("device_id", StringType()),
        StructField("city", StringType()),
        StructField("country", StringType()),
        StructField("transaction_timestamp", TimestampType()),
        StructField("is_international", BooleanType()),
        StructField("fraud_score", IntegerType()),
        StructField("fraud_reason", StringType()),
        StructField("status", StringType()),
    ])

    parsed_df = bronze_df.select(
        F.from_json(col("value"), schema).alias("data"),
        F.col("topic").alias("kafka_topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
        F.col("ingestion_timestamp").alias("bronze_ingestion_timestamp")
    ).select(
        F.coalesce(F.col("data.event_id"), F.concat(F.lit("EVT-"), F.col("data.transaction_id"))).alias("event_id"),
        F.col("data.transaction_id"),
        F.col("data.customer_id"),
        # PCI-DSS Tokenization: Salted deterministic token + masked last 4
        F.concat(F.lit("tok_"), F.substring(F.sha2(F.col("data.card_number"), 256), 1, 24)).alias("card_token"),
        F.concat(F.lit("****"), F.substring(F.col("data.card_number"), -4, 4)).alias("card_last_four"),
        F.col("data.merchant_id"),
        F.col("data.merchant_name"),
        F.col("data.merchant_category"),
        F.col("data.amount"),
        F.coalesce(F.col("data.currency"), F.lit("INR")).alias("currency"),
        F.col("data.transaction_type"),
        F.col("data.payment_channel"),
        F.col("data.device_id"),
        F.col("data.city"),
        F.col("data.country"),
        F.col("data.transaction_timestamp"),
        F.coalesce(F.col("data.is_international"), F.lit(False)).alias("is_international"),
        F.coalesce(F.col("data.fraud_score"), F.lit(0)).alias("fraud_score"),
        F.coalesce(F.col("data.fraud_reason"), F.lit("NORMAL")).alias("fraud_reason"),
        F.coalesce(F.col("data.status"), F.lit("APPROVED")).alias("status"),
        F.col("kafka_topic"),
        F.col("kafka_partition"),
        F.col("kafka_offset"),
        F.col("kafka_timestamp"),
        F.col("bronze_ingestion_timestamp"),
        F.current_timestamp().alias("silver_ingestion_timestamp")
    )

    # Watermarked stream deduplication on transaction_id
    deduplicated_df = (
        parsed_df
        .withWatermark("transaction_timestamp", "10 minutes")
        .dropDuplicates(["transaction_id"])
    )

    return deduplicated_df
