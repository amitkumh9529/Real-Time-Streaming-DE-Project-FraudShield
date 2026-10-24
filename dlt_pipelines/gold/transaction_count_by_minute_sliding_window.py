from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame


@dp.table(
name="fraudshield.gold.transaction_count_by_minute_sliding_window"
,comment="Aggregated transaction count over 5-minute sliding windows with 1-minute slide"
)
def transaction_count_by_minute_sliding_window() -> DataFrame:

    transactions=spark.readStream.table("fraudshield.silver.transactions")

    transactions_with_watermark=transactions.withWatermark("transaction_timestamp", "5 minutes")

    transaction_count_df=transactions_with_watermark.groupBy(
        F.window("transaction_timestamp","5 minutes","1 minute")
    ).agg(

    F.count("*").alias("transaction_count")
    ).select(
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        F.col("transaction_count")
    )
    return transaction_count_df
