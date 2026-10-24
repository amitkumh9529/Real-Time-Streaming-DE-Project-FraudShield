# Databricks notebook source
# Smoke test validating Confluent Cloud Kafka connectivity and Spark Structured Streaming.
# Credentials are retrieved securely from 'fraudshield-scope' Databricks Secret Scope.

# COMMAND ----------

import json

kafka_connection_json=dbutils.secrets.get(scope="fraudshield-scope",key="kafka_connection_details")
kafka_config=json.loads(kafka_connection_json)
bootstrap_servers=kafka_config['bootstrap_servers']
api_key=kafka_config['api_key']
api_secret=kafka_config['api_secret']
topic=kafka_config['topic']

# COMMAND ----------

jaas_config=f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="{api_key}" password="{api_secret}";'

# COMMAND ----------

sample_batch=(spark.read.format("kafka")
              .option("kafka.bootstrap.servers",bootstrap_servers)
              .option("subscribe",topic)
            .option("kafka.security.protocol", "SASL_SSL")
        .option("kafka.sasl.mechanism", "PLAIN")
        .option("kafka.sasl.jaas.config", jaas_config)
        .option("startingOffsets","earliest")
        .load()
)

# COMMAND ----------

sample_batch.count()

# COMMAND ----------

display(sample_batch)

# COMMAND ----------

from pyspark.sql.functions import col

parsed_batch=sample_batch.select(
col("key").cast("string"),
col("value").cast("string"),
col("topic"),
col("partition"),
col("offset"),
col("timestamp"),
col("timestampType")
)

# COMMAND ----------

display(parsed_batch)

# COMMAND ----------

parsed_batch.write.saveAsTable("fraudshield.bronze.transactions_batch_test")

# COMMAND ----------

streaming_df=(spark.readStream.format("kafka")
              .option("kafka.bootstrap.servers",bootstrap_servers)
              .option("subscribe",topic)
            .option("kafka.security.protocol", "SASL_SSL")
        .option("kafka.sasl.mechanism", "PLAIN")
        .option("kafka.sasl.jaas.config", jaas_config)
        .option("startingOffsets","earliest")
        .load()
)

# COMMAND ----------

from pyspark.sql.functions import col

parsed_streaming_df=streaming_df.select(
col("key").cast("string"),
col("value").cast("string"),
col("topic"),
col("partition"),
col("offset"),
col("timestamp"),
col("timestampType")
)

# COMMAND ----------

streaming_query=(parsed_streaming_df.writeStream.format("delta")
.outputMode("Append")
.option("checkpointLocation", "/Volumes/fraudshield/source/transactions/checkpoint/")
.trigger(availableNow=True)
.toTable("fraudshield.bronze.transactions_streaming_test")
)
print("Query Started with query id: ",streaming_query.id)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from fraudshield.bronze.transactions_streaming_test

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from fraudshield.silver.transactions

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from fraudshield.gold.transaciton_count_by_minute_sliding_window
