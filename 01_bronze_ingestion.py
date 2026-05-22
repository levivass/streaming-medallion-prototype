# Databricks notebook source
# MAGIC %md
# MAGIC #Bronze réteg megvalósítása

# COMMAND ----------

# DBTITLE 1,Cell 2
from pyspark.sql.functions import current_timestamp, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType, IntegerType

# COMMAND ----------

# MAGIC %run ./common_config

# COMMAND ----------

bronze_schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("card_number", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("merchant_category", StringType(), True),
    StructField("merchant_type", StringType(), True),
    StructField("merchant", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("currency", StringType(), True),
    StructField("country", StringType(), True),
    StructField("city", StringType(), True),
    StructField("city_size", StringType(), True),
    StructField("card_type", StringType(), True),
    StructField("card_present", BooleanType(), True),
    StructField("device", StringType(), True),
    StructField("channel", StringType(), True),
    StructField("device_fingerprint", StringType(), True),
    StructField("ip_address", StringType(), True),
    StructField("distance_from_home", IntegerType(), True),
    StructField("high_risk_merchant", BooleanType(), True),
    StructField("transaction_hour", StringType(), True),
    StructField("weekend_transaction", BooleanType(), True),
    StructField("velocity_last_hour", StringType(), True),
    StructField("is_fraud", BooleanType(), True)
])

# COMMAND ----------

print("Starting ingestion from Landing Zone...")

raw_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header", "true")
    .option("cloudFiles.rescuedDataColumn", "_rescued_data")
    .schema(bronze_schema)
    .load(landing_zone_dir)
)

# COMMAND ----------

# DBTITLE 1,Cell 5
print("Enriching raw stream with metadata...")

bronze_with_metadata = (
    raw_stream
    .withColumn("ingestion_timestamp", current_timestamp())
    .withColumn("_source_file", col("_metadata.file_name"))
    .withColumn("_source_file_size", col("_metadata.file_size"))
    .withColumn("_source_file_modification_time", col("_metadata.file_modification_time"))
)

# COMMAND ----------

print("Starting enriched data stream writing to table...")

streaming_query = (
    bronze_with_metadata
    .writeStream
    .format("delta")
    .option("checkpointLocation", checkpoint_path_bronze)
    .trigger(availableNow=True)
    .outputMode("append")
    .table(bronze_table_name)
).awaitTermination()