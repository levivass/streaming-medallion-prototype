# Databricks notebook source
# MAGIC %md
# MAGIC #Silver réteg megvalósítása

# COMMAND ----------

from pyspark.sql.functions import col, to_timestamp, from_json, when
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType

# COMMAND ----------

# MAGIC %run ./common_config

# COMMAND ----------

velocity_schema = StructType([
    StructField("num_transactions", IntegerType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("unique_merchants", IntegerType(), True),
    StructField("unique_countries", IntegerType(), True),
    StructField("max_single_amount", DoubleType(), True),
])

# COMMAND ----------

df_bronze = spark.readStream.table(bronze_table_name)

df_silver_transformed = (
    df_bronze
    .withColumnRenamed("timestamp", "transaction_timestamp")
    .withColumn("transaction_timestamp", col("transaction_timestamp").cast("timestamp"))
    .withColumnRenamed("distance_from_home", "is_foreign_transaction")
    .withColumn("is_foreign_transaction", col("is_foreign_transaction").cast("boolean"))
    .withColumn("velocity_struct", from_json(col("velocity_last_hour"), velocity_schema))
)

df_silver_flattened = \
    df_silver_transformed \
    .selectExpr("*", "velocity_struct.*") \
    .drop("velocity_last_hour", "velocity_struct")

# COMMAND ----------

def route_to_quarantine(df_batch, batch_id):
    df_evaluated = df_batch.withColumn("failure_reason",
        when(col("_rescued_data").isNotNull(), "Schema error (rescued)")
        .when(col("transaction_timestamp").isNull(), "Missing timestamp")
        .when(col("amount") < 0, "Negative amount")
        .otherwise("Valid")
    )

    df_valid = df_evaluated.filter(col("failure_reason") == "Valid").drop("failure_reason")

    (df_valid.write
        .format("delta")
        .mode("append")
        .saveAsTable(silver_table_name))
    
    df_quarantine = df_evaluated.filter(col("failure_reason") != "Valid")

    (df_quarantine.write
        .format("delta")
        .mode("append")
        .saveAsTable(quarantine_table_name))


# COMMAND ----------

streaming_query_silver = (
    df_silver_flattened
    .writeStream
    .foreachBatch(route_to_quarantine)
    .option("checkpointLocation", checkpoint_path_silver)
    .trigger(availableNow=True)
    .start()
).awaitTermination()