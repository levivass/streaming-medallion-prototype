# Databricks notebook source
# MAGIC %md
# MAGIC #Gold réteg megvalósítása

# COMMAND ----------

from pyspark.sql.functions import count

# COMMAND ----------

# MAGIC %run ./common_config

# COMMAND ----------

df_quarantine = spark.readStream.table(quarantine_table_name)

df_gold_agg = (
    df_quarantine
    .groupBy("channel", "failure_reason")
    .agg(count("*").alias("dropped_record_count"))
)

# COMMAND ----------

query_gold = (
    df_gold_agg
    .writeStream
    .format("delta")
    .outputMode("complete")
    .option("checkpointLocation", checkpoint_path_gold)
    .trigger(availableNow=True)
    .toTable(gold_table_name)
).awaitTermination()