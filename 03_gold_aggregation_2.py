# Databricks notebook source
# MAGIC %md
# MAGIC #Gold réteg megvalósítása

# COMMAND ----------

from pyspark.sql.functions import count, sum, round

# COMMAND ----------

# MAGIC %run ./common_config

# COMMAND ----------

df_silver = spark.readStream.table(silver_table_name)

df_gold_agg_2 = (
    df_silver
    .groupBy("device", "transaction_hour")
    .agg(
        count("*").alias("transaction_count"),
        round(sum("amount")).alias("total_amount")
    )
)

# COMMAND ----------

query_gold = (
    df_gold_agg_2
    .writeStream
    .format("delta")
    .outputMode("complete")
    .option("checkpointLocation", checkpoint_path_gold_2)
    .trigger(availableNow=True)
    .toTable(gold_table_name_2)
).awaitTermination()