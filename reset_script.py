# Databricks notebook source
dbutils.fs.rm("/Volumes/workspace/default/szakdoga/checkpoints/", recurse=True)
dbutils.fs.rm("/Volumes/workspace/default/szakdoga/staging_chunks/", recurse=True)
dbutils.fs.rm("/Volumes/workspace/default/szakdoga/stream_input/", recurse=True)

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS bronze_transactions;
# MAGIC DROP TABLE IF EXISTS silver_transactions;
# MAGIC DROP TABLE IF EXISTS quarantine_transactions;
# MAGIC DROP TABLE IF EXISTS gold_transactions;
# MAGIC DROP TABLE IF EXISTS gold_transactions_2;