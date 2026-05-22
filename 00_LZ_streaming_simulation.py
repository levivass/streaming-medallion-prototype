# Databricks notebook source
# MAGIC %md
# MAGIC #Streaming szimuláló script

# COMMAND ----------

import time
import os
import shutil
import random
from pyspark.sql.functions import col, when, rand, lit

# COMMAND ----------

# MAGIC %run ./common_config

# COMMAND ----------

BATCH_SIZE = 75000

# COMMAND ----------

dbutils.fs.rm(staging_dir, recurse=True)
dbutils.fs.rm(landing_zone_dir, recurse=True)
dbutils.fs.mkdirs(staging_dir)
dbutils.fs.mkdirs(landing_zone_dir)

# COMMAND ----------

df_master = spark.read.csv(source_file, header=True)
print(f"Successfully read input file \"{source_file}\"")

# COMMAND ----------

print("Generating random values...")
df_with_rand = df_master.withColumn("rand_val", rand(42))

# COMMAND ----------

print("Beginning artificial data corruption...")
df_corrupted = (
    df_with_rand
    .withColumn("amount",
                when(col("rand_val") < 0.005, lit("-1.0"))
                .when((col("rand_val") >= 0.005) & (col("rand_val") < 0.01), lit("INVALID_DATA"))
                .otherwise(col("amount"))
    )
    .withColumn("timestamp",
                when((col("rand_val") >= 0.01) & (col("rand_val") < 0.015), lit(None))
                .otherwise(col("timestamp"))
    )
    .drop("rand_val")
)
print("Data corruption complete.")

# COMMAND ----------

print("Beginning input file repartitioning...")
df_corrupted.write.option("maxRecordsPerFile", BATCH_SIZE).csv(staging_dir, header=True, mode="overwrite")
print(f"Successfully wrote partitioned input to \"{staging_dir}\"")

# COMMAND ----------

print("Beginning streaming simulation...")
chunk_files = [f for f in os.listdir(staging_dir) if f.endswith(".csv")]
print(f"Starting to land {len(chunk_files)} files...")

for idx, filename in enumerate(chunk_files):
    source_path = os.path.join(staging_dir, filename)
    target_path = os.path.join(landing_zone_dir, filename)

    shutil.move(source_path, target_path)
    print(f"Landed file {idx+1}/{len(chunk_files)}: \"{filename}\"")

    #time.sleep(random.randint(5, 15))

print("Streaming simulation complete.")