# Databricks notebook source
# MAGIC %md
# MAGIC #Job orchestration implementation

# COMMAND ----------

import time
from databricks.sdk import WorkspaceClient
from IPython.display import clear_output

# COMMAND ----------

# MAGIC %run ./common_config

# COMMAND ----------

JOB_ID = "373728011333882"
w = WorkspaceClient()
i = 0
previous_bronze_count = 0

# COMMAND ----------

print("Waiting for data to arrive into Landing Zone...")
while True:
    try:
        files = dbutils.fs.ls(landing_zone_dir)
        if len(files) > 0:
            print("Data detected. Starting pipeline...")
            break
    except:
        pass
    time.sleep(2)

# COMMAND ----------

while True:
    print(f"Running iteration {i+1}...")
    print(f"Starting job with job ID: {JOB_ID}")
    run_response = w.jobs.run_now(job_id=JOB_ID)
    print(f"Started job with run ID: {run_response.run_id}")
    state = run_response.result().state.result_state.value
    print(f"Job finished with state {state}.\n")

    clear_output(wait=True)

    bronze_count = spark.sql("SELECT COUNT(*) FROM " + bronze_table_name).collect()[0][0]
    silver_count = spark.sql("SELECT COUNT(*) FROM " + silver_table_name).collect()[0][0]
    quarantine_count = spark.sql("SELECT COUNT(*) FROM " + quarantine_table_name).collect()[0][0]

    quarantine_percentage = round(quarantine_count / bronze_count * 100, 2) if bronze_count > 0 else 0

    print(f"Bronze table has {bronze_count} records.")
    print(f"Silver table has {silver_count} records.")
    print(f"Quarantine table has {quarantine_count} records. ({quarantine_percentage}% of bronze)\n")

    df_gold = spark.sql("SELECT * FROM " + gold_table_name + " ORDER BY dropped_record_count DESC")
    print("Gold table:")
    display(df_gold)
    
    df_gold_2 = spark.sql("SELECT * FROM " + gold_table_name_2 + " ORDER BY transaction_count DESC")
    print("Gold_2 table:")
    display(df_gold_2)

    print(f"\nIteration #{i+1} finished.")
    print("-" * 42)

    if bronze_count == previous_bronze_count and bronze_count > 0:
        print("\nNo new data detected. Exiting...")
        break
    else:
        previous_bronze_count = bronze_count
    
    i += 1

print("Simulation finished.")