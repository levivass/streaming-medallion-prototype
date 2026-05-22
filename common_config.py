# Databricks notebook source
source_file = "/Volumes/workspace/default/szakdoga/synthetic_fraud_data.csv"
staging_dir = "/Volumes/workspace/default/szakdoga/staging_chunks/"
landing_zone_dir = "/Volumes/workspace/default/szakdoga/stream_input/"

bronze_table_name = "workspace.default.bronze_transactions"
silver_table_name = "workspace.default.silver_transactions"
quarantine_table_name = "workspace.default.quarantine_transactions"
gold_table_name = "workspace.default.gold_transactions"
gold_table_name_2 = "workspace.default.gold_transactions_2"

checkpoint_path_bronze = "/Volumes/workspace/default/szakdoga/checkpoints/bronze/"
checkpoint_path_silver = "/Volumes/workspace/default/szakdoga/checkpoints/silver/"
checkpoint_path_gold = "/Volumes/workspace/default/szakdoga/checkpoints/gold/"
checkpoint_path_gold_2 = "/Volumes/workspace/default/szakdoga/checkpoints/gold_2/"