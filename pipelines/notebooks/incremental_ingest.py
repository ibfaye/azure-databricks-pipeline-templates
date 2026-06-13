# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Incremental Data Ingestion
# MAGIC
# MAGIC Lightweight hourly notebook for near-real-time ingestion.
# MAGIC Uses Auto Loader with `availableNow` trigger to process
# MAGIC only new files since the last run.
# MAGIC
# MAGIC **Schedule**: Every hour via `incremental_load.yml`

# COMMAND ----------
# DBTITLE 1,Initialize
import sys
sys.path.append("/Workspace/Shared/pipelines/")

from pipelines.src.config import PipelineConfig
from pipelines.src.writers import DeltaWriter
from pyspark.sql.functions import current_timestamp, input_file_name

config = PipelineConfig.from_widgets()

# Read source and table from widgets (with defaults)
try:
    source_name = dbutils.widgets.get("source")
except Exception:
    source_name = "web_events"

try:
    target_table = dbutils.widgets.get("table")
except Exception:
    target_table = "raw_web_events"

try:
    trigger_mode = dbutils.widgets.get("trigger")
except Exception:
    trigger_mode = "availableNow"

writer = DeltaWriter(config)

print(f"✅ Incremental load initialized")
print(f"   Source: {source_name}")
print(f"   Target: {target_table}")
print(f"   Trigger: {trigger_mode}")

# COMMAND ----------
# DBTITLE 1,Auto Loader — Incremental Read
source_path = f"{config.landing_path}{source_name}/"
checkpoint = f"{config.checkpoint_path}{source_name}/"

df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.schemaLocation", f"{checkpoint}schema")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .load(source_path)
    .withColumn("_ingested_at", current_timestamp())
    .withColumn("_source_file", input_file_name())
)

# COMMAND ----------
# DBTITLE 1,Write to Bronze
target_path = f"{config.bronze_path}{target_table}"

stream = (
    df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{checkpoint}write")
    .trigger(availableNow=True)
    .start(target_path)
)

stream.awaitTermination()

# COMMAND ----------
# DBTITLE 1,Summary
print("=" * 60)
print(f"🏆 Incremental load complete — {source_name} → {target_table}")
print(f"   Source: {source_path}")
print(f"   Target: {target_path}")
print("=" * 60)
