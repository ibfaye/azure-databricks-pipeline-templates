# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Bronze Layer: Raw Data Ingestion
# MAGIC
# MAGIC This notebook ingests raw data from the landing zone (ADLS Gen2)
# MAGIC into Unity Catalog's Bronze catalog with audit columns.
# MAGIC
# MAGIC **Pipeline**: Medallion → Bronze → Silver → Gold
# MAGIC
# MAGIC **Sources**: CSV, Parquet, JSON, streaming (Auto Loader)

# COMMAND ----------
# DBTITLE 1,Initialize Configuration
import sys
sys.path.append("/Workspace/Shared/pipelines/")

from pipelines.src.config import PipelineConfig
from pipelines.src.readers import DataLakeReader
from pipelines.src.writers import DeltaWriter, UnityCatalogWriter
from pipelines.src.transformers import Deduplicator, DataValidator
from pyspark.sql.functions import lit, current_timestamp

# Load config from notebook widgets
config = PipelineConfig.from_widgets()

reader = DataLakeReader(config)
writer = DeltaWriter(config)
uc_writer = UnityCatalogWriter(config)

print(f"✅ Pipeline initialized — Environment: {config.environment}")
print(f"   Bronze path:  {config.bronze_path}")
print(f"   Landing path: {config.landing_path}")

# COMMAND ----------
# DBTITLE 1,Ingest Sales Transactions
try:
    # Read from landing zone
    sales_raw = reader.read_parquet(f"{config.landing_path}sales_transactions/")
    sales_raw = sales_raw.withColumn("_source_name", lit("sales_transactions")).withColumn("_ingested_at", current_timestamp())

    # Deduplicate
    dedup = Deduplicator(unique_keys=["transaction_id", "transaction_date"])
    sales_deduped = dedup.deduplicate(sales_raw)

    # Validate
    validator = DataValidator(null_threshold_pct=5.0)
    sales_validated = validator.validate_not_null(
        sales_deduped, ["transaction_id", "amount", "transaction_date"]
    )

    # Write to Bronze Unity Catalog
    uc_writer.write_table(
        sales_validated,
        catalog=config.bronze_catalog,
        schema="sales",
        table="raw_sales_transactions",
        mode="append",
        partition_by=["date(transaction_date)"],
        comment="Raw sales transactions — Bronze layer (Unity Catalog)",
    )

    print(f"✅ Sales transactions ingested: {sales_validated.count():,} rows")
except Exception as e:
    print(f"❌ Sales transactions failed: {e}")
    raise

# COMMAND ----------
# DBTITLE 1,Ingest Customer Profiles
try:
    customers_raw = reader.read_csv(
        f"{config.landing_path}customer_profiles/",
    )
    customers_raw = customers_raw.withColumn("_source_name", lit("customer_profiles"))

    dedup = Deduplicator(unique_keys=["customer_id"])
    customers_deduped = dedup.deduplicate(customers_raw)

    uc_writer.write_table(
        customers_deduped,
        catalog=config.bronze_catalog,
        schema="customers",
        table="raw_customer_profiles",
        mode="overwrite",  # Full refresh for master data
        comment="Raw customer profiles — Bronze layer (Unity Catalog)",
    )

    print(f"✅ Customer profiles ingested: {customers_deduped.count():,} rows")
except Exception as e:
    print(f"❌ Customer profiles failed: {e}")
    raise

# COMMAND ----------
# DBTITLE 1,Ingest Inventory Movements
try:
    inv_raw = reader.read_parquet(f"{config.landing_path}inventory_movements/")
    inv_raw = inv_raw.withColumn("_source_name", lit("inventory_movements"))

    dedup = Deduplicator(unique_keys=["movement_id"])
    inv_deduped = dedup.deduplicate(inv_raw)

    uc_writer.write_table(
        inv_deduped,
        catalog=config.bronze_catalog,
        schema="operations",
        table="raw_inventory_movements",
        mode="append",
        partition_by=["date(movement_date)"],
        comment="Raw inventory movements — Bronze layer (Unity Catalog)",
    )

    print(f"✅ Inventory movements ingested: {inv_deduped.count():,} rows")
except Exception as e:
    print(f"❌ Inventory movements failed: {e}")
    raise

# COMMAND ----------
# DBTITLE 1,Ingest Web Events (Incremental)
try:
    web_raw = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{config.checkpoint_path}web_events/schema")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(f"{config.landing_path}web_events/")
        .withColumn("_source_name", lit("web_events"))
    )

    # Write stream to bronze
    web_stream = (
        web_raw.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{config.checkpoint_path}web_events/checkpoint")
        .trigger(availableNow=True)
        .start(f"{config.bronze_path}customers/raw_web_events")
    )

    web_stream.awaitTermination()
    print("✅ Web events stream completed")
except Exception as e:
    print(f"❌ Web events failed: {e}")
    raise

# COMMAND ----------
# DBTITLE 1,Ingest IoT Sensor Readings (Streaming)
try:
    iot_raw = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{config.checkpoint_path}iot/schema")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(f"{config.landing_path}iot/")
        .withColumn("_source_name", lit("iot_readings"))
    )

    iot_stream = (
        iot_raw.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{config.checkpoint_path}iot/checkpoint")
        .trigger(availableNow=True)
        .start(f"{config.bronze_path}iot/raw_iot_sensor_readings")
    )

    iot_stream.awaitTermination()
    print("✅ IoT readings stream completed")
except Exception as e:
    print(f"❌ IoT readings failed: {e}")
    raise

# COMMAND ----------
# DBTITLE 1,Final Summary
print("=" * 60)
print("🏆 Bronze Ingestion Complete!")
print(f"   Bronze catalog: {config.bronze_catalog}")
print(f"   Storage: {config.storage_account}")
print("=" * 60)
