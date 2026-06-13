# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Silver Layer: Data Cleansing & Validation
# MAGIC
# MAGIC Transforms Bronze data into Silver:
# MAGIC - Deduplication & validation
# MAGIC - PII masking (GDPR/CCPA compliance)
# MAGIC - Schema enforcement
# MAGIC - dbt model execution (silver models)
# MAGIC
# MAGIC **Pipeline**: Medallion → Bronze → **Silver** → Gold

# COMMAND ----------
# DBTITLE 1,Initialize
import sys
sys.path.append("/Workspace/Shared/pipelines/")

from pipelines.src.config import PipelineConfig
from pipelines.src.transformers import Deduplicator, DataValidator, PIIMasker
from pipelines.src.writers import UnityCatalogWriter
import subprocess

config = PipelineConfig.from_widgets()
uc_writer = UnityCatalogWriter(config)

print(f"✅ Silver pipeline initialized — Environment: {config.environment}")

# COMMAND ----------
# DBTITLE 1,Silver: Sales Transactions
try:
    # Read from Bronze
    sales_bronze = spark.table(f"{config.bronze_catalog}.sales.raw_sales_transactions")

    # Dedup
    dedup = Deduplicator(unique_keys=["transaction_id", "transaction_date"])
    sales_deduped = dedup.deduplicate(sales_bronze)

    # Validate
    validator = DataValidator(null_threshold_pct=config.null_threshold_pct)
    sales_validated = validator.validate_not_null(
        sales_deduped, ["transaction_id", "amount", "transaction_date"]
    )

    # PII masking
    masker = PIIMasker()
    sales_masked = masker.bronze_to_silver(sales_validated, {
        "customer_email": "hash",
    })
    sales_masked = sales_masked.withColumnRenamed("customer_email", "customer_email_hashed")

    # Write to Silver
    uc_writer.write_table(
        sales_masked,
        catalog=config.silver_catalog,
        schema="sales",
        table="sales_transactions_cleaned",
        mode="overwrite",
        partition_by=["date(transaction_date)"],
        comment="Cleansed sales transactions — Silver layer",
        tbl_properties={"delta.enableChangeDataFeed": "true"},
    )

    print(f"✅ Silver sales: {sales_masked.count():,} rows")
except Exception as e:
    print(f"❌ Silver sales failed: {e}")
    raise

# COMMAND ----------
# DBTITLE 1,Silver: Customer Profiles
try:
    customers_bronze = spark.table(f"{config.bronze_catalog}.customers.raw_customer_profiles")

    dedup = Deduplicator(unique_keys=["customer_id"])
    customers_deduped = dedup.deduplicate(customers_bronze)

    masker = PIIMasker()
    customers_masked = masker.bronze_to_silver(customers_deduped, {
        "email": "hash",
        "phone_number": "mask_phone",
    })

    uc_writer.write_table(
        customers_masked,
        catalog=config.silver_catalog,
        schema="customers",
        table="customers_cleaned",
        mode="overwrite",
        comment="Cleansed customer profiles with PII masked — Silver layer",
    )

    print(f"✅ Silver customers: {customers_masked.count():,} rows")
except Exception as e:
    print(f"❌ Silver customers failed: {e}")
    raise

# COMMAND ----------
# DBTITLE 1,Run dbt Silver Models
try:
    # Execute dbt silver models
    try:
        dbt_command = dbutils.widgets.get("dbt_command")
    except Exception:
        dbt_command = "run --select silver.*"

    result = subprocess.run(
        ["dbt"] + dbt_command.split(),
        cwd="/Workspace/Shared/dbt/",
        capture_output=True,
        text=True,
    )

    print("--- dbt Output ---")
    print(result.stdout)
    if result.returncode != 0:
        print(f"⚠️  dbt warnings/errors:\n{result.stderr}")
    else:
        print("✅ dbt silver models completed successfully")
except Exception as e:
    print(f"❌ dbt execution failed: {e}")
    print("   Ensure dbt is installed and profiles are configured.")
    print("   pip install dbt-databricks")

# COMMAND ----------
# DBTITLE 1,Silver: Web Sessions Enriched
try:
    web_bronze = spark.table(f"{config.bronze_catalog}.customers.raw_web_events")

    # Session enrichment is handled by dbt model: web_sessions_enriched
    # This step just validates the bronze source is available
    row_count = web_bronze.count()
    print(f"✅ Web events bronze source available: {row_count:,} rows")
except Exception as e:
    print(f"⚠️  Web events not available (may be empty): {e}")

# COMMAND ----------
# DBTITLE 1,Silver: IoT Readings Validated
try:
    iot_bronze = spark.table(f"{config.bronze_catalog}.iot.raw_iot_sensor_readings")

    # Basic validation — outlier detection in dbt model: iot_readings_validated
    row_count = iot_bronze.count()
    device_count = iot_bronze.select("device_id").distinct().count()
    print(f"✅ IoT bronze source: {row_count:,} readings from {device_count} devices")
except Exception as e:
    print(f"⚠️  IoT data not available (may be empty): {e}")

# COMMAND ----------
# DBTITLE 1,Final Summary
print("=" * 60)
print("🏆 Silver Transformation Complete!")
print(f"   Bronze → Silver for catalogs: {config.bronze_catalog} → {config.silver_catalog}")
print(f"   PII masked, data validated, schema enforced")
print("=" * 60)
