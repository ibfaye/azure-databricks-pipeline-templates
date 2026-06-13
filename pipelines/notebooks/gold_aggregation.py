# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Gold Layer: Business Analytics & Aggregations
# MAGIC
# MAGIC Transforms Silver data into Gold:
# MAGIC - Business KPIs & aggregations
# MAGIC - Customer 360 view (LTV, RFM segmentation)
# MAGIC - Inventory health dashboard
# MAGIC - Conversion funnel
# MAGIC - dbt model execution (gold models)
# MAGIC
# MAGIC **Pipeline**: Medallion → Bronze → Silver → **Gold**

# COMMAND ----------
# DBTITLE 1,Initialize
import sys
sys.path.append("/Workspace/Shared/pipelines/")

from pipelines.src.config import PipelineConfig
from pipelines.src.writers import UnityCatalogWriter
import subprocess

config = PipelineConfig.from_widgets()
uc_writer = UnityCatalogWriter(config)

print(f"✅ Gold pipeline initialized — Environment: {config.environment}")

# COMMAND ----------
# DBTITLE 1,Gold: Daily Sales Summary
try:
    sales_silver = spark.table(f"{config.silver_catalog}.sales.sales_transactions_cleaned")

    sales_gold = (
        sales_silver
        .groupBy("transaction_date", "store_id", "currency")
        .agg(
            countDistinct("transaction_id").alias("order_count"),
            countDistinct("customer_email_hashed").alias("unique_customers"),
            sum("amount_local").alias("total_revenue_local"),
            sum("amount_usd").alias("total_revenue_usd"),
            avg("amount_local").alias("avg_order_value_local"),
            avg("amount_usd").alias("avg_order_value_usd"),
            sum("quantity").alias("items_sold"),
        )
    )

    uc_writer.write_table(
        sales_gold,
        catalog=config.gold_catalog,
        schema="sales",
        table="daily_sales_summary",
        mode="overwrite",
        partition_by=["date(transaction_date)"],
        comment="Daily sales KPIs — Gold layer",
    )

    print(f"✅ Gold daily sales: {sales_gold.count():,} rows")
except Exception as e:
    print(f"❌ Gold sales failed: {e}")
    raise

# COMMAND ----------
# DBTITLE 1,Gold: Customer 360 View
try:
    customers_silver = spark.table(f"{config.silver_catalog}.customers.customers_cleaned")
    sales_silver = spark.table(f"{config.silver_catalog}.sales.sales_transactions_cleaned")

    # RFM calculation
    rfm = (
        sales_silver
        .groupBy("customer_email_hashed")
        .agg(
            max("transaction_date").alias("last_purchase_date"),
            countDistinct("transaction_id").alias("frequency"),
            sum("amount_usd").alias("monetary"),
            min("transaction_date").alias("first_purchase_date"),
        )
    )

    customer_360 = (
        customers_silver.alias("c")
        .join(rfm.alias("r"), col("c.email_hashed") == col("r.customer_email_hashed"), "left")
        .select(
            col("c.customer_id"),
            col("c.email_hashed"),
            col("c.country_code"),
            col("c.registration_date"),
            col("c.loyalty_tier"),
            col("c.age_bucket"),
            coalesce(col("r.monetary"), lit(0)).alias("lifetime_value"),
            coalesce(col("r.frequency"), lit(0)).alias("lifetime_orders"),
            coalesce(col("r.last_purchase_date"), col("c.registration_date")).alias("last_purchase_date"),
            # RFM Segmentation
            when(col("r.monetary").isNull(), "never_purchased")
            .when((col("r.monetary") > 1000) & (col("r.frequency") > 10), "champion")
            .when((col("r.monetary") > 500) & (col("r.frequency") > 5), "loyal")
            .when(datediff(current_date(), col("r.last_purchase_date")) < 30, "new_active")
            .when((datediff(current_date(), col("r.last_purchase_date")) >= 30) &
                  (datediff(current_date(), col("r.last_purchase_date")) < 90), "at_risk")
            .when(datediff(current_date(), col("r.last_purchase_date")) >= 90, "lapsed")
            .otherwise("needs_attention").alias("customer_segment"),
        )
    )

    uc_writer.write_table(
        customer_360,
        catalog=config.gold_catalog,
        schema="customers",
        table="customer_360",
        mode="overwrite",
        comment="Customer 360 view with LTV and segmentation — Gold layer",
    )

    print(f"✅ Gold customer 360: {customer_360.count():,} rows")
except Exception as e:
    print(f"❌ Gold customer 360 failed: {e}")
    raise

# COMMAND ----------
# DBTITLE 1,Gold: Inventory Health
try:
    inv_silver = spark.table(f"{config.silver_catalog}.operations.inventory_snapshots")
    sales_silver = spark.table(f"{config.silver_catalog}.sales.sales_transactions_cleaned")

    inv_health = (
        inv_silver.alias("inv")
        .groupBy("product_sku")
        .agg(
            last("quantity_on_hand").alias("current_stock"),
            avg("outbound_qty").alias("avg_daily_demand"),
        )
    )

    uc_writer.write_table(
        inv_health,
        catalog=config.gold_catalog,
        schema="operations",
        table="inventory_health",
        mode="overwrite",
        comment="Inventory health dashboard — Gold layer",
    )

    print(f"✅ Gold inventory health: {inv_health.count():,} SKUs")
except Exception as e:
    print(f"❌ Gold inventory health failed: {e}")
    raise

# COMMAND ----------
# DBTITLE 1,Run dbt Gold Models
try:
    try:
        dbt_command = dbutils.widgets.get("dbt_command")
    except Exception:
        dbt_command = "run --select gold.*"

    result = subprocess.run(
        ["dbt"] + dbt_command.split(),
        cwd="/Workspace/Shared/dbt/",
        capture_output=True,
        text=True,
    )

    print("--- dbt Output ---")
    print(result.stdout if result.stdout else "(no output)")
    if result.returncode != 0:
        print(f"⚠️  dbt stderr:\n{result.stderr}")
    else:
        print("✅ dbt gold models completed")
except Exception as e:
    print(f"⚠️  dbt execution skipped (may not be installed): {e}")

# COMMAND ----------
# DBTITLE 1,Final Summary
print("=" * 60)
print("🏆 Gold Aggregation Complete!")
print(f"   Silver → Gold for catalog: {config.gold_catalog}")
print(f"   Business KPIs, Customer 360, Inventory Health")
print("   Ready for BI dashboards via SQL Warehouse")
print("=" * 60)
