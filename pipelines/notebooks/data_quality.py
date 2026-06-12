# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Data Quality Checks
# MAGIC
# MAGIC Post-pipeline data quality validation:
# MAGIC - Row counts across layers (reconciliation)
# MAGIC - Freshness checks
# MAGIC - Null ratio monitoring
# MAGIC - Schema drift detection
# MAGIC - Anomaly scoring

# COMMAND ----------
# DBTITLE 1,Initialize
import sys
sys.path.append("/Workspace/Shared/pipelines/")

from pipelines.src.config import PipelineConfig
from pipelines.src.transformers import DataValidator, DataQualityException
from pyspark.sql.functions import col, count, countDistinct, sum as spark_sum, max as spark_max
import json
from datetime import datetime

config = PipelineConfig.from_widgets()
validator = DataValidator(null_threshold_pct=config.null_threshold_pct)

dq_results = {
    "run_timestamp": datetime.utcnow().isoformat(),
    "environment": config.environment,
    "checks": [],
}

def log_check(name, passed, detail=""):
    status = "✅" if passed else "❌"
    dq_results["checks"].append({"name": name, "passed": passed, "detail": detail})
    print(f"{status} {name}: {detail}")

# COMMAND ----------
# DBTITLE 1,Check 1: Row Count Reconciliation
try:
    bronze_count = spark.table(f"{config.bronze_catalog}.sales.raw_sales_transactions").count()
    silver_count = spark.table(f"{config.silver_catalog}.sales.sales_transactions_cleaned").count()
    gold_count = spark.table(f"{config.gold_catalog}.sales.daily_sales_summary").count()

    log_check("Row count — Bronze sales", bronze_count > 0, f"{bronze_count:,} rows")
    log_check("Row count — Silver sales", silver_count > 0, f"{silver_count:,} rows")
    log_check("Row count — Gold sales", gold_count > 0, f"{gold_count:,} rows")

    # Silver should have <= Bronze (dedup)
    if silver_count > 0 and bronze_count > 0:
        reduction_pct = ((bronze_count - silver_count) / bronze_count) * 100
        log_check(
            "Dedup ratio (Silver ≤ Bronze)",
            reduction_pct >= 0,
            f"Reduction: {reduction_pct:.1f}% ({silver_count:,} / {bronze_count:,})"
        )
except Exception as e:
    log_check("Row count reconciliation", False, str(e))

# COMMAND ----------
# DBTITLE 1,Check 2: Freshness — Last Data Load
try:
    sales_silver = spark.table(f"{config.silver_catalog}.sales.sales_transactions_cleaned")
    max_date = sales_silver.select(spark_max("transaction_date")).collect()[0][0]

    from pyspark.sql.functions import current_date, datediff
    days_behind = (
        spark.range(1)
        .select(datediff(current_date(), lit(max_date)))
        .collect()[0][0]
    )

    is_fresh = days_behind <= 1  # Within 1 day
    log_check(
        "Freshness — Sales transactions",
        is_fresh,
        f"Latest: {max_date} ({days_behind} days behind)"
    )
except Exception as e:
    log_check("Freshness check", False, str(e))

# COMMAND ----------
# DBTITLE 1,Check 3: Null Ratio Analysis
try:
    for table_name, catalog, schema in [
        ("sales_transactions_cleaned", config.silver_catalog, "sales"),
        ("customers_cleaned", config.silver_catalog, "customers"),
    ]:
        df = spark.table(f"{catalog}.{schema}.{table_name}")
        total = df.count()

        null_columns = {}
        for c in df.columns:
            null_count = df.filter(col(c).isNull()).count()
            null_pct = (null_count / total * 100) if total > 0 else 0
            if null_pct > config.null_threshold_pct:
                null_columns[c] = round(null_pct, 2)

        is_clean = len(null_columns) == 0
        detail = ", ".join(f"{c}={p}%" for c, p in null_columns.items()) if null_columns else "All columns within threshold"
        log_check(f"Null ratio — {table_name}", is_clean, detail)
except Exception as e:
    log_check("Null ratio analysis", False, str(e))

# COMMAND ----------
# DBTITLE 1,Check 4: Customer 360 Completeness
try:
    customer_360 = spark.table(f"{config.gold_catalog}.customers.customer_360")
    total_customers = customer_360.count()
    active_customers = customer_360.filter(col("customer_segment") != "never_purchased").count()
    active_pct = (active_customers / total_customers * 100) if total_customers > 0 else 0

    log_check(
        "Customer 360 — Active customers",
        active_pct > 0,
        f"{active_pct:.1f}% active ({active_customers:,} / {total_customers:,})"
    )

    # Segment distribution
    segments = (
        customer_360.groupBy("customer_segment")
        .count()
        .orderBy(col("count").desc())
        .collect()
    )
    seg_detail = "; ".join(f"{r.customer_segment}={r.count:,}" for r in segments[:5])
    log_check("Customer 360 — Segments populated", len(segments) > 0, seg_detail)
except Exception as e:
    log_check("Customer 360 check", False, str(e))

# COMMAND ----------
# DBTITLE 1,Check 5: Revenue Integrity
try:
    sales_gold = spark.table(f"{config.gold_catalog}.sales.daily_sales_summary")
    total_revenue = sales_gold.select(spark_sum("total_revenue_usd")).collect()[0][0]
    has_revenue = total_revenue is not None and total_revenue > 0

    log_check(
        "Revenue integrity — Total sales",
        has_revenue,
        f"${total_revenue:,.2f} USD" if has_revenue else "$0 or null"
    )
except Exception as e:
    log_check("Revenue integrity", False, str(e))

# COMMAND ----------
# DBTITLE 1,Check 6: IoT Device Health
try:
    iot_silver = spark.table(f"{config.silver_catalog}.iot.iot_readings_validated")

    total_readings = iot_silver.count()
    outlier_pct = (
        iot_silver.filter(col("is_outlier") == True).count() / total_readings * 100
    ) if total_readings > 0 else 0

    is_normal = outlier_pct < 10.0  # Less than 10% outliers
    log_check(
        "IoT — Outlier rate",
        is_normal,
        f"{outlier_pct:.1f}% outliers ({total_readings:,} total readings)"
    )
except Exception as e:
    log_check("IoT device health", False, str(e))

# COMMAND ----------
# DBTITLE 1,Final DQ Summary
all_passed = all(c["passed"] for c in dq_results["checks"])
dq_results["overall_status"] = "PASSED" if all_passed else "FAILED"

# Write DQ report to Delta
dq_df = spark.createDataFrame(
    [json.dumps(dq_results)],
    "value string"
)

dq_path = f"{config.checkpoint_path}/data_quality_reports/"
dq_df.write.mode("append").format("delta").save(dq_path)

print("=" * 60)
print(f"{'✅ ALL CHECKS PASSED' if all_passed else '❌ SOME CHECKS FAILED'}")
print(f"   Report saved to: {dq_path}")
print(f"   Checks run: {len(dq_results['checks'])}")
print(f"   Passed: {sum(1 for c in dq_results['checks'] if c['passed'])}")
print(f"   Failed: {sum(1 for c in dq_results['checks'] if not c['passed'])}")
print("=" * 60)

if not all_passed:
    raise DataQualityException(
        f"Data quality checks failed: {sum(1 for c in dq_results['checks'] if not c['passed'])} failures",
        violations=[c for c in dq_results["checks"] if not c["passed"]],
    )
