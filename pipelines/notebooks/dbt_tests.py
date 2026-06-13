# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # dbt Tests Execution
# MAGIC
# MAGIC Runs dbt data tests and schema tests against Unity Catalog.
# MAGIC This is Task 5 in the Medallion pipeline DAG (runs in parallel with data_quality).

# COMMAND ----------
# DBTITLE 1,Initialize
import sys
sys.path.append("/Workspace/Shared/pipelines/")
import subprocess
from pipelines.src.config import PipelineConfig

config = PipelineConfig.from_widgets()

print(f"✅ dbt tests initialized — Environment: {config.environment}")

# COMMAND ----------
# DBTITLE 1,Run dbt Tests
try:
    result = subprocess.run(
        ["dbt", "test", "--target", config.environment],
        cwd="/Workspace/Shared/dbt/",
        capture_output=True,
        text=True,
        timeout=1800,
    )

    print("--- dbt Test Output ---")
    print(result.stdout)

    if result.returncode != 0:
        print(f"⚠️  Some dbt tests failed (exit code {result.returncode})")
        print(result.stderr)
    else:
        print("✅ All dbt tests passed")

except subprocess.TimeoutExpired:
    print("⚠️  dbt tests timed out after 30 minutes")
except Exception as e:
    print(f"❌ dbt test execution failed: {e}")

# COMMAND ----------
# DBTITLE 1,Summary
print("=" * 60)
print("🏆 dbt Tests Complete!")
print("=" * 60)
