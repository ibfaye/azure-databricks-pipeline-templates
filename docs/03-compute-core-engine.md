# Module 3: Compute & Core Engine

> **The runtime layer.** You will understand how the distributed compute engine that powers every notebook in this repository actually works — from Apache Spark's driver-executor architecture through Databricks cluster topology to PySpark's lazy evaluation model. This module bridges the gap between "I can run the notebooks" and "I understand what's happening under the hood."

---

## 1. Learning Objectives

| # | Conceptual | Practical |
|---|-----------|-----------|
| 1 | Master Apache Spark's distributed architecture: Driver nodes, Executor JVMs, Task scheduling, shuffle mechanics, and the DAG scheduler | Read a Spark UI DAG visualization from a repo pipeline run and trace data lineage from `read_parquet()` through `groupBy()` to `write_table()` |
| 2 | Understand Databricks cluster topologies: All-Purpose vs. Job clusters, Single-User vs. Shared access modes, Auto-scaling triggers, and Photon acceleration | Deploy the repo's three cluster definitions (all-purpose, jobs, spot) and measure cost/performance tradeoffs for each pipeline stage |
| 3 | Internalize PySpark's lazy evaluation: Transformations (narrow vs. wide), Actions (triggers of computation), the Catalyst Optimizer, and Tungsten execution engine | Write an optimized PySpark transformation that avoids UDFs, uses native functions, and demonstrates `explain()` output showing Catalyst's physical plan |
| 4 | Select the correct Databricks Runtime (DBR) for a given workload: Standard vs. ML vs. Photon, LTS vs. Beta channels, and Scala version implications | Justify the repo's choice of `14.3.x-scala2.12` and identify when you'd deviate (e.g., Photon for SQL-heavy Gold aggregations) |
| 5 | Design cost-optimal cluster configurations: Spot vs. on-demand, auto-scaling thresholds, driver sizing, and instance type selection | Configure a job cluster JSON from the repo's `medallion_pipeline.yml` that balances cost and SLA for a 30-minute SLA pipeline |

---

## 2. Theoretical Foundations

### 2.1 Apache Spark Architecture: The Stage-Level Mental Model

Every notebook in the repo runs on a Spark cluster. Understanding the architecture is essential for debugging "why is this taking so long?" questions.

**The repo's cluster as defined in `medallion_pipeline.yml`:**
```yaml
job_clusters:
  - job_cluster_key: "etl_cluster"
    new_cluster:
      spark_version: "14.3.x-scala2.12"
      node_type_id: "Standard_DS3_v2"
      data_security_mode: "SINGLE_USER"
      autoscale:
        min_workers: 2
        max_workers: 10
```

This seemingly simple YAML block defines a cluster with 1 Driver + 2–10 Workers. Here's what happens when `bronze_ingestion.py` runs:

```
┌─────────────────────────────────────────────────────────────────┐
│ Databricks Control Plane                                         │
│  ┌──────────┐                                                    │
│  │  Driver   │  Standard_DS3_v2 (4 vCPU, 14 GiB RAM)             │
│  │  Node     │  - Parses Python notebook                         │
│  │           │  - Catalyst optimizer: logical → physical plan    │
│  │           │  - DAG Scheduler: splits plan into Stages         │
│  │           │  - Task Scheduler: assigns Tasks to Executors     │
│  │           │  - Aggregates results                             │
│  └─────┬─────┘                                                    │
│        │ Spark Context (network shuffle)                          │
│  ┌─────┴──────────────────────────────────────────────────┐     │
│  │                      Executor JVMs                      │     │
│  │  ┌──────────┐  ┌──────────┐  ┌──────────┐             │     │
│  │  │ Worker 1  │  │ Worker 2  │  │ Worker 3  │  ... N    │     │
│  │  │ 4 vCPU    │  │ 4 vCPU    │  │ 4 vCPU    │             │     │
│  │  │ 14 GiB RAM │  │ 14 GiB RAM │  │ 14 GiB RAM │             │     │
│  │  │           │  │           │  │           │             │     │
│  │  │ Task 0    │  │ Task 3    │  │ Task 6    │             │     │
│  │  │ Task 1    │  │ Task 4    │  │ Task 7    │             │     │
│  │  │ Task 2    │  │ Task 5    │  │ Task 8    │             │     │
│  │  │ (parquet) │  │ (parquet) │  │ (parquet) │             │     │
│  │  └──────────┘  └──────────┘  └──────────┘             │     │
│  └────────────────────────────────────────────────────────┘     │
├─────────────────────────────────────────────────────────────────┤
│ ADLS Gen2 (abfss://)                                             │
│  ├── bronze/sales/raw_sales_transactions/                        │
│  │   ├── part-00000.parquet  ← Worker 1 reads this               │
│  │   ├── part-00001.parquet  ← Worker 1 reads this               │
│  │   ├── part-00002.parquet  ← Worker 2 reads this               │
│  │   └── ...                                                     │
└─────────────────────────────────────────────────────────────────┘
```

**The critical equation for parallelism:**
```
Partitions (input) = Number of Parquet files × Row groups per file (typically 1 per 128MB)
Tasks per stage = Number of partitions
Parallelism = min(Tasks, vCPUs available across all Executors)

Example with repo defaults:
  - 2 workers × 4 vCPUs = 8 concurrent tasks
  - If your Bronze table has 200 partitions, Spark processes them in 200/8 = 25 rounds
  - Adding 8 workers (32 vCPUs) cuts this to 200/32 ≈ 7 rounds
```

### 2.2 Narrow vs. Wide Transformations: The Shuffle Boundary

This is the single most important concept for Spark performance. The repo's notebooks are full of both types:

**Narrow transformations** (no shuffle — data stays on the same executor):

```python
# From pipelines/src/transformers.py — ALL of these are narrow
df.withColumn("_source_name", lit("sales_transactions"))     # Narrow: 1 input → 1 output partition
df.withColumn("_ingested_at", current_timestamp())            # Narrow: no data movement
df.filter(col("_row_num") == 1)                               # Narrow: rows stay local
df.drop("_row_num")                                           # Narrow: column removal
```

**Wide transformations** (shuffle — data crosses executor boundaries):

```python
# From pipelines/src/transformers.py — window functions CAUSE SHUFFLE
window = Window.partitionBy(*self.unique_keys).orderBy(col(self.order_by).desc())
df.withColumn("_row_num", row_number().over(window))

# From gold_aggregation.py — groupBy is a shuffle
sales_gold = (
    sales_silver
    .groupBy("transaction_date", "store_id", "currency")  # ← SHUFFLE
    .agg(
        countDistinct("transaction_id").alias("order_count"),
        sum("amount_local").alias("total_revenue_local"),
    )
)
```

**The cost of a shuffle:**
1. Every executor writes its partial results to disk (shuffle write)
2. Data is re-partitioned by the grouping key across the network
3. Every executor reads the data it's now responsible for (shuffle read)
4. In the Spark UI, this shows as an **Exchange** operator between stages

**The repo's defense against shuffle costs:**
```python
spark_conf = {
    "spark.sql.adaptive.enabled": "true",              # AQE optimizes shuffle partitions at runtime
    "spark.sql.adaptive.coalescePartitions.enabled": "true",  # Reduces partitions post-shuffle if data is small
    "spark.sql.shuffle.partitions": "auto",            # Let AQE decide, not hardcoded 200
}
```

### 2.3 The DAG and Catalyst Optimizer: How PySpark Plans Your Code

When you write `sales_silver.groupBy("date").agg(sum("amount"))`, Spark doesn't execute it immediately. It builds a **logical plan**, optimizes it through Catalyst, then generates a **physical plan**:

```python
# You can see this plan yourself:
sales_silver = spark.table("bronze.sales.raw_sales_transactions")
explain_output = (
    sales_silver
    .filter(col("transaction_date") >= "2025-01-01")
    .groupBy("store_id")
    .agg(sum("amount").alias("total"))
    ._jdf.queryExecution().toString()
)
print(explain_output)
```

**What Catalyst does (simplified):**

1. **Parsing:** Converts your DataFrame API calls into an unresolved logical plan
2. **Analysis:** Resolves table names, column references against the catalog
3. **Logical Optimization:** Push predicate filters as close to the source as possible (predicate pushdown), combine adjacent filters, eliminate redundant projections
4. **Physical Planning:** Decides join strategies (broadcast hash join vs. sort-merge join), chooses shuffle partition counts, selects scan strategies (parquet with column pruning and predicate pushdown)

**A concrete example from the repo:**

```python
# What you write (in silver_transformation.py):
sales_bronze = spark.table(f"{config.bronze_catalog}.sales.raw_sales_transactions")
dedup = Deduplicator(unique_keys=["transaction_id", "transaction_date"])
sales_deduped = dedup.deduplicate(sales_bronze)
sales_deduped.write.mode("overwrite").saveAsTable("silver.sales.sales_transactions_cleaned")
```

```
What Catalyst produces:

== Physical Plan ==
WriteIntoDataSourceCommand
  +- ColumnarToRow
     +- Project [transaction_id, store_id, ..., _row_num]
        +- Filter (_row_num#123 = 1)                    ← Pushed close to scan
           +- Window [row_number() OVER (PARTITION BY transaction_id, transaction_date ORDER BY _ingested_at DESC)]
              +- Sort [transaction_id, transaction_date, _ingested_at DESC]  ← Required for window
                 +- Exchange hashpartitioning(transaction_id, transaction_date)  ← SHUFFLE
                    +- FileScan parquet [transaction_id, store_id, ..., _ingested_at]  ← Column pruning
                       PartitionFilters: [date(transaction_date) is not null]
                       PushedFilters: []  ← No predicate pushdown (reading all)
```

Notice: Catalyst **automatically** prunes columns (only reads the columns you reference) and pushes partition filters to the scan. You don't need to optimize this — but you MUST understand the shuffle at the `Exchange` line, because that's where you'll see performance bottlenecks.

### 2.4 Databricks Cluster Topologies

The repo defines three cluster types in `terraform/modules/databricks-cluster/main.tf`:

**1. All-Purpose Cluster:**
```hcl
resource "databricks_cluster" "all_purpose" {
  cluster_name            = "${var.cluster_name}-ap"
  data_security_mode      = "SINGLE_USER"
  single_user_name        = data.databricks_current_user.main.user_name
  autotermination_minutes = 30
}
```
- **Use:** Interactive development, ad-hoc analysis, notebook exploration
- **Persistence:** Stays running until idle timeout (30 min)
- **Best for:** Module 3–5 learning exercises, debugging pipeline logic
- **NOT for:** Production pipelines (costly to keep running)

**2. Job Cluster:**
```hcl
resource "databricks_cluster" "jobs" {
  cluster_name            = "${var.cluster_name}-jobs"
  data_security_mode      = "SINGLE_USER"
  autotermination_minutes = 30
}
```
- **Use:** Automated pipeline execution
- **Lifecycle:** Spun up when the job starts, terminated when it finishes
- **Cost model:** Pay only for the duration of pipeline execution
- **The repo's `medallion_pipeline.yml` uses this pattern via `job_clusters`**

**3. Spot Cluster (cost-optimized):**
```hcl
resource "databricks_cluster" "spot" {
  count = var.spot_bid_max_price != null ? 1 : 0
  azure_attributes {
    availability       = "SPOT_AZURE"
    spot_bid_max_price = var.spot_bid_max_price  # e.g., 80 (80% of on-demand price)
  }
}
```
- **Use:** Non-latency-sensitive batch workloads where a node eviction is acceptable
- **Risk:** Azure can reclaim spot VMs with 30 seconds notice
- **Mitigation:** The repo's pipeline is idempotent (deduplication in Bronze, overwrite in Silver), so a spot eviction during a run just means re-running the notebook
- **Savings:** Typically 60–80% cheaper than on-demand

**The access mode decision — `SINGLE_USER` vs `SHARED`:**

The repo uses `SINGLE_USER` exclusively. This is intentional:
- `SINGLE_USER`: One identity (the service principal) runs all commands. Full Spark capabilities (R, Scala, Python UDFs, DBFS mounts).
- `SHARED` (Unity Catalog): Multiple users share the cluster. Restricted to Python and SQL. No UDFs, no file system access.
- `USER_ISOLATION`: Not used in this repo — relevant for multi-tenant scenarios where users should not see each others' data.

### 2.5 Databricks Runtime (DBR) Selection

The repo pins `14.3.x-scala2.12`. Here's the decision framework:

| DBR Variant | Spark Version | Best For | When to Use |
|-------------|--------------|----------|-------------|
| **Standard** | 3.5.0 | General ETL | The repo default. Balanced performance/cost. |
| **Photon** | 3.5.0 (Photon engine) | SQL-heavy workloads, aggregations | Gold layer `daily_sales_summary` would benefit. 2–8× faster on `GROUP BY` + `SUM()`. |
| **ML** | 3.5.0 + ML libs | Machine learning training | Not used in this repo. Adds GPU drivers, MLflow, pre-installed ML libraries. |
| **LTS** | Varies | Production stability | `14.3.x` IS an LTS version (Long Term Support — supported for 3 years). |

**When to upgrade from `14.3.x`:**
- **Photon for Gold:** If Gold aggregation takes > 5 minutes, switching to Photon runtime with the same cluster size typically cuts time in half
- **Liquid Clustering (DBR 13.3+):** Replace manual Z-Ordering with `CLUSTER BY` for automatic optimization
- **Predictive Optimization (account-level feature, enabled by default on new workspaces):** Databricks automatically runs `OPTIMIZE`, `ANALYZE`, and `VACUUM` on Unity Catalog managed tables using serverless compute — reduces the need for explicit pipeline optimization steps

### 2.6 PySpark vs. UDFs: The Performance Cliff

The repo systematically avoids Python UDFs (User Defined Functions). This is a deliberate performance choice:

**❌ What the repo does NOT do:**
```python
# Python UDF — serializes every row to Python, processes it, serializes back to JVM
from pyspark.sql.functions import udf
@udf
def mask_email_udf(email):
    if email is None:
        return None
    local, domain = email.split("@")
    return local[:2] + "***@" + domain[0] + "***" + domain[domain.index("."):]

# This would be 10-100× slower than the native approach below
```

**✅ What the repo DOES (native Spark SQL functions):**
```python
# From pipelines/src/transformers.py — all native functions, zero Python overhead
@staticmethod
def mask_email(df: DataFrame, column: str) -> DataFrame:
    return df.withColumn(
        column,
        regexp_replace(
            col(column),
            r'^(.{2})([^@]*)(@)([^.]+)(.*)$',
            r'$1***$3$4***$5'
        )
    )
```

**The performance difference:**
- **Native functions:** Execute entirely within the JVM. Spark's Tungsten engine compiles them to Java bytecode. No serialization.
- **Python UDFs:** Each row is pickled → sent to a Python process → unpickled → processed → repickled → sent back to JVM → unpickled. This is **per-row overhead** that dwarfs the actual computation.

**When UDFs are unavoidable (and what to do):**
- Complex NLP preprocessing (use Spark NLP library instead)
- Custom cryptographic operations (use Pandas UDFs / Vectorized UDFs — they batch rows)
- Legacy business logic in Python (wrap in a Pandas UDF with `@pandas_udf`)

---

## 3. Hands-on Execution

### 3.1 Deploy All Three Cluster Types

```bash
cd terraform

# Enable spot cluster by adding to terraform.tfvars:
echo 'spot_bid_max_price = 80' >> terraform.tfvars

# Apply
terraform apply

# Verify all three clusters exist
databricks clusters list --output json | jq '.[].cluster_name'
# Expected: dbx-pipeline-dev-ap, dbx-pipeline-dev-jobs, dbx-pipeline-dev-spot
```

### 3.2 Analyze a Pipeline Run's Spark UI

After running `bronze_ingestion.py`:

1. Open the Databricks workspace
2. Go to **Compute** → click the running cluster
3. Click the **Spark UI** tab (opens the Spark master UI)
4. Navigate to the **SQL/DataFrame** tab for the completed job
5. Find the `groupBy` aggregation and inspect:

```
Stage 2: Exchange (Shuffle)
├── Shuffle Write: 125.3 MB
├── Shuffle Read:  125.3 MB  
├── Tasks: 16 completed
├── Duration: 45 seconds
└── Spill (Memory): 0 B  ← No spill = good; if > 0, increase executor memory

Stage 3: Aggregate
├── Tasks: 8 completed
├── Duration: 12 seconds
└── Input: 125.3 MB (shuffle read)
```

**Key metrics to watch:**
- **Shuffle Spill:** If > 0, data is being written to disk during shuffle → increase `spark.sql.shuffle.partitions` or add more executor memory
- **Task Skew:** If one task takes 5× longer than others → partition skew → add salt to the join key or increase partition count
- **GC Time:** If > 10% of task time → increase executor memory or reduce `spark.memory.fraction`

### 3.3 Write and Compare Optimized vs. Unoptimized Code

```python
# Cell 1: Unoptimized — Python UDF for deduplication
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType
import hashlib

@udf(returnType=StringType())
def slow_hash(col_val):
    if col_val is None:
        return None
    return hashlib.sha256(col_val.encode()).hexdigest()

df = spark.table("bronze.sales.raw_sales_transactions")

# Time the UDF approach
import time
start = time.time()
df_slow = df.withColumn("hashed_slow", slow_hash(col("customer_email")))
df_slow.count()  # Force evaluation
slow_time = time.time() - start
print(f"❌ Python UDF: {slow_time:.2f}s")
```

```python
# Cell 2: Optimized — Native Spark function
from pyspark.sql.functions import sha2, concat_ws, lit

start = time.time()
df_fast = df.withColumn(
    "hashed_fast",
    sha2(concat_ws("|", lit("salt"), col("customer_email")), 256)
)
df_fast.count()
fast_time = time.time() - start
print(f"✅ Native function: {fast_time:.2f}s")
print(f"   Speedup: {slow_time/fast_time:.1f}×")
```

### 3.4 Inspect the Execution Plan

```python
# Print the full execution plan for the Deduplicator
from pipelines.src.transformers import Deduplicator
from pipelines.src.config import PipelineConfig

config = PipelineConfig(environment="dev", storage_account="stdevdatabricksdl")
df = spark.table("bronze.sales.raw_sales_transactions")

dedup = Deduplicator(unique_keys=["transaction_id", "transaction_date"])
result = dedup.deduplicate(df)

# Show the logical and physical plans
print("=" * 80)
print("PARSED LOGICAL PLAN")
print("=" * 80)
print(result._jdf.queryExecution().logical().toString())
print()

print("=" * 80)
print("OPTIMIZED LOGICAL PLAN (after Catalyst)")
print("=" * 80)
print(result._jdf.queryExecution().optimizedPlan().toString())
print()

print("=" * 80)
print("PHYSICAL PLAN")
print("=" * 80)
print(result._jdf.queryExecution().sparkPlan().toString())
```

### 3.5 Configure a Cost-Optimal Job Cluster

Test different cluster configurations and measure pipeline duration:

```python
# Notebook: Compare cluster configurations for Bronze ingestion
# This code would run in a notebook that tests different cluster configs

import time

# Configuration A: Small cluster (repo default)
#   autoscale: 2-10 workers, Standard_DS3_v2
#   Expected: ~3-5 minutes for 1M rows

# Configuration B: Single-node
#   autoscale: 0-0 workers, Standard_DS4_v2 (more memory on driver)
#   Expected: ~8-12 minutes for 1M rows (cheaper for small data)

# Configuration C: Compute-optimized
#   autoscale: 2-8 workers, Standard_F4s_v2 (compute-optimized, less memory)
#   Expected: ~2-3 minutes for 1M rows (faster CPU, less RAM per dollar)

start = time.time()
# Run bronze_ingestion.py logic here...
duration = time.time() - start

# Compare costs:
# DS3_v2 = $0.30/hour per node × 3 nodes × 5 min = ~$0.075/run
# DS4_v2 single = $0.50/hour × 1 node × 10 min = ~$0.083/run
# F4s_v2 × 3 nodes × 3 min = $0.27/hr × 3 × 3 min = ~$0.041/run
```

### 3.6 Enable Photon for Comparison

Add a Photon-enabled cluster definition:

```hcl
# terraform/modules/databricks-cluster/main.tf — add this resource
resource "databricks_cluster" "photon" {
  cluster_name            = "${var.cluster_name}-photon"
  spark_version           = var.spark_version
  node_type_id            = var.node_type_id
  autotermination_minutes = 30
  data_security_mode      = "SINGLE_USER"
  single_user_name        = data.databricks_current_user.main.user_name

  autoscale {
    min_workers = var.autoscale_min_workers
    max_workers = var.autoscale_max_workers
  }

  # Photon-specific config
  spark_conf = merge(var.spark_conf, {
    "spark.databricks.photon.enabled" = "true"
  })

  runtime_engine = "PHOTON"

  custom_tags = merge({ PhotonEnabled = "true" }, var.custom_tags)
}
```

Then run Gold aggregation on both Standard and Photon clusters and compare.

---

## 4. Validation & Troubleshooting

### 4.1 Verification Checklist

| ✓ | Check | Command / Assertion |
|---|-------|-------------------|
| ☐ | All three cluster types deployed | `databricks clusters list` shows `-ap`, `-jobs`, `-spot` |
| ☐ | Job cluster auto-terminates after pipeline | Cluster listed as `TERMINATED` in Databricks UI ~30 min after pipeline completes |
| ☐ | Spark UI shows < 50 MB spill per stage | Spark UI → Stages → Shuffle Spill (Memory) = 0 |
| ☐ | Native function > 5× faster than equivalent UDF | See Section 3.3 benchmark |
| ☐ | `explain()` shows column pruning | Physical Plan shows only columns actually used |
| ☐ | `explain()` shows predicate pushdown | `PushedFilters` is not empty in FileScan |
| ☐ | AQE coalescing post-shuffle partitions | Spark UI → SQL tab → `AdaptiveSparkPlan` → shows reduced partition count |
| ☐ | No OOM errors on any executor | Spark UI → Executors → no failed tasks with `OutOfMemoryError` |
| ☐ | GC time < 10% of task duration | Spark UI → Stages → GC Time column |

### 4.2 Common Failure States

#### Failure 1: Executor OutOfMemoryError

```
org.apache.spark.memory.SparkOutOfMemoryError: Unable to acquire 256 MB of memory
```

**Root cause:** The `Standard_DS3_v2` node has 14 GiB RAM. Spark reserves ~40% for execution memory (~5.6 GiB). With 4 tasks/executor, each task gets ~1.4 GiB. If a single row group in Parquet exceeds this, the executor OOMs.

**Fix:**
```python
# Option A: Increase executor memory fraction
spark.conf.set("spark.memory.fraction", "0.8")  # Give Spark 80% instead of 60%

# Option B: Use larger instance type
node_type_id = "Standard_DS4_v2"  # 8 vCPU, 28 GiB RAM

# Option C: Reduce max partitions read per executor
spark.conf.set("spark.sql.files.maxPartitionBytes", "134217728")  # 128 MB
```

#### Failure 2: Pipeline runs for 2+ hours, then job cluster times out

```
Workflow timed out after 7200 seconds (timeout_seconds in medallion_pipeline.yml)
```

**Root cause:** The repo's `timeout_seconds: 7200` (2 hours) is exceeded. Usually caused by data volume growth without scaling up.

**Fix:**
```yaml
# Increase timeout AND add more workers
timeout_seconds: 14400  # 4 hours
job_clusters:
  - job_cluster_key: "etl_cluster"
    new_cluster:
      autoscale:
        min_workers: 4   # Up from 2
        max_workers: 20  # Up from 10
```

#### Failure 3: Spot cluster node evicted mid-pipeline

```
Lost executor 3 on 10.0.4.15: Azure Spot instance eviction
```

**Root cause:** Azure reclaimed the spot VM. The pipeline notebook throws an exception.

**Fix:** The repo's idempotent design handles this — just re-run the pipeline:
```yaml
# In the workflow — configure retries
max_retries: 2
min_retry_interval_millis: 60000  # 1 minute
```

Alternatively, use spot only for non-critical stages:
```yaml
tasks:
  - task_key: "bronze_ingestion"
    job_cluster_key: "etl_cluster"       # On-demand cluster

  - task_key: "data_quality"
    job_cluster_key: "spot_cluster"      # Spot cluster (acceptable to retry)
```

#### Failure 4: UDF runs 100× slower than expected

**Symptom:** A pipeline that normally takes 5 minutes takes 2+ hours after adding a custom transformation.

**Root cause:** Python UDF on a large DataFrame.

**Diagnosis:**
```python
# Check Spark UI → SQL tab → find the UDF stage
# Look for: BatchEvalPython or ArrowEvalPython ← this is a UDF
# If the stage time is dominated by this operator, the UDF is the bottleneck
```

**Fix:** Rewrite using native PySpark functions. See the repo's `PIIMasker` class for reference implementations of `hash_column`, `mask_email`, and `mask_phone` — all native, zero Python overhead.

### 4.3 Cluster Sizing Decision Matrix

| Data Volume (per run) | Workers | Instance | Photon? | Spot? | Cost/Run (est.) |
|----------------------|---------|----------|---------|-------|-----------------|
| < 10 GB | 1–2 | DS3_v2 | No | Yes | $0.05–0.10 |
| 10–100 GB | 2–5 | DS4_v2 | Yes (Gold only) | Bronze/Silver | $0.20–0.80 |
| 100 GB–1 TB | 5–15 | DS5_v2 | Yes | Bronze only | $1.50–5.00 |
| > 1 TB | 15–50 | E8s_v5 (memory-optimized) | Yes | No | $10–50+ |

**The repo's default (2–10 × DS3_v2) is sized for the 10–100 GB range and is intentionally conservative.**

---

## Module 3 Completion Criteria

You have completed Module 3 when:

1. You can trace a Spark job through the DAG from `read_parquet()` to `write_table()` in the Spark UI and identify each stage boundary
2. You have run the same transformation as both a UDF and a native function, measuring at least a 5× performance difference
3. `explain()` output shows Catalyst applying predicate pushdown and column pruning on your queries
4. You can explain why `Window.partitionBy().orderBy()` causes a shuffle and how AQE mitigates the cost
5. You have compared pipeline duration on Standard vs. Photon runtime for the Gold aggregation and recorded the difference
6. You can justify the repo's choice of `Standard_DS3_v2` vs. `Standard_DS4_v2` and know when to switch

**Estimated time:** 5–7 hours, including multiple pipeline runs with different cluster configurations.
