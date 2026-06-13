# Module 2: Storage Architecture & Data Modeling

> **The storage layer.** You will master the physical and logical layout of the enterprise data platform that the `azure-databricks-pipeline-templates` repository operates on — from ADLS Gen2 container hierarchies through Delta Lake ACID transactions to the Medallion design pattern's operational boundaries.

---

## 1. Learning Objectives

| # | Conceptual | Practical |
|---|-----------|-----------|
| 1 | Understand ADLS Gen2's hierarchical namespace: why `abfss://` URIs matter, how directory-level ACLs work, and the performance implications of flat vs. hierarchical namespaces | Navigate the repo's container structure (`bronze`, `silver`, `gold`, `landing`, `checkpoint`, `{env}-metastore`) and trace how every `abfss://` path in `config.py` resolves |
| 2 | Master the Medallion architecture's distinct boundaries — Bronze (immutable append-only raw landing), Silver (deduplicated, validated, PII-masked conformed data), Gold (business-aggregate, denormalized for consumption) | Trace a single record from `landing → bronze → silver → gold` through the repo's notebook chain and identify exactly which transformations happen at each boundary |
| 3 | Internalize Delta Lake internals: the transaction log (`_delta_log`), checkpoint files, Parquet file layout, ACID guarantees, time travel, and the three isolation levels | Run `DESCRIBE HISTORY` on repo-generated tables, roll back to a previous version, and explain the `_delta_log/*.json` JSON commit structure |
| 4 | Understand optimization strategies: Z-Ordering (co-locating related data), Liquid Clustering, file compaction (`OPTIMIZE`), and retention policies (`VACUUM`) | Execute `OPTIMIZE ... ZORDER BY` and `VACUUM` on a repo table and measure query time before/after |
| 5 | Design multi-domain storage schemas: when to partition, when to use a separate container vs. a separate directory, and how the repo uses Unity Catalog schemas for logical separation | Create a new `bronze.finance` schema following the repo pattern, write data to it, and verify it appears in Unity Catalog |

---

## 2. Theoretical Foundations

### 2.1 ADLS Gen2: Hierarchical Namespace & URI Anatomy

The repository's **entire data plane** operates through `abfss://` URIs. This is not cosmetic — it's the enabling abstraction for the entire platform.

**`pipelines/src/config.py` — the URI factory:**
```python
@property
def bronze_path(self) -> str:
    return f"abfss://{self.bronze_container}@{self.storage_account}.dfs.core.windows.net/"

@property
def silver_path(self) -> str:
    return f"abfss://{self.silver_container}@{self.storage_account}.dfs.core.windows.net/"

@property
def gold_path(self) -> str:
    return f"abfss://{self.gold_container}@{self.storage_account}.dfs.core.windows.net/"

@property
def landing_path(self) -> str:
    return f"abfss://{self.landing_container}@{self.storage_account}.dfs.core.windows.net/"

@property
def checkpoint_path(self) -> str:
    return f"abfss://{self.checkpoint_container}@{self.storage_account}.dfs.core.windows.net/"
```

**URI anatomy breakdown:**

```
abfss://bronze@stdevdatabricksdl.dfs.core.windows.net/sales/raw_sales_transactions/
│       │       │                  └─ .dfs.core.windows.net = HNS endpoint (not blob.core)
│       │       └─ Storage account name (from terraform output storage_account_name)
│       └─ Container name (mapped 1:1 with a storage container resource)
└─ Protocol: Azure Blob File System (Spark-native ADLS Gen2 driver)
```

**Why `.dfs.core.windows.net` and NOT `.blob.core.windows.net`?**

- `blob.core.windows.net` = flat namespace endpoint (legacy Blob Storage)
- `dfs.core.windows.net` = hierarchical namespace endpoint (ADLS Gen2)

The repo provisions the storage account with `is_hns_enabled = true` in Terraform:

```hcl
resource "azurerm_storage_account" "datalake" {
  name                = "st${var.environment}databricksdl"
  account_kind        = "StorageV2"
  is_hns_enabled      = true   # ← This is the critical flag
}
```

With HNS enabled, you get:
- **Directory-level ACLs** (not just container-level)
- **Atomic directory renames** (O(1) metadata operation, not O(n) data copy)
- **Path-based POSIX operations** (list files in a directory with a single API call)

**Without HNS**, listing files under `bronze/sales/` would require scanning every blob whose prefix matches — an O(n) operation that becomes untenable at scale.

### 2.2 The Medallion Architecture: Boundaries, Not Suggestions

The repo enforces the Medallion pattern through **physical containers** (not just logical folders). This is a deliberate design decision with operational consequences:

**`terraform/modules/azure-resources/main.tf` — the physical containers:**
```hcl
resource "azurerm_storage_container" "bronze"    { name = "bronze" }
resource "azurerm_storage_container" "silver"    { name = "silver" }
resource "azurerm_storage_container" "gold"      { name = "gold" }
resource "azurerm_storage_container" "checkpoint" { name = "checkpoint" }
resource "azurerm_storage_container" "landing"    { name = "landing" }
resource "azurerm_storage_container" "metastore"  { name = "${var.environment}-metastore" }
```

**Boundary contract — what each layer guarantees:**

| Layer | Container | Write Pattern | Data Characteristics | Immutability |
|-------|-----------|---------------|---------------------|--------------|
| **Landing** | `landing` | External systems drop files here | Raw files in source-native format (CSV, JSON, Parquet). No schema applied. | Ephemeral — files are moved/archived after ingestion |
| **Bronze** | `bronze` | Append-only (`mode="append"`) via `UnityCatalogWriter.write_table()` | Raw data with audit columns (`_ingested_at`, `_source_file`, `_source_name`). Source schema preserved. | **Immutable.** Never UPDATE or DELETE in Bronze. |
| **Silver** | `silver` | Overwrite or Merge via `DeltaWriter.merge()` with deduplication | Cleansed, validated, PII-masked. Enforced schema. Change Data Feed enabled. | Rebuildable from Bronze. Full refresh (`mode="overwrite"`) is acceptable. |
| **Gold** | `gold` | Overwrite (`mode="overwrite"`) via aggregation logic | Business aggregations, denormalized views. Partitioned by date. | Rebuildable from Silver. No PII. Ready for BI consumption. |
| **Checkpoint** | `checkpoint` | Structured Streaming metadata writes | Auto Loader schema locations, streaming checkpoints, DQ reports | Managed by Spark. Never manually modify. |

**Why physical containers instead of folders?**

1. **RBAC boundary:** You can grant different permissions per container: analysts get `SELECT` on `gold` but never see `bronze` raw PII.
2. **Lifecycle management:** Different retention policies per layer — Bronze can have aggressive lifecycle rules (archive after 90 days), Gold retains indefinitely.
3. **Cost attribution:** Container-level metrics in Azure Monitor let you track storage costs per layer.
4. **Performance:** ADLS Gen2 can apply ACLs at the container level more efficiently than recursive directory ACLs.

**The ingestion flow trace (from the repo's notebooks):**

```
┌──────────────────────────────────────────────────────────────────────────┐
│ bronze_ingestion.py                                                       │
│                                                                           │
│  landing/sales_transactions/*.parquet                                     │
│       │                                                                    │
│       ▼ reader.read_parquet() + audit columns                              │
│  ┌─────────────────────────┐                                              │
│  │ Deduplicator            │  dedup on (transaction_id, transaction_date) │
│  │ DataValidator           │  null check on transaction_id, amount, date  │
│  └─────────────────────────┘                                              │
│       │                                                                    │
│       ▼ uc_writer.write_table(mode="append", partition_by=["date(...)"])  │
│  bronze.sales.raw_sales_transactions  ← Unity Catalog table               │
├──────────────────────────────────────────────────────────────────────────┤
│ silver_transformation.py                                                  │
│                                                                           │
│  spark.table("bronze.sales.raw_sales_transactions")                       │
│       │                                                                    │
│       ▼ dedup → validate → PII mask (hash customer_email)                 │
│  silver.sales.sales_transactions_cleaned                                  │
│       │  + tbl_properties: {"delta.enableChangeDataFeed": "true"}         │
├──────────────────────────────────────────────────────────────────────────┤
│ gold_aggregation.py                                                       │
│                                                                           │
│  spark.table("silver.sales.sales_transactions_cleaned")                   │
│       │                                                                    │
│       ▼ groupBy(date, store_id, currency) → KPIs                          │
│  gold.sales.daily_sales_summary                                           │
│       │  partition_by=["date(transaction_date)"]                          │
└──────────────────────────────────────────────────────────────────────────┘
```

### 2.3 Delta Lake Internals: The Transaction Log

Every table created by the repo's `DeltaWriter` or `UnityCatalogWriter` is a Delta table. Understanding the transaction log is essential for debugging data issues.

**File layout of a Delta table:**
```
abfss://bronze@stdevdatabricksdl.dfs.core.windows.net/sales/raw_sales_transactions/
├── _delta_log/
│   ├── 00000000000000000000.json     # Commit 0: table creation
│   ├── 00000000000000000001.json     # Commit 1: first append
│   ├── 00000000000000000002.json     # Commit 2: second append
│   ├── 00000000000000000003.checkpoint.parquet  # Checkpoint (snapshot)
│   └── _last_checkpoint              # Pointer to latest checkpoint
├── transaction_date=2025-01-15/
│   ├── part-00000-abc.parquet
│   └── part-00001-def.parquet
├── transaction_date=2025-01-16/
│   └── part-00000-ghi.parquet
└── ...
```

**What each JSON commit contains:**
```json
{
  "commitInfo": {
    "timestamp": 1736971200000,
    "operation": "WRITE",
    "operationParameters": {"mode": "Append", "partitionBy": "[\"date(transaction_date)\"]"},
    "isolationLevel": "WriteSerializable",
    "isBlindAppend": true
  },
  "protocol": {"minReaderVersion": 1, "minWriterVersion": 2},
  "add": {
    "path": "transaction_date=2025-01-15/part-00000-abc.parquet",
    "size": 45678901,
    "partitionValues": {"transaction_date": "2025-01-15"},
    "modificationTime": 1736971200000,
    "dataChange": true,
    "stats": "{\"numRecords\":10000,\"minValues\":{...},\"maxValues\":{...}}"
  }
}
```

**The ACID guarantee:**
Delta Lake provides serializable isolation through **optimistic concurrency control (OCC)**. Two writes to the same table proceed independently, and the second commit checks the `_delta_log` to ensure no conflicting writes occurred. If a conflict is detected, the second writer retries. This is why you never see partial writes in Delta tables — either the entire commit succeeds or it doesn't.

**The repo configures Delta for performance:**
```python
# From pipelines/src/config.py
spark_conf: Dict[str, str] = field(default_factory=lambda: {
    "spark.databricks.delta.optimizeWrite.enabled": "true",   # Auto-compact small files
    "spark.databricks.delta.autoCompact.enabled": "true",     # Auto-merge small files on write
    "spark.sql.adaptive.enabled": "true",                     # AQE for dynamic partition sizing
    "spark.sql.adaptive.coalescePartitions.enabled": "true",  # Reduce partition count post-shuffle
    "spark.sql.shuffle.partitions": "auto",                   # Let AQE decide partition count
})
```

**Time travel:** The repo's data quality notebook (`data_quality.py`) doesn't explicitly use time travel, but understanding it is critical for operational recovery:
```sql
-- Roll back to a specific version
RESTORE TABLE bronze.sales.raw_sales_transactions TO VERSION AS OF 5;

-- Query a historical snapshot
SELECT * FROM bronze.sales.raw_sales_transactions
  VERSION AS OF 3;

-- Query as of a point in time (requires timestamp to be in the log)
SELECT * FROM bronze.sales.raw_sales_transactions
  TIMESTAMP AS OF '2025-06-01T00:00:00Z';
```

### 2.4 Optimization: Z-Ordering, Liquid Clustering, and Vacuum

The repo's `DeltaWriter.optimize()` method runs automatically after every write:

```python
def optimize(self, table_path: str, zorder_by: Optional[List[str]] = None) -> None:
    if zorder_by:
        zorder_cols = ", ".join(zorder_by)
        self.spark.sql(f"OPTIMIZE delta.`{table_path}` ZORDER BY ({zorder_cols})")
    else:
        self.spark.sql(f"OPTIMIZE delta.`{table_path}`")
```

**Z-Ordering** is a multi-dimensional clustering technique. Unlike partitioning (which creates physically separate directories), Z-Ordering co-locates related data in the same Parquet files:

```python
# Example: in Gold layer, co-locate data by store_id and transaction_date
# so queries filtering on either column only scan relevant files
writer.write(
    gold_df,
    table_path=f"{config.gold_path}sales/daily_sales_summary",
    zorder_by=["store_id", "transaction_date"],
)
```

**When to use Z-Ordering vs. Partitioning:**

| Strategy | Best for | Example | Repo usage |
|----------|----------|---------|------------|
| Partitioning | Low-cardinality columns (< 1000 values) | `date(transaction_date)` | Bronze/Silver/Gold all use date partitioning |
| Z-Ordering | High-cardinality columns (> 1000 values) | `store_id`, `customer_id` | Gold layer: `customer_360` benefits from customer-level Z-ordering |
| Liquid Clustering | Databricks Runtime 13.3+ — simpler, self-tuning | Replaces both partitioning and Z-ordering | Not yet in this repo (DBR 14.3.x supports it — consider adopting) |

**Vacuum:** Deletes stale Parquet files older than the retention window:
```python
def vacuum(self, table_path: str, retention_hours: int = 168) -> None:
    self.spark.sql(f"VACUUM delta.`{table_path}` RETAIN {retention_hours} HOURS")
```

**Critical vacuum semantics:**
- Default retention is **7 days**. You cannot vacuum files younger than this (Delta prevents it).
- Vacuum physically deletes files. After vacuum, time travel beyond the retention window becomes impossible.
- Run vacuum after `OPTIMIZE` — the optimize operation creates new compacted files but doesn't delete the old ones. Vacuum cleans up the orphans.

### 2.5 Unity Catalog Tables vs. External Tables vs. Managed Tables

The repo uses **Unity Catalog managed tables** (the default for `UnityCatalogWriter`):

```python
def write_table(self, df, catalog, schema, table, mode="overwrite", ...):
    full_name = f"{catalog}.{schema}.{table}"
    self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
    df.write.mode(mode).saveAsTable(full_name)
```

But also provides an explicit `create_external_table()` method:
```python
def create_external_table(self, location, catalog, schema, table, file_format="delta"):
    full_name = f"{catalog}.{schema}.{table}"
    self.spark.sql(
        f"CREATE TABLE IF NOT EXISTS {full_name} "
        f"USING {file_format} "
        f"LOCATION '{location}'"
    )
```

**The distinction matters:**

| Table Type | Storage Location | DROP TABLE Behavior | Use Case |
|-----------|-----------------|---------------------|----------|
| **Managed** | `<catalog-storage-root>/<schema>/<table>/` | Deletes data AND metadata | The repo default. Good for pipeline-owned tables where lifecycle is governed by the pipeline. |
| **External** | Specified by `LOCATION` | Drops metadata only; data survives | Good for tables where data is produced by another system (e.g., an ML pipeline writes parquet, then you register it as a UC table). |

---

## 3. Hands-on Execution

### 3.1 Explore the Existing Container Schema

After Module 1 deployment, verify the container layout:

```bash
# List storage containers (data-plane operation — requires Storage Blob Data Contributor)
az storage container list \
  --account-name stdevdatabricksdl \
  --auth-mode login \
  --query "[].name" -o tsv

# Expected output:
# bronze
# checkpoint
# dev-metastore
# gold
# landing
# silver
```

### 3.2 Upload Sample Data to the Landing Zone

Create sample sales data to test the pipeline:

```python
# In a Databricks notebook
import pandas as pd
from datetime import datetime, timedelta
import random

# Generate 100 sample sales transactions
np.random.seed(42)
n = 100

df = pd.DataFrame({
    "transaction_id": [f"TXN-{i:06d}" for i in range(n)],
    "store_id": np.random.choice(["STORE-NYC", "STORE-LON", "STORE-DXB", "STORE-SIN"], n),
    "product_sku": np.random.choice([f"SKU-{i:04d}" for i in range(20)], n),
    "quantity": np.random.randint(1, 10, n),
    "unit_price": np.round(np.random.uniform(5, 200, n), 2),
    "amount": np.round(np.random.uniform(5, 2000, n), 2),
    "currency": np.random.choice(["USD", "EUR", "GBP"], n),
    "payment_method": np.random.choice(["CARD", "CASH", "WALLET"], n),
    "customer_email": [f"user{random.randint(1,500)}@example.com" for _ in range(n)],
    "transaction_date": [
        (datetime(2025, 6, 1) + timedelta(hours=random.randint(0, 720))).strftime("%Y-%m-%d %H:%M:%S")
        for _ in range(n)
    ],
})

# Write to landing zone as parquet
from pipelines.src.config import PipelineConfig
config = PipelineConfig.from_widgets()

landing_path = f"{config.landing_path}sales_transactions/"
df_spark = spark.createDataFrame(df)
df_spark.write.mode("overwrite").parquet(landing_path)

print(f"✅ Sample data written to {landing_path}")
```

### 3.3 Trace a Record Through All Three Layers

After running the bronze ingestion notebook, query each layer and verify the transformation chain:

```python
# Step 1: Verify Bronze — raw data with audit columns
bronze_df = spark.table("bronze.sales.raw_sales_transactions")
bronze_df.select("transaction_id", "_ingested_at", "_source_name").show(5, truncate=False)

# Step 2: Verify Silver — deduplicated, PII masked
silver_df = spark.table("silver.sales.sales_transactions_cleaned")
# customer_email should be SHA256 hashed, not plaintext
silver_df.select("transaction_id", "customer_email_hashed").show(5, truncate=False)

# Step 3: Verify Gold — aggregated
gold_df = spark.table("gold.sales.daily_sales_summary")
gold_df.select("transaction_date", "store_id", "order_count", "total_revenue_usd").show(5)

# Step 4: Time travel — view table history
spark.sql("DESCRIBE HISTORY bronze.sales.raw_sales_transactions").show(truncate=False)
```

### 3.4 Create a New Domain

Add a `finance` domain following the repo pattern:

```python
# In Databricks notebook — adds finance schema to all three catalogs
from pipelines.src.config import PipelineConfig
config = PipelineConfig.from_widgets()

# Create schemas in each catalog (mirrors what Terraform does for existing schemas)
for catalog in [config.bronze_catalog, config.silver_catalog, config.gold_catalog]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.finance")
    print(f"✅ Created {catalog}.finance")

# Write test data to bronze.finance
test_df = spark.createDataFrame(
    [(1, "invoice-001", "2025-06-01", 1500.00, "USD")],
    ["id", "invoice_number", "date", "amount", "currency"]
)

from pipelines.src.writers import UnityCatalogWriter
writer = UnityCatalogWriter(config)
writer.write_table(
    test_df,
    catalog=config.bronze_catalog,
    schema="finance",
    table="raw_invoices",
    mode="overwrite",
    comment="Raw invoice data — Bronze finance layer",
)

# Verify
spark.table(f"{config.bronze_catalog}.finance.raw_invoices").show()
```

### 3.5 Run OPTIMIZE and VACUUM Manually

Experience the performance impact of optimization:

```python
# Before optimization — check file count
files_before = spark.sql(
    "SELECT COUNT(*) as file_count FROM ("
    "  DISTINCT input_file_name() FROM bronze.sales.raw_sales_transactions"
    ")"
).collect()[0][0]
print(f"Files before OPTIMIZE: {files_before}")

# Run OPTIMIZE with Z-Ordering
table_path = f"{config.bronze_path}sales/raw_sales_transactions"
spark.sql(f"OPTIMIZE delta.`{table_path}` ZORDER BY (transaction_date)")

# After optimization
files_after = spark.sql(
    "SELECT COUNT(*) as file_count FROM ("
    "  DISTINCT input_file_name() FROM bronze.sales.raw_sales_transactions"
    ")"
).collect()[0][0]
print(f"Files after OPTIMIZE: {files_after}")
print(f"Compaction: {files_before} → {files_after} files ({(1 - files_after/files_before)*100:.1f}% reduction)")

# Vacuum — clean up orphan files
spark.sql(f"VACUUM delta.`{table_path}` RETAIN 168 HOURS")
print("✅ Vacuum complete — orphan files removed")
```

### 3.6 Configure Delta Table Properties

Add table properties that control behavior:

```sql
-- Enable Change Data Feed (required for CDF-based incremental processing)
ALTER TABLE silver.sales.sales_transactions_cleaned
  SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

-- Enable column mapping (allows renaming/dropping columns without rewriting data)
ALTER TABLE bronze.sales.raw_sales_transactions
  SET TBLPROPERTIES (
    'delta.columnMapping.mode' = 'name',
    'delta.minReaderVersion' = '2',
    'delta.minWriterVersion' = '5'
  );

-- Set a custom retention threshold for time travel
ALTER TABLE gold.sales.daily_sales_summary
  SET TBLPROPERTIES ('delta.logRetentionDuration' = 'interval 30 days');

-- Enable deletion vectors (more efficient DELETE/UPDATE operations)
ALTER TABLE silver.sales.sales_transactions_cleaned
  SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'true');
```

---

## 4. Validation & Troubleshooting

### 4.1 Verification Checklist

| ✓ | Check | Command / Assertion |
|---|-------|-------------------|
| ☐ | All 6 containers exist | `az storage container list --account-name stdevdatabricksdl --auth-mode login --query "[].name"` |
| ☐ | Landing zone accepts writes | Write parquet to `abfss://landing@.../` and verify file count increases |
| ☐ | Bronze table exists in Unity Catalog | `spark.table("bronze.sales.raw_sales_transactions").count() > 0` |
| ☐ | Silver table has PII masked | `silver_df.select("customer_email_hashed")` — values are SHA256 hex, not email addresses |
| ☐ | Gold table is aggregated | `gold_df.groupBy("transaction_date").count()` has fewer rows than Silver |
| ☐ | DESCRIBE HISTORY works | `spark.sql("DESCRIBE HISTORY bronze.sales.raw_sales_transactions")` returns valid version history |
| ☐ | Time travel works | Query `VERSION AS OF 0` and get the original data |
| ☐ | OPTIMIZE reduces file count | `files_before > files_after` |
| ☐ | VACUUM succeeds without error | No `FileNotFoundException` on subsequent queries |
| ☐ | New schema `finance` appears in all three catalogs | `spark.sql("SHOW SCHEMAS IN bronze").filter("databaseName = 'finance'").count() == 1` |

### 4.2 Common Failure States

#### Failure 1: "Cannot create table — path already exists"

```
Error: Cannot create table because the path already exists
and is in use by another Delta table.
```

**Root cause:** A previous failed run left a Delta table at the same path.

**Fix:**
```python
# Option A: Drop the table from Unity Catalog, then recreate
spark.sql("DROP TABLE IF EXISTS bronze.sales.raw_sales_transactions")

# Option B: Remove the underlying files (irreversible!)
import shutil
dbutils.fs.rm("abfss://bronze@stdevdatabricksdl.dfs.core.windows.net/sales/raw_sales_transactions/", recurse=True)
```

#### Failure 2: VACUUM fails with retention violation

```
Error: VACUUM requires a retention period of at least 168 hours.
```

**Root cause:** Delta enforces a minimum 7-day retention — you tried `RETAIN 0 HOURS`.

**Fix:** Always use `RETAIN 168 HOURS` or higher. If you genuinely need to remove files immediately (e.g., GDPR deletion), use:
```python
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
# Then vacuum — but understand this breaks time travel
spark.sql("VACUUM delta.`...` RETAIN 0 HOURS")
```

#### Failure 3: Query returns stale data after Silver overwrite

**Symptom:** Gold aggregation shows old numbers after Silver was refreshed.

**Root cause:** The Silver write uses `mode="overwrite"`, which atomically replaces the table. If Gold queries were in-flight during the overwrite, they saw a snapshot of the old data.

**Fix:** This is expected behavior — Delta's snapshot isolation means readers see a consistent snapshot. Redesign the pipeline DAG so Gold always runs AFTER Silver completes:
```yaml
# In medallion_pipeline.yml — the correct dependency
tasks:
  - task_key: "gold_aggregation"
    depends_on:
      - task_key: "silver_transformation"  # ← Gold waits for Silver
```

#### Failure 4: Parquet files accumulating despite auto-compaction

**Symptom:** Storage costs climbing; thousands of small Parquet files in Bronze.

**Root cause:** Auto Loader's micro-batch writes produce many small files. `autoCompact` helps but has limits.

**Fix:** Schedule an explicit `OPTIMIZE` at the end of each pipeline run:
```python
# In bronze_ingestion.py, after all writes
from pipelines.src.writers import DeltaWriter
dw = DeltaWriter(config)
for table in ["raw_sales_transactions", "raw_customer_profiles", "raw_inventory_movements"]:
    path = f"{config.bronze_path}{table}"
    dw.optimize(path)
    dw.vacuum(path)
```

### 4.3 Partition Strategy Decision Framework

| Data Characteristic | Partition Strategy | Example from Repo |
|--------------------|-------------------|-------------------|
| Time-series data, queried by date range | `date(timestamp_col)` | `partition_by=["date(transaction_date)"]` in Bronze/Silver |
| Multi-tenant data, queried by tenant | `tenant_id` | Not used in repo — would partition `by=["region", "date(date)"]` |
| High-cardinality dimension, frequently filtered | Don't partition — use Z-Order | `zorder_by=["customer_id"]` in Gold |
| Append-only streaming with watermark | No partition on Bronze (causes small files) | Auto Loader streams to path-based Delta, not partitioned |

**The Golden Rule of Partitioning:** Each partition should contain **at least 1 GB** of data. Partitioning on `date(timestamp)` with hourly data creates 24 partitions/day — fine. Partitioning on `customer_id` with 1M customers creates 1M partitions — catastrophic (Spark will OOM listing them).

---

## Module 2 Completion Criteria

You have completed Module 2 when:

1. You can explain the exact write pattern (`append` vs `overwrite`) for each Medallion layer and justify why
2. `DESCRIBE HISTORY` returns at least 3 versions on a Bronze table you populated
3. You have traced a single record from Landing → Bronze → Silver → Gold and verified the PII masking
4. `OPTIMIZE ... ZORDER BY` reduced file count by at least 50% on a table with multiple writes
5. You created a new domain schema (`finance`) following the repo's three-catalog pattern
6. You can explain the difference between `abfss://` and `wasbs://` and when to use `dfs.core.windows.net` vs `blob.core.windows.net`

**Estimated time:** 4–6 hours, including sample data generation and repeated pipeline runs.
