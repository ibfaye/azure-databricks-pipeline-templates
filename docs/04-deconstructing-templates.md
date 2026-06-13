# Module 4: Deconstructing the Templates

> **Repository mastery.** You will conduct a code-level, systemic architectural analysis of every component in `azure-databricks-pipeline-templates`. By the end, you understand not just *what* each file does, but *why* it was designed that way — enabling you to extend, debug, and adapt the templates for any enterprise scenario.

---

## 1. Learning Objectives

| # | Conceptual | Practical |
|---|-----------|-----------|
| 1 | Internalize the configuration-driven architecture: how `PipelineConfig` decouples code from environment, how notebook widgets pass parameters, and how the Databricks Workflow YAML orchestrates the DAG | Trace the full parameter path from `medallion_pipeline.yml`'s `{{environment}}` variable through `dbutils.widgets.get("environment")` to `PipelineConfig.from_widgets()` |
| 2 | Master the modular Python toolkit: understand the single-responsibility design of `readers.py` (ingestion), `transformers.py` (logic), and `writers.py` (persistence) — and how they compose without coupling | Add a new `read_delta_cdf()` method to `DataLakeReader` for Change Data Feed reads, following the existing pattern |
| 3 | Understand idempotency patterns: how the `Deduplicator` with window functions ensures re-runs don't duplicate data, how `merge()` handles upserts, and how Auto Loader checkpoints prevent re-processing | Run the same notebook twice and verify zero row duplication via `SELECT COUNT(*) ... GROUP BY` before and after |
| 4 | Comprehend the error handling strategy: per-entity try/except blocks with explicit failure isolation, `DataQualityException` for structured validation failures, and the `raise` pattern that halts the DAG on critical failures | Intentionally corrupt a source file and observe how the pipeline fails fast (FAILFAST mode in `read_csv()`) with a clear error message |
| 5 | Trace the dbt integration: how notebooks shell out to `subprocess.run(["dbt", ...])`, how the `generate_schema_name` macro routes models to Unity Catalog based on tags, and how the dbt DAG complements the notebook DAG | Run `dbt compile` locally from the `/dbt` directory and inspect the generated SQL for a Silver model |

---

## 2. Theoretical Foundations

### 2.1 Configuration-Driven Architecture: The `PipelineConfig` Contract

The entire repository pivots on a single abstraction: `pipelines/src/config.py`. Understanding it deeply is the key to extending the platform.

**The full `PipelineConfig` dataclass — annotated for architecture:**

```python
@dataclass
class PipelineConfig:
    """Production-grade pipeline configuration."""

    # ── Environment Tier ──────────────────────────────────────────────────
    environment: str = "dev"              # dev | staging | prod
    # Controls: resource naming, logging verbosity, retry behavior

    # ── Unity Catalog Tier ────────────────────────────────────────────────
    bronze_catalog: str = "bronze"        # UC three-level namespace: catalog
    silver_catalog: str = "silver"
    gold_catalog: str = "gold"

    # ── Storage Tier ──────────────────────────────────────────────────────
    storage_account: Optional[str] = None   # Resolved from secrets at runtime
    landing_container: str = "landing"
    bronze_container: str = "bronze"
    silver_container: str = "silver"
    gold_container: str = "gold"
    checkpoint_container: str = "checkpoint"
    # These resolve to abfss:// URIs via the @property methods below

    # ── Pipeline Behavior Tier ────────────────────────────────────────────
    trigger_interval: str = "daily"       # daily | hourly | streaming
    max_retries: int = 3
    retry_delay_seconds: int = 300         # 5 minutes

    # ── Data Quality Tier ─────────────────────────────────────────────────
    null_threshold_pct: float = 5.0        # Fail if >5% nulls in key columns
    freshness_warning_hours: int = 24      # Warn if data > 1 day old
    freshness_error_hours: int = 48        # Error if data > 2 days old

    # ── Spark Configuration Tier ──────────────────────────────────────────
    spark_conf: Dict[str, str] = field(default_factory=lambda: {
        "spark.databricks.delta.optimizeWrite.enabled": "true",
        "spark.databricks.delta.autoCompact.enabled": "true",
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.sql.shuffle.partitions": "auto",
    })
```

**The design pattern: separation of WHAT from HOW**

Everything in `PipelineConfig` answers **WHAT** the pipeline should do ("use the bronze catalog", "fail if nulls exceed 5%"). The notebooks answer **HOW** ("read from this path, apply that transformation"). This separation means:

- **Same notebook code, different environments:** The exact same `bronze_ingestion.py` runs in dev and prod — only the secrets scope contents differ.
- **Testability:** You can instantiate `PipelineConfig(environment="ci", storage_account="testaccount")` in a unit test without touching real infrastructure.
- **Auditability:** Every config value is explicit. There are no magic strings or implicit defaults scattered across notebooks.

**The secret resolution chain:**

```
medallion_pipeline.yml
  └─ parameters:
       ├── environment: "dev"
       └── storage_account: "stdatabricksdl"
            │
            ▼
Databricks Workflow injects parameters as notebook widgets
            │
            ▼
bronze_ingestion.py
  └─ config = PipelineConfig.from_widgets()
       │
       ├── dbutils.widgets.get("environment") → "dev"
       ├── dbutils.widgets.get("storage_account") → "stdatabricksdl"
       │
       └── __post_init__():
            └── self._get_secret("storage-account-name")
                 ├── Primary: dbutils.secrets.get("pipeline-secrets", "storage-account-name")
                 └── Fallback: os.getenv("STORAGE_ACCOUNT_NAME")
```

**The two-tier constructor strategy:**

```python
@classmethod
def from_widgets(cls) -> "PipelineConfig":
    """Create config from Databricks notebook widgets (production path)."""
    try:
        env = dbutils.widgets.get("environment")
    except Exception:
        env = os.getenv("ENVIRONMENT", "dev")
    try:
        account = dbutils.widgets.get("storage_account")
    except Exception:
        account = os.getenv("STORAGE_ACCOUNT")
    return cls(environment=env, storage_account=account)
```

You can also instantiate directly for testing:
```python
config = PipelineConfig(environment="ci", storage_account="testaccount")
```

### 2.2 The Modular Toolkit Architecture

The repo's `pipelines/src/` directory follows a strict single-responsibility pattern:

```
pipelines/src/
├── config.py       # Configuration — WHAT to do
├── readers.py      # Data ingestion — HOW to read
├── transformers.py # Data transformation — HOW to process
├── writers.py      # Data persistence — HOW to write
```

**`readers.py` — The ingestion contract:**

```python
class DataLakeReader:
    """Read data from ADLS Gen2 in various formats with schema inference."""

    def read_csv(self, path, schema=None, **options) -> DataFrame:
        """Production defaults: FAILFAST on malformed data, header inference."""
        defaults = {"header": "true", "mode": "FAILFAST", ...}
        defaults.update(options)
        return self.spark.read.schema(schema).options(**defaults).csv(path)

    def read_stream(self, path, source_format="parquet", schema_location=None, **options) -> DataFrame:
        """Auto Loader with schema evolution: addNewColumns."""
        return (
            self.spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", source_format)
            .option("cloudFiles.schemaLocation", schema_location or self.config.checkpoint_path)
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")  # ← Critical
            .load(path)
        )
```

**Key design decisions in `read_csv()`:**
- `mode="FAILFAST"`: If ANY row is malformed, the entire read fails. This is intentional — you want to know about bad data at ingestion time, not 3 days later when a Gold report looks wrong.
- `inferSchema="true"`: Auto-detects types from data. Tradeoff: slower first read but no manual schema maintenance.
- `multiLine="false"`: Assumes one record per line. Change to `"true"` if your CSVs have embedded newlines in quoted fields (but expect a performance hit).

**`transformers.py` — The four transformation classes:**

| Class | Responsibility | Idempotent? | Used In |
|-------|---------------|-------------|---------|
| `Deduplicator` | Row-level dedup using `row_number() OVER (PARTITION BY ... ORDER BY ...)` | ✅ Yes — keeps latest record per key | Bronze & Silver |
| `DataValidator` | Schema validation, null thresholds, freshness windows | ✅ Yes — read-only checks | Bronze & Silver |
| `PIIMasker` | GDPR/CCPA compliance: hash, mask email, mask phone | ⚠️ Masking is deterministic but irreversible | Silver only |
| `DataQualityException` | Structured exception with violation details | N/A (exception class) | `data_quality.py` |

**`writers.py` — Two writer classes for different targets:**

```python
class DeltaWriter:
    """Write to Delta Lake by path (abfss://...)."""
    def write(self, df, table_path, mode, partition_by, zorder_by, optimize=True, vacuum_retention_hours=168):
        """Write + auto-optimize + auto-vacuum."""
    def merge(self, source, target_path, merge_keys, update_columns, insert_only=False):
        """UPSERT using Delta MERGE."""
    def stream_write(self, df, table_path, checkpoint_path, trigger_interval):
        """Structured Streaming sink."""

class UnityCatalogWriter:
    """Write to Unity Catalog tables (catalog.schema.table)."""
    def write_table(self, df, catalog, schema, table, mode, partition_by, comment, tbl_properties):
        """CREATE SCHEMA IF NOT EXISTS + saveAsTable."""
    def merge_table(self, source, catalog, schema, table, merge_keys, update_columns):
        """MERGE into a UC table."""
    def create_external_table(self, location, catalog, schema, table):
        """Register existing Delta path as UC table."""
    def grant_select(self, catalog, schema, table, principal):
        """GRANT SELECT for RBAC."""
```

**The composition pattern in `bronze_ingestion.py`:**
```python
# ── Wire up dependencies ──
config = PipelineConfig.from_widgets()
reader = DataLakeReader(config)             # <- config injected
writer = DeltaWriter(config)                # <- same config instance
uc_writer = UnityCatalogWriter(config)      # <- shared state

# ── Use them ──
sales_raw = reader.read_parquet(f"{config.landing_path}sales_transactions/")
sales_deduped = Deduplicator(unique_keys=["transaction_id", "transaction_date"]).deduplicate(sales_raw)
uc_writer.write_table(sales_deduped, catalog=config.bronze_catalog, schema="sales", table="raw_sales_transactions")
```

**This is Dependency Injection without a framework.** Every class receives its dependencies through `__init__`. There are no global variables, no singletons, no hidden state. The result: each class is independently testable.

### 2.3 Idempotency: The Golden Rule of Pipeline Engineering

Every pipeline operation in this repo is designed to be **idempotent** — running it twice produces the same result as running it once.

**Pattern 1: Deduplication on unique keys (Bronze ingestion)**

```python
# From transformers.py
class Deduplicator:
    def deduplicate(self, df: DataFrame) -> DataFrame:
        window = Window.partitionBy(*self.unique_keys).orderBy(col(self.order_by).desc())
        return (
            df.withColumn("_row_num", row_number().over(window))
            .filter(col("_row_num") == 1)
            .drop("_row_num")
        )
```

**How this achieves idempotency:**
1. First run: all rows pass through (no duplicates yet). Window assigns `_row_num = 1` to everything.
2. Second run: if source files contain the same rows, the window partitions by unique keys and marks the second occurrence as `_row_num = 2`. The filter removes it.
3. Result: exactly one copy of each unique record regardless of how many times the pipeline runs.

**The critical requirement:** The `order_by` clause determines WHICH copy is kept. The repo uses `_ingested_at DESC` (most recent ingestion timestamp). This means if the same `transaction_id` arrives in multiple batches, the latest version wins.

**Pattern 2: `mode="append"` with dedup (Bronze)**

```python
uc_writer.write_table(
    sales_deduped,
    catalog=config.bronze_catalog,
    schema="sales",
    table="raw_sales_transactions",
    mode="append",                         # ← Append, not overwrite
    partition_by=["date(transaction_date)"],
)
```

**Why append + dedup instead of merge?**
- Merge requires a full table scan to find matching rows. For large Bronze tables, this is expensive.
- Append + periodic dedup is cheaper: you append everything, then dedup only the new partition.
- Tradeoff: Bronze may temporarily contain duplicates between pipeline runs. This is acceptable because Silver deduplicates again.

**Pattern 3: `mode="overwrite"` for master data (Bronze customers)**

```python
uc_writer.write_table(
    customers_deduped,
    catalog=config.bronze_catalog,
    schema="customers",
    table="raw_customer_profiles",
    mode="overwrite",                      # ← Full refresh
)
```

**Why overwrite for customer master data?**
- Customer profiles are a full extract from the CRM — not an incremental feed.
- Overwrite is atomic in Delta Lake: readers see either the old version or the new version, never a partial update.
- No merge keys needed — simpler and faster than upsert.

**Pattern 4: `availableNow` trigger for streaming (Web events, IoT)**

```python
iot_stream = (
    iot_raw.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{config.checkpoint_path}iot/checkpoint")
    .trigger(availableNow=True)            # ← Process all available files, then stop
    .start(f"{config.bronze_path}iot/raw_iot_sensor_readings")
)
iot_stream.awaitTermination()
```

**The `availableNow` trigger:**
- Processes ALL files currently in the source directory, then stops.
- Uses the checkpoint location to track which files have already been ingested.
- Re-running the notebook with `availableNow` is safe: already-processed files are skipped.
- This is NOT continuous streaming — it's batch-style incremental processing. For true streaming, use `trigger(processingTime='10 seconds')`.

### 2.4 Error Handling: Fail Fast, Fail Loud, Fail Isolated

The repo implements a three-tier error handling strategy:

**Tier 1: Per-entity isolation (try/except)**

```python
# From bronze_ingestion.py — each entity gets its own try/except
try:
    sales_raw = reader.read_parquet(...)
    sales_deduped = dedup.deduplicate(sales_raw)
    uc_writer.write_table(sales_deduped, ...)
    print(f"✅ Sales transactions ingested: {sales_deduped.count():,} rows")
except Exception as e:
    print(f"❌ Sales transactions failed: {e}")
    raise   # ← Re-raise: halts the pipeline
```

**Why isolate per entity?** If `sales_transactions` ingestion fails (malformed parquet), you still want `customer_profiles` and `inventory_movements` to succeed. The `raise` at the end ensures the overall notebook fails (which triggers alerts), but other entities aren't collateral damage.

**Tier 2: Structured validation exceptions**

```python
# From transformers.py
class DataQualityException(Exception):
    def __init__(self, message: str, violations: Optional[List[Dict]] = None):
        super().__init__(message)
        self.violations = violations or []

# Used in data_quality.py:
if not all_passed:
    raise DataQualityException(
        f"Data quality checks failed: {sum(1 for c in dq_results['checks'] if not c['passed'])} failures",
        violations=[c for c in dq_results["checks"] if not c["passed"]],
    )
```

**Why a structured exception?** The `violations` list is machine-readable. You can log it to a monitoring system, include it in alert emails, or write it to a Delta table for trend analysis. A plain `Exception("something broke")` loses all diagnostic detail.

**Tier 3: FAILFAST mode for source data**

```python
# From readers.py
def read_csv(self, path, schema=None, **options) -> DataFrame:
    defaults = {
        "mode": "FAILFAST",   # ← Die immediately on malformed data
    }
```

**Why FAILFAST?** In a production pipeline, silently corrupt data is worse than a pipeline failure. `FAILFAST` ensures you catch schema drift, encoding issues, and truncated files at ingestion time — not 3 days later when the CEO asks why the revenue report is off by $2M.

### 2.5 The dbt Integration Pattern

The repo uses dbt for transformations that are better expressed in SQL than PySpark. The integration is via subprocess:

```python
# From silver_transformation.py
result = subprocess.run(
    ["dbt"] + dbt_command.split(),          # e.g., ["dbt", "run", "--select", "silver.*"]
    cwd="/Workspace/Shared/dbt/",           # dbt project root
    capture_output=True,
    text=True,
)
print(result.stdout)
if result.returncode != 0:
    print(f"⚠️  dbt warnings/errors:\n{result.stderr}")
```

**The tag-based catalog routing macro:**

```jinja
{# From dbt/macros/utils.sql #}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if node.tags and 'bronze' in node.tags -%}
        {{ var('bronze_catalog', 'bronze') }}.{{ custom_schema_name | trim }}
    {%- elif node.tags and 'silver' in node.tags -%}
        {{ var('silver_catalog', 'silver') }}.{{ custom_schema_name | trim }}
    {%- elif node.tags and 'gold' in node.tags -%}
        {{ var('gold_catalog', 'gold') }}.{{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
```

**How dbt models route to Unity Catalog:**

```
dbt_project.yml
  models:
    databricks_medallion:
      bronze:
        +tags: ["bronze"]          ← Tag: "bronze"
      silver:
        +tags: ["silver"]          ← Tag: "silver"
      gold:
        +tags: ["gold"]            ← Tag: "gold"

Generate schema name macro sees the tag →
  bronze → "bronze.bronze_raw"     ← {catalog}.{schema}
  silver → "silver.silver_cleansed"
  gold   → "gold.gold_analytics"

Final table name:
  "bronze.bronze_raw.stg_sales_transactions"
```

**The dbt ↔ notebook boundary:**

| Layer | Notebook handles | dbt handles |
|-------|-----------------|-------------|
| Bronze | Raw ingestion, audit columns, streaming | View creation (`stg_*` SQL views over Unity Catalog tables) |
| Silver | PII masking, dedup (Python logic) | SQL transformations, joins, enrichment, SCD Type 2 |
| Gold | Business KPIs via PySpark aggregations | SQL aggregations, window functions, complex joins |

**Why both?** PySpark is better at: file I/O, streaming, complex deduplication, programmatic schema evolution. dbt is better at: SQL transformations, data modeling (refs, sources), testing, documentation generation, and lineage.

---

## 3. Hands-on Execution

### 3.1 Clone the Repository into Databricks Repos

```bash
# In Databricks workspace → Repos → Add Repo
# Git URL: https://github.com/ibfaye/azure-databricks-pipeline-templates
# Branch: main
# Path: /Repos/<your-email>/azure-databricks-pipeline-templates
```

### 3.2 Trace the Full Parameter Path

Add these diagnostic cells to a test notebook:

```python
# Cell 1: Show all widgets (workflow-injected parameters)
import json
widgets = {}
try:
    for w in dbutils.widgets.getAll():
        widgets[w] = dbutils.widgets.get(w)
except Exception:
    widgets = {"error": "No widgets (running interactively)"}
print(json.dumps(widgets, indent=2))

# Cell 2: Show resolved config
from pipelines.src.config import PipelineConfig
config = PipelineConfig.from_widgets()
print(f"Environment:       {config.environment}")
print(f"Storage Account:   {config.storage_account}")
print(f"Bronze Path:       {config.bronze_path}")
print(f"Silver Catalog:    {config.silver_catalog}")
print(f"Gold Catalog:      {config.gold_catalog}")

# Cell 3: Show resolved secrets
try:
    sa = PipelineConfig._get_secret("storage-account-name")
    print(f"Storage account from secrets: {sa}")
except Exception as e:
    print(f"Secrets unavailable: {e}")
```

### 3.3 Add a New Reader Method — `read_delta_cdf()`

Extend `DataLakeReader` to support Change Data Feed reads:

```python
# Add to pipelines/src/readers.py, inside DataLakeReader class:

def read_delta_cdf(
    self,
    table_path: str,
    starting_version: Optional[int] = None,
    starting_timestamp: Optional[str] = None,
) -> DataFrame:
    """
    Read Change Data Feed from a Delta table.
    
    Args:
        table_path: Path to the Delta table
        starting_version: Read CDF changes since this version
        starting_timestamp: Read CDF changes since this timestamp
    
    Returns:
        DataFrame with additional columns: _change_type, _commit_version, _commit_timestamp
    """
    reader = self.spark.read.format("delta") \
        .option("readChangeFeed", "true")
    
    if starting_version is not None:
        reader = reader.option("startingVersion", starting_version)
    elif starting_timestamp is not None:
        reader = reader.option("startingTimestamp", starting_timestamp)
    else:
        # Default: read from version 1 (skip table creation)
        reader = reader.option("startingVersion", 1)
    
    return reader.table(table_path)  # Use .table() for UC tables, .load() for paths
```

Verify it works:
```python
# Test the new method
reader = DataLakeReader(config)

# Read CDF from the silver table
cdf_df = reader.read_delta_cdf(
    table_path=f"{config.silver_catalog}.sales.sales_transactions_cleaned",
    starting_version=0
)
cdf_df.select("_change_type", "transaction_id", "amount", "_commit_version").show(10)
# Expected columns: _change_type (insert/update_preimage/update_postimage/delete)
```

### 3.4 Test Idempotency — Run Twice, Verify Zero Duplication

```python
# Run 1: Record baseline
spark.sql("""
    SELECT transaction_id, transaction_date, COUNT(*) as cnt
    FROM bronze.sales.raw_sales_transactions
    GROUP BY transaction_id, transaction_date
    HAVING COUNT(*) > 1
""").show()
# Should be empty — no duplicates

# Count total rows
run1_count = spark.table("bronze.sales.raw_sales_transactions").count()
print(f"Run 1 count: {run1_count:,}")

# Run the bronze_ingestion notebook AGAIN (via the Databricks UI or API)

# Run 2: Verify no duplicates were introduced
spark.sql("""
    SELECT transaction_id, transaction_date, COUNT(*) as cnt
    FROM bronze.sales.raw_sales_transactions
    GROUP BY transaction_id, transaction_date
    HAVING COUNT(*) > 1
""").show()
# Should STILL be empty

run2_count = spark.table("bronze.sales.raw_sales_transactions").count()
print(f"Run 2 count: {run2_count:,}")
print(f"Delta: {run2_count - run1_count} new rows (should be 0 if same source)")
```

### 3.5 Intentionally Corrupt Source Data (Fail-Fast Test)

```python
# Create a corrupted CSV in the landing zone
corrupt_path = f"{config.landing_path}corrupt_test/"
corrupt_csv = """transaction_id,amount,date
TXN-001,100.00,2025-01-01
TXN-002,INVALID,2025-01-02
TXN-003,200.00,2025-01-03
"""
dbutils.fs.put(f"{corrupt_path}corrupt.csv", corrupt_csv, overwrite=True)

# Try to read it with FAILFAST
try:
    df = spark.read \
        .option("header", "true") \
        .option("mode", "FAILFAST") \
        .csv(corrupt_path)
    df.show()
except Exception as e:
    print(f"✅ Expected failure: {type(e).__name__}")
    print(f"   Message: {e}")
    # Clean up
    dbutils.fs.rm(corrupt_path, recurse=True)
```

### 3.6 Run dbt Models Manually

```bash
# On the Databricks cluster (in a notebook %sh cell or via terminal):
cd /Workspace/Shared/dbt/

# Install dependencies
dbt deps

# Compile all models (syntax check — no execution)
dbt compile

# Run only bronze models
dbt run --select bronze.*

# Run silver models with a specific target
dbt run --select silver.* --target dev

# Run tests
dbt test --select silver.*

# Generate documentation
dbt docs generate
```

### 3.7 Add a New dbt Model Following the Repo Pattern

Create `dbt/models/silver/customer_ltv_enriched.sql`:

```sql
{{ config(
    materialized='incremental',
    unique_key='customer_id',
    on_schema_change='append_new_columns',
    tags=['silver', 'customers']
) }}

WITH customer_base AS (
    SELECT * FROM {{ ref('customers_cleaned') }}
),

sales_summary AS (
    SELECT
        customer_email_hashed,
        SUM(amount_usd) AS total_spend_usd,
        COUNT(DISTINCT transaction_id) AS total_orders,
        MIN(transaction_date) AS first_purchase_date,
        MAX(transaction_date) AS last_purchase_date,
        DATEDIFF(CURRENT_DATE(), MAX(transaction_date)) AS days_since_last_purchase
    FROM {{ source('silver_sales', 'sales_transactions_cleaned') }}
    GROUP BY customer_email_hashed
)

SELECT
    c.customer_id,
    c.email_hashed,
    c.country_code,
    c.loyalty_tier,
    COALESCE(s.total_spend_usd, 0) AS lifetime_value_usd,
    COALESCE(s.total_orders, 0) AS lifetime_orders,
    s.first_purchase_date,
    s.last_purchase_date,
    s.days_since_last_purchase,
    -- LTV tier classification
    CASE
        WHEN COALESCE(s.total_spend_usd, 0) > 10000 THEN 'platinum'
        WHEN COALESCE(s.total_spend_usd, 0) > 5000  THEN 'gold'
        WHEN COALESCE(s.total_spend_usd, 0) > 1000  THEN 'silver'
        ELSE 'bronze'
    END AS ltv_tier,
    {{ add_audit_columns() }}
FROM customer_base c
LEFT JOIN sales_summary s ON c.email_hashed = s.customer_email_hashed
```

Then add the source reference in `dbt/models/bronze/schema.yml`:
```yaml
  - name: silver_sales
    database: "{{ var('silver_catalog', 'silver') }}"
    schema: sales
    tables:
      - name: sales_transactions_cleaned
```

Run it:
```bash
dbt run --select customer_ltv_enriched
```

---

## 4. Validation & Troubleshooting

### 4.1 Verification Checklist

| ✓ | Check | Command / Assertion |
|---|-------|-------------------|
| ☐ | `PipelineConfig.from_widgets()` resolves all parameters | Print all config properties; no `None` values |
| ☐ | `_get_secret()` returns value from either secrets or env | Both `dbutils.secrets.get()` and `os.getenv()` paths tested |
| ☐ | Double-run produces zero duplicate rows | `GROUP BY unique_keys HAVING COUNT(*) > 1` returns empty |
| ☐ | Corrupted CSV fails with clear error message | FAILFAST mode raises `SparkException` with column/row details |
| ☐ | `read_delta_cdf()` returns `_change_type` column | Column contains `insert`, `update_preimage`, etc. |
| ☐ | dbt model compiles without errors | `dbt compile` exit code 0 |
| ☐ | dbt model routes to correct UC catalog | `dbt ls --output json` shows correct `catalog.schema.name` |
| ☐ | New dbt model follows the existing pattern | Materialized config, tags, audit columns, surrogate keys |

### 4.2 Common Failure States

#### Failure 1: `dbutils.widgets.get()` fails with "No widget defined"

```
com.databricks.dbutils_v1.InputWidgetNotDefined: No widget named 'environment'
```

**Root cause:** Running the notebook interactively (not via a Workflow). Workflows inject widgets; interactive runs don't.

**Fix:** The `from_widgets()` method already handles this:
```python
try:
    env = dbutils.widgets.get("environment")
except Exception:
    env = os.getenv("ENVIRONMENT", "dev")  # Fallback to env var or default
```

Manually set widgets for interactive runs:
```python
dbutils.widgets.text("environment", "dev")
dbutils.widgets.text("storage_account", "stdevdatabricksdl")
```

#### Failure 2: `dbt deps` fails with semver error

```
"~> 1.2" is not a valid semantic version.
```

**Root cause:** dbt 1.8+ requires strict semver arrays. The `~>` operator was removed.

**Fix:** The repo's `packages.yml` already uses the correct format:
```yaml
version: [">=1.2.0", "<2.0.0"]  # ✅ Array of separate strings
```
If you added a package with old syntax, convert to array format.

#### Failure 3: dbt model writes to wrong catalog

**Symptom:** `stg_sales_transactions` lands in `main.default` instead of `bronze.bronze_raw`.

**Root cause:** The `generate_schema_name` macro checks for tags, but the model isn't tagged.

**Fix:** Ensure the model's folder is tagged in `dbt_project.yml`:
```yaml
models:
  databricks_medallion:
    bronze:
      +tags: ["bronze"]  # ← This cascades to all models in /bronze/
```

#### Failure 4: DeltaWriter.merge() fails — "Not a Delta table"

```
ValueError: Target path is not a Delta table: abfss://silver@.../table_name
```

**Root cause:** The target path doesn't have a `_delta_log/` directory.

**Fix:** Use `write()` first to create the Delta table, then `merge()` for subsequent updates:
```python
# First run: create the table
writer.write(df, table_path, mode="overwrite")

# Subsequent runs: merge
writer.merge(source_df, table_path, merge_keys=["id"])
```

#### Failure 5: CDF read returns empty DataFrame

**Root cause:** Change Data Feed is not enabled on the source table.

**Fix:**
```sql
ALTER TABLE silver.sales.sales_transactions_cleaned
  SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');
```

### 4.3 Extension Patterns

The repo is designed to be extended. Here are the supported extension points:

| Extension | Where | Pattern |
|-----------|-------|---------|
| New source format (Avro, ORC) | `readers.py` | Add `read_avro()` method following `read_csv()` pattern |
| New transformation (normalization, encoding) | `transformers.py` | Add new class with `__init__` + transform method |
| New write target (Kafka, Event Hubs) | `writers.py` | Add new writer class with `write()` method |
| New pipeline stage (ML inference) | `notebooks/` | Create new notebook, add task to workflow YAML |
| New dbt model (new business domain) | `dbt/models/` | Create SQL file in bronze/silver/gold folder |
| New test (custom dbt test) | `dbt/tests/` | Create generic test SQL with `{% test %}` block |
| New Terraform resource (Azure Function, Event Hub) | `terraform/modules/` | Add resource to existing module or new module |

---

## Module 4 Completion Criteria

You have completed Module 4 when:

1. You can trace any parameter from the Workflow YAML through widgets → `PipelineConfig.from_widgets()` → a notebook's use of `config.bronze_path`
2. You have extended `readers.py` with a new method (e.g., `read_delta_cdf()`) that follows the existing error handling and config injection patterns
3. You have run the same notebook twice and verified zero duplication (idempotency validated)
4. You have intentionally corrupted a source file and observed FAILFAST producing a clear, actionable error
5. You have created a new dbt model that routes to the correct Unity Catalog schema using the tag-based macro
6. You can explain which pipeline operations are idempotent and which are not, and why

**Estimated time:** 6–8 hours, including reading every source file in the repo and implementing at least one extension.
