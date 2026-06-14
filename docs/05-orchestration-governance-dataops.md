# Module 5: Orchestration, Governance & DataOps

> **The production layer.** You will wrap the isolated pipeline templates into a resilient, governed, automated enterprise system — deploying via CI/CD, orchestrating with Databricks Workflows, and securing with Unity Catalog's granular access controls. This module transforms you from "pipeline developer" to "data platform engineer."

---

## 1. Learning Objectives

| #   | Conceptual                                                                                                                                                                                                                   | Practical                                                                                                                                        |
| --- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------ |
| 1   | Master Databricks Workflows as a DAG orchestration engine: task dependencies, parameter passing, repair runs, retry policies, and the distinction from Airflow/Prefect                                                       | Deploy the exact `medallion_pipeline.yml` workflow via `databricks jobs create`, trigger a run, and interpret the run graph                      |
| 2   | Internalize Unity Catalog governance: three-level namespaces (`catalog.schema.table`), fine-grained grants (`SELECT`, `MODIFY`, `ALL_PRIVILEGES`), lineage tracking, and the security model difference from `hive_metastore` | Grant `SELECT` on `gold.sales.daily_sales_summary` to a read-only group and verify they CANNOT query `bronze.sales.raw_sales_transactions`       |
| 3   | Design CI/CD pipelines for data platforms: infrastructure-as-code validation (terraform fmt/validate/tflint), dbt compilation/testing in CI, and immutable deployment patterns for notebook code                             | Set up a GitHub Actions workflow that runs `terraform validate`, `tflint`, and `dbt compile` on every PR — exactly as the repo's CI already does |
| 4   | Implement observability: structured logging, pipeline metrics (row counts, durations, freshness), alerting on failure, and the `data_quality.py` → Delta table → dashboard pipeline                                          | Query the DQ reports Delta table and build a simple dashboard showing pass/fail trends over 30 days                                              |
| 5   | Automate the full deployment lifecycle: from `git push` → CI validation → Databricks workspace update → workflow trigger                                                                                                     | Implement a GitHub Action that runs `databricks workspace import` to push notebook changes and triggers a workflow run                           |

---

## 2. Theoretical Foundations

### 2.1 Databricks Workflows: The DAG Orchestration Engine

The repo's `medallion_pipeline.yml` defines a five-task DAG. Understanding its structure is the key to production orchestration:

```yaml
tasks:
  # Task 1: Bronze Ingestion (no dependencies — runs immediately)
  - task_key: 'bronze_ingestion'
    notebook_task:
      notebook_path: '/Shared/pipelines/medallion/bronze_ingestion'
      base_parameters:
        environment: '{{environment}}'
        storage_account: '{{storage_account}}'
    timeout_seconds: 3600
    max_retries: 1

  # Task 2: Silver Transformation (depends on Task 1)
  - task_key: 'silver_transformation'
    depends_on:
      - task_key: 'bronze_ingestion'
    notebook_task:
      notebook_path: '/Shared/pipelines/medallion/silver_transformation'
      base_parameters:
        environment: '{{environment}}'
        dbt_command: 'run --select silver.* --target {{environment}}'

  # Task 3: Gold Aggregation (depends on Task 2)
  - task_key: 'gold_aggregation'
    depends_on:
      - task_key: 'silver_transformation'

  # Task 4: Data Quality (depends on Task 3 — parallel to Task 5)
  - task_key: 'data_quality'
    depends_on:
      - task_key: 'gold_aggregation'

  # Task 5: dbt Tests (depends on Task 3 — runs in parallel with Task 4)
  - task_key: 'dbt_tests'
    depends_on:
      - task_key: 'gold_aggregation'
```

**The resulting DAG:**

```
                        ┌─────────────────┐
                        │ bronze_ingestion │ (no dependencies)
                        └────────┬────────┘
                                 │
                        ┌────────▼────────┐
                        │silver_transform │
                        └────────┬────────┘
                                 │
                        ┌────────▼────────┐
                        │ gold_aggregation│
                        └──┬──────────┬───┘
                           │          │
              ┌────────────▼──┐  ┌───▼───────────┐
              │ data_quality  │  │  dbt_tests     │  ← Parallel execution
              └───────────────┘  └────────────────┘
```

**Key workflow features in the repo:**

**1. Parameter passing via `{{mustache}}` syntax:**

```yaml
parameters:
  - name: 'environment'
    default: 'dev'
  - name: 'storage_account'
    default: 'stdatabricksdl'
```

These are defined at the workflow level and injected into each task's `base_parameters`. When you trigger a run with overrides, every task receives the same value:

```bash
databricks jobs run-now --job-id 123 \
  --notebook-params '{"environment":"prod","storage_account":"stproddatabricksdl"}'
```

**2. `max_retries` with `min_retry_interval_millis`:**

```yaml
max_retries: 1
min_retry_interval_millis: 300000 # 5 minutes
```

If Bronze ingestion fails (transient ADLS throttling), Databricks waits 5 minutes, then retries once. This is a **linear retry** — for exponential backoff, you'd need to implement retry logic inside the notebook itself.

**3. `timeout_seconds` as a safety net:**

```yaml
timeout_seconds: 7200 # 2 hours for the entire workflow
```

Per-task timeouts protect against infinite loops:

```yaml
- task_key: 'bronze_ingestion'
  timeout_seconds: 3600 # 1 hour max for bronze
```

**4. Email notifications on failure:**

```yaml
email_notifications:
  on_failure:
    - 'data-engineering@company.com'
  no_alert_for_skipped_runs: false
```

**5. Repair runs — the operational superpower:**

```bash
# Re-run only failed tasks (dependencies are respected)
databricks jobs repair-run --run-id 456 --rerun-all-failed-tasks

# Re-run a specific task and all its downstream dependencies
databricks jobs repair-run --run-id 456 \
  --latest-allow-list '[{"task_key":"silver_transformation"}]'
```

### 2.2 Terraform-Managed Workflows

The repo also defines the workflow in Terraform (`terraform/main.tf:49-143`) — this is the **production deployment path**:

```hcl
resource "databricks_job" "medallion_pipeline" {
  name = "${var.project_name}-${var.environment}-medallion"

  job_cluster {
    job_cluster_key = "default"
    new_cluster {
      spark_version      = var.dbr_version
      node_type_id       = var.cluster_node_type
      data_security_mode = "SINGLE_USER"
      autoscale {
        min_workers = var.autoscale_min_workers
        max_workers = var.autoscale_max_workers
      }
    }
  }

  task {
    task_key        = "bronze_ingestion"
    job_cluster_key = "default"
    notebook_task {
      notebook_path = "/Shared/pipelines/medallion/bronze_ingestion"
      base_parameters = {
        catalog         = "bronze"
        landing_path    = "abfss://landing@${module.azure.storage_account_name}.dfs.core.windows.net/"
        checkpoint_path = "abfss://checkpoint@${module.azure.storage_account_name}.dfs.core.windows.net/bronze/"
      }
    }
  }
  # ... more tasks ...

  schedule {
    quartz_cron_expression = "0 0 6 * * ?"  # Daily at 6 AM UTC
    timezone_id            = "UTC"
  }
}
```

**Why Terraform-managed workflows?**

- **Infrastructure as Code:** The workflow definition is version-controlled alongside the infrastructure it depends on
- **Parameter binding at deploy time:** Terraform resolves `${module.azure.storage_account_name}` at `terraform apply`, injecting the actual storage account name into the workflow
- **Environment isolation:** `terraform apply -var="environment=prod"` creates a completely separate workflow with prod-specific parameters
- **Rollback:** `git revert` + `terraform apply` = instant rollback to a known-good workflow definition

### 2.3 Unity Catalog Governance

The repo's Terraform module deploys a comprehensive Unity Catalog security model:

**The three-catalog hierarchy with grants:**

```hcl
# Bronze: admins get ALL_PRIVILEGES, readers get USE + SELECT
resource "databricks_grants" "bronze_catalog" {
  catalog = databricks_catalog.bronze.name
  grant {
    principal  = var.admin_group_name     # "admins"
    privileges = ["ALL_PRIVILEGES"]
  }
  grant {
    principal  = var.reader_group_name    # "analysts"
    privileges = ["USE_CATALOG", "USE_SCHEMA", "SELECT"]
  }
}
```

**The resulting access matrix:**

| Principal                                  | Bronze                                     | Silver                          | Gold                            | Metastore                                      |
| ------------------------------------------ | ------------------------------------------ | ------------------------------- | ------------------------------- | ---------------------------------------------- |
| `admins`                                   | ALL_PRIVILEGES                             | ALL_PRIVILEGES                  | ALL_PRIVILEGES                  | CREATE_CATALOG, CREATE_EXTERNAL_LOCATION, etc. |
| `analysts`                                 | USE_CATALOG, USE_SCHEMA, SELECT            | USE_CATALOG, USE_SCHEMA, SELECT | USE_CATALOG, USE_SCHEMA, SELECT | (none — can't create)                          |
| Data engineers (individual users)          | ALL_PRIVILEGES                             | ALL_PRIVILEGES                  | ALL_PRIVILEGES                  | Inherits from group membership                 |
| Service principal (`sp-unity-catalog-dev`) | Owner (via `databricks_service_principal`) | Owner                           | Owner                           | Storage credential creator                     |

**Why this matters operationally:**

1. **Analysts query Gold but never see PII in Bronze:** The `SELECT` grant on `gold` lets them build dashboards. Without `SELECT` on `bronze`, they can't accidentally query raw customer emails.
2. **Data engineers write pipelines, analysts read results:** The group-based RBAC means you add a person to the `analysts` group once, and they automatically get read access to all three layers.
3. **Service principal isolation:** The pipeline runs as a specific service principal with elevated storage permissions. No human has those credentials — they exist only in Terraform state and Databricks.

**The `UnityCatalogWriter.grant_select()` helper:**

```python
# From pipelines/src/writers.py
def grant_select(self, catalog: str, schema: str, table: str, principal: str) -> None:
    full_name = f"{catalog}.{schema}.{table}"
    self.spark.sql(f"GRANT SELECT ON TABLE {full_name} TO `{principal}`")
```

This is called programmatically after table creation — no manual SQL needed:

```python
uc_writer.grant_select("gold", "sales", "daily_sales_summary", "analysts")
```

### 2.4 CI/CD: The Existing Pipeline

The repo ships with two GitHub Actions workflows:

**Workflow 1: `terraform-validate.yml`**

```yaml
on:
  pull_request:
    paths: ['terraform/**']
  push:
    branches: [main]
    paths: ['terraform/**']
```

**Jobs:**

1. **`fmt-validate`:** `terraform fmt -recursive` (auto-fix) → `terraform init -backend=false` → `terraform validate`
2. **`tflint`:** `tflint --init` → `tflint --recursive`

**Workflow 2: `dbt-ci.yml`**

```yaml
on:
  pull_request:
    paths: ['dbt/**', '.github/workflows/dbt-ci.yml']
  push:
    branches: [main]
    paths: ['dbt/**', '.github/workflows/dbt-ci.yml']
```

**Job: `dbt-validate`:** Install dbt-databricks (or Fusion-compatible adapter) → generate `profiles.yml` → `dbt deps` → `dbt compile --target ci`

**The architectural insight:** Both workflows are **validate-only** — they check syntax and structure but don't connect to production resources. This is intentional:

- Terraform validation uses `-backend=false` (no state file access needed)
- dbt compile uses CI credentials with minimal permissions
- Neither workflow can affect production

**What's missing from the repo's CI (and you should add in Module 5):**

- **Notebook deployment:** Pushing updated `.py` notebooks to the Databricks workspace
- **Workflow trigger:** Automatically running the pipeline after a successful deployment
- **Integration tests:** Running a subset of the pipeline against test data in CI
- **Python linting:** Running `flake8` / `mypy` / `black` on `pipelines/src/*.py`

### 2.5 Observability: The DQ Reports Pipeline

The repo's `data_quality.py` notebook produces structured JSON reports and writes them to a Delta table:

```python
dq_results = {
    "run_timestamp": datetime.now(timezone.utc).isoformat(),
    "environment": config.environment,
    "checks": [
        {"name": "Row count — Bronze sales", "passed": True, "detail": "1,234 rows"},
        {"name": "Freshness — Sales transactions", "passed": True, "detail": "Latest: 2025-06-13"},
        # ...
    ],
}

dq_df = spark.createDataFrame([json.dumps(dq_results)], "value string")
dq_path = f"{config.checkpoint_path}/data_quality_reports/"
dq_df.write.mode("append").format("delta").save(dq_path)
```

**This enables a complete observability stack:**

```
Pipeline Run → data_quality.py → dq_reports Delta table
                                      │
                                      ├── Databricks SQL Dashboard (last 30 days of pass/fail)
                                      ├── Azure Monitor Alert (query every hour, alert on failures)
                                      └── Power BI / Tableau (long-term DQ trends)
```

**Building on this: you can query DQ trends:**

```sql
-- Count failures over the last 7 days
SELECT
  DATE(from_unixtime(run_timestamp/1000)) AS run_date,
  SUM(CASE WHEN check.passed = false THEN 1 ELSE 0 END) AS failures,
  COUNT(*) AS total_checks
FROM (
  SELECT
    from_json(value, 'STRUCT<run_timestamp: BIGINT, checks: ARRAY<STRUCT<name: STRING, passed: BOOLEAN>>>') AS parsed
  FROM delta.`abfss://checkpoint@.../data_quality_reports/`
)
LATERAL VIEW EXPLODE(parsed.checks) AS check
WHERE parsed.run_timestamp > unix_timestamp(current_date() - INTERVAL 7 DAYS) * 1000
GROUP BY run_date
ORDER BY run_date;
```

---

## 3. Hands-on Execution

### 3.1 Deploy the Workflow via Databricks CLI

```bash
# Authenticate
databricks configure --token

# Deploy the workflow YAML
databricks jobs create --json @pipelines/workflows/medallion_pipeline.yml
# Returns: { "job_id": 12345 }

# Or, deploy via Terraform (production path):
cd terraform
terraform apply -target=databricks_job.medallion_pipeline
```

### 3.2 Trigger and Monitor a Run

```bash
# Trigger a manual run (override parameters)
JOB_ID=$(databricks jobs list --name "medallion_pipeline" --output json | jq -r '.[0].job_id')

RUN_ID=$(databricks jobs run-now --job-id $JOB_ID \
  --notebook-params '{"environment":"dev","storage_account":"stdevdatabricksdl"}' \
  --output json | jq -r '.run_id')

echo "Run ID: $RUN_ID"

# Monitor progress
databricks runs get --run-id $RUN_ID --output json | jq '{state: .state.life_cycle_state, tasks: [.tasks[] | {task_key, state: .state.life_cycle_state}]}'

# Wait for completion
databricks runs wait --run-id $RUN_ID --timeout 30m

# Check final status
databricks runs get-output --run-id $RUN_ID --output json | jq '.metadata.state.result_state'
```

### 3.3 Perform a Repair Run

```bash
# Simulate a failure: Silver transformation fails, Gold is skipped
# ... after fixing the Silver notebook ...

# Repair — re-run Silver and all downstream tasks (Gold, DQ, dbt tests)
databricks jobs repair-run --run-id $RUN_ID \
  --rerun-all-failed-tasks

# Or, re-run only a specific task chain:
databricks jobs repair-run --run-id $RUN_ID \
  --latest-allow-list '[{"task_key":"gold_aggregation"}]'
```

### 3.4 Test Unity Catalog Permissions

```bash
# 1. Create a test user (or use your own)
# In Databricks SQL:

-- Grant SELECT on Gold only
GRANT SELECT ON TABLE gold.sales.daily_sales_summary TO `analysts`;

-- Verify: analysts CAN query Gold
-- (Run as a user in the 'analysts' group)
SELECT * FROM gold.sales.daily_sales_summary LIMIT 5;
-- ✅ Works

-- Verify: analysts CANNOT query Bronze
SELECT * FROM bronze.sales.raw_sales_transactions LIMIT 5;
-- ❌ Error: User does not have SELECT permission on table
```

### 3.5 Build a DQ Dashboard

Create a Databricks SQL query and dashboard:

```sql
-- Query: DQ Pass/Fail Trend (Last 30 Days)
WITH dq_data AS (
  SELECT
    from_json(value, 'STRUCT<run_timestamp: STRING, environment: STRING, checks: ARRAY<STRUCT<name: STRING, passed: BOOLEAN, detail: STRING>>, overall_status: STRING>') AS parsed
  FROM delta.`abfss://checkpoint@${storage_account}.dfs.core.windows.net/data_quality_reports/`
),
exploded AS (
  SELECT
    DATE(parsed.run_timestamp) AS run_date,
    parsed.environment,
    check.name AS check_name,
    check.passed,
    check.detail
  FROM dq_data
  LATERAL VIEW EXPLODE(parsed.checks) AS check
  WHERE DATE(parsed.run_timestamp) >= CURRENT_DATE() - INTERVAL 30 DAYS
)
SELECT
  run_date,
  check_name,
  SUM(CASE WHEN passed THEN 0 ELSE 1 END) AS failure_count,
  COUNT(*) AS total_runs,
  ROUND(SUM(CASE WHEN passed THEN 0 ELSE 1 END) * 100.0 / COUNT(*), 1) AS failure_rate_pct
FROM exploded
GROUP BY run_date, check_name
ORDER BY run_date DESC, failure_rate_pct DESC;
```

Then create a Databricks SQL Dashboard with:

- **Line chart:** Failure rate over time
- **Table:** Latest failures with detail messages
- **Counter:** Total pipeline runs, overall pass rate

### 3.6 Extend CI/CD with Notebook Deployment

Add a new GitHub Actions workflow `deploy-notebooks.yml`:

```yaml
name: Deploy Notebooks to Databricks

on:
  push:
    branches: [main]
    paths:
      - 'pipelines/**'
      - 'dbt/**'

jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Setup Databricks CLI
        uses: databricks/setup-cli@main
        with:
          token: ${{ secrets.DATABRICKS_TOKEN }}
          host: ${{ secrets.DATABRICKS_HOST }}

      - name: Deploy notebooks to workspace
        run: |
          # Upload each notebook to the workspace
          for notebook in pipelines/notebooks/*.py; do
            notebook_name=$(basename "$notebook" .py)
            echo "Deploying: $notebook_name"

            databricks workspace import \
              --language PYTHON \
              --overwrite \
              "$notebook" \
              "/Shared/pipelines/medallion/$notebook_name"
          done

      - name: Deploy dbt project
        run: |
          # Sync dbt directory to workspace
          databricks workspace import-dir \
            --overwrite \
            dbt/ \
            /Shared/dbt/

      - name: Trigger medallion pipeline
        run: |
          JOB_ID=$(databricks jobs list --output json | \
            jq -r '.[] | select(.settings.name | startswith("dbx-pipeline")) | .job_id' | head -1)

          databricks jobs run-now --job-id $JOB_ID
```

**Add required secrets to GitHub:**

```bash
gh secret set DATABRICKS_HOST --body "https://adb-1234567890.7.azuredatabricks.net"
gh secret set DATABRICKS_TOKEN --body "dapi..."
```

### 3.7 Set Up Azure Monitor Alerts

```bash
# Create an action group first (required for alert notifications)
az monitor action-group create \
  --name "data-engineering-alerts" \
  --resource-group "rg-dbx-pipeline-dev" \
  --action email data-engineering@company.com

# Create an alert on Databricks job failures
az monitor metrics alert create \
  --name "medallion-pipeline-failure" \
  --resource-group "rg-dbx-pipeline-dev" \
  --scopes "/subscriptions/$SUB_ID/resourceGroups/rg-dbx-pipeline-dev/providers/Microsoft.Databricks/workspaces/dbw-dbx-pipeline-dev" \
  --condition "avg Failed Runs > 0" \
  --window-size 15m \
  --evaluation-frequency 5m \
  --action "/subscriptions/$SUB_ID/resourceGroups/rg-dbx-pipeline-dev/providers/microsoft.insights/actionGroups/data-engineering-alerts"
```

### 3.8 Implement the Full Deployment Lifecycle

The complete flow:

```
Developer pushes to main branch
        │
        ▼
GitHub Actions triggers: deploy-notebooks.yml
        │
        ├── terraform validate + tflint (existing CI)
        ├── dbt compile (existing CI)
        ├── databricks workspace import (new — deploys notebooks)
        ├── databricks workspace import-dir (new — deploys dbt)
        └── databricks jobs run-now (new — triggers pipeline)
                │
                ▼
Databricks Workflow executes:
        │
        ├── bronze_ingestion
        ├── silver_transformation
        ├── gold_aggregation
        ├── data_quality ────────┐
        └── dbt_tests ───────────┤
                                 ▼
                        DQ reports written to Delta
                                 │
                                 ▼
                        Dashboard updated, alerts fired if failures
```

---

## 4. Validation & Troubleshooting

### 4.1 Verification Checklist

| ✓   | Check                                               | Command / Assertion                                                                                     |
| --- | --------------------------------------------------- | ------------------------------------------------------------------------------------------------------- |
| ☐   | Workflow deployed and schedulable                   | `databricks jobs list` shows `medallion_pipeline`                                                       |
| ☐   | Manual run completes all 5 tasks                    | `databricks runs get-output --run-id <id>` shows `result_state: SUCCESS`                                |
| ☐   | Repair run re-executes only failed/downstream tasks | After simulating failure, repair run shows `silver`, `gold`, `dq`, `dbt_tests` re-run; `bronze` skipped |
| ☐   | Analysts can query Gold but not Bronze              | Run queries as analyst user — Gold succeeds, Bronze fails with permission error                         |
| ☐   | DQ reports table accumulates records                | `SELECT COUNT(*) FROM delta.\`<checkpoint_path>/data_quality_reports/\`` increases after each run       |
| ☐   | GitHub Actions CI passes on PR                      | `terraform-validate.yml` → ✅, `dbt-ci.yml` → ✅                                                        |
| ☐   | Notebook deployment updates workspace files         | Check Databricks workspace → `/Shared/pipelines/medallion/bronze_ingestion` has latest commit SHA       |
| ☐   | Pipeline triggered automatically after push         | Push to main → check Databricks Jobs UI for a new run                                                   |

### 4.2 Common Failure States

#### Failure 1: Workflow deploy fails — "notebook not found"

```
Error: Notebook does not exist at /Shared/pipelines/medallion/bronze_ingestion
```

**Root cause:** The workflow YAML references notebooks that haven't been deployed to the workspace yet.

**Fix:** Deploy notebooks BEFORE creating the workflow:

```bash
# Deploy all notebooks
for notebook in pipelines/notebooks/*.py; do
  databricks workspace import --language PYTHON --overwrite \
    "$notebook" "/Shared/pipelines/medallion/$(basename "$notebook" .py)"
done

# Then create the workflow
databricks jobs create --json @pipelines/workflows/medallion_pipeline.yml
```

Or use Terraform's `depends_on` (in a more advanced setup):

```hcl
resource "databricks_notebook" "bronze" {
  source = "${path.root}/../pipelines/notebooks/bronze_ingestion.py"
  path   = "/Shared/pipelines/medallion/bronze_ingestion"
}

resource "databricks_job" "medallion" {
  depends_on = [databricks_notebook.bronze]
  # ...
}
```

#### Failure 2: "User does not have SELECT permission" for analysts

**Symptom:** Analyst user can't query Gold tables despite having group membership.

**Root cause (common):** The `USE_CATALOG` and `USE_SCHEMA` grants must be explicitly given. `SELECT` alone is insufficient — the user needs `USE` on both the catalog and the schema to even "see" the table.

**Fix:**

```sql
-- The full grant chain
GRANT USE_CATALOG ON CATALOG gold TO `analysts`;
GRANT USE_SCHEMA ON SCHEMA gold.sales TO `analysts`;
GRANT SELECT ON TABLE gold.sales.daily_sales_summary TO `analysts`;
```

The repo's Terraform grants `USE_CATALOG` at the catalog level, which cascades to all schemas:

```hcl
grant {
  principal  = var.reader_group_name
  privileges = ["USE_CATALOG", "USE_SCHEMA", "SELECT"]
}
```

#### Failure 3: GitHub Actions deploy step fails — "Invalid access token"

```
Error: invalid access token
```

**Root cause:** The `DATABRICKS_TOKEN` GitHub secret is expired or missing.

**Fix:** Generate a new Databricks personal access token:

```bash
# In Databricks workspace → User Settings → Developer → Access Tokens → Generate
# Copy the token (shown only once)

gh secret set DATABRICKS_TOKEN --body "dapi..."
```

**Production best practice:** Use a service principal token instead of a personal access token:

```bash
databricks tokens create --comment "CI/CD deployment" --lifetime-seconds 7776000
```

#### Failure 4: DQ report table grows unbounded

**Symptom:** Storage costs increasing; DQ report table has millions of rows from daily runs.

**Root cause:** The DQ notebook uses `mode="append"` but the vacuum doesn't clean up old data because the retention window (90 days) matches the vacuum window.

**Fix:** The repo already handles this:

```python
# From data_quality.py — automatic retention management
try:
    spark.sql(f"VACUUM delta.`{dq_path}` RETAIN 2160 HOURS")  # 90 days
except Exception:
    pass
```

### 4.3 Production Readiness Checklist

Before deploying to production, verify:

| #   | Requirement                                          | Implementation                                                     |
| --- | ---------------------------------------------------- | ------------------------------------------------------------------ |
| 1   | Workflow scheduled with appropriate CRON             | `quartz_cron_expression: "0 0 6 * * ?"`                            |
| 2   | Max concurrent runs = 1 (prevents overlapping runs)  | `max_concurrent_runs: 1`                                           |
| 3   | Email alerts configured                              | `email_notifications.on_failure: ["data-engineering@company.com"]` |
| 4   | Separate dev/staging/prod environments               | `environment` variable controls all differences                    |
| 5   | Production uses premium SKU (Unity Catalog required) | `databricks_workspace_sku = "premium"`                             |
| 6   | State file stored remotely (Azure Storage)           | `backend "azurerm"` in `providers.tf`                              |
| 7   | Secrets in Key Vault, never in code                  | `dbutils.secrets.get()` reads from AKV-backed scope                |
| 8   | Cost tags on all resources                           | `custom_tags: { cost_center: "data-engineering" }`                 |
| 9   | Budget alerts configured                             | `az consumption budget create`                                     |
| 10  | CI/CD deploys automatically on merge to main         | GitHub Actions → Databricks workspace                              |

---

## Module 5 Completion Criteria

You have completed Module 5 when:

1. A `git push` to `main` triggers CI validation (Terraform + dbt), deploys notebooks to Databricks, and triggers the medallion pipeline — all automated
2. The pipeline runs successfully through all 5 stages (Bronze → Silver → Gold → DQ + dbt Tests)
3. An analyst in the `analysts` group can query `gold.sales.daily_sales_summary` but gets a permission error on `bronze.sales.raw_sales_transactions`
4. You have performed a repair run on a partially failed pipeline and only the failed/downstream tasks re-executed
5. The DQ reports Delta table has at least 3 runs recorded, and you can query pass/fail trends via SQL
6. Azure budget alerts are configured and will notify you before the monthly cost exceeds $500

**Estimated time:** 5–7 hours, including CI/CD setup, workflow debugging, and permission testing.

---

## Curriculum Completion

You have completed all five modules of the `azure-databricks-pipeline-templates` mastery curriculum. You now possess:

- **Module 1:** The ability to provision the entire Azure infrastructure stack from scratch using IaC
- **Module 2:** A deep understanding of ADLS Gen2, Delta Lake internals, and the Medallion pattern's operational contract
- **Module 3:** Mastery of Spark's distributed execution, cluster topology, and PySpark optimization
- **Module 4:** The ability to read, extend, and debug every component in the repository
- **Module 5:** Production-grade orchestration, governance, CI/CD, and observability

**The next step:** Extend the platform. Add a new data source (Kafka, Event Hubs, REST API), implement a new Gold aggregation, add a dbt snapshot for SCD Type 2, or build a real-time streaming branch using `trigger(processingTime='10 seconds')`. The templates are designed to be extended — you now know exactly how.
