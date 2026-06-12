# Architecture Deep Dive

## Principles

1. **Infrastructure as Code**: Everything is declarative in Terraform — no manual Azure Portal clicks
2. **Medallion Architecture**: Bronze (raw) → Silver (validated) → Gold (business-ready)
3. **Unity Catalog-First**: All data lives in UC catalogs from day one — no `hive_metastore`
4. **dbt as Transformation Engine**: SQL-based, version-controlled, testable transformations
5. **Security by Default**: VNet isolation, PII masking, least-privilege RBAC

## Terraform Module Design

```
terraform/
├── modules/
│   ├── azure-resources/       # Core Azure infra (VNet, ADLS, KV)
│   ├── databricks-workspace/  # Workspace + Unity Catalog + RBAC
│   └── databricks-cluster/    # Cluster definitions with policies
├── environments/
│   ├── dev/                   # Single-node, spot instances
│   └── prod/                  # GRD storage, larger clusters, tighter NSG
├── main.tf                    # Root — orchestrates modules
├── providers.tf               # Provider versions + backend config
├── variables.tf               # All input variables
└── outputs.tf                 # Workspace URL, KV URI, SP client ID
```

### Module Dependencies

```
azure-resources (VNet, ADLS, KV)
        │
        ▼
databricks-workspace (Workspace, UC Metastore, Catalogs)
        │
        ▼
databricks-cluster (Clusters, Libraries, Policies)
```

## Unity Catalog Design

### Catalog Hierarchy

- **Bronze**: Raw ingested data — append-only, partitioned by date, full audit trail
- **Silver**: Cleansed data — deduplicated, validated, PII-masked, incremental loads
- **Gold**: Business aggregates — table materialization, optimized for BI queries

### Access Control

| Principal | Bronze | Silver | Gold |
|-----------|--------|--------|------|
| **Data Engineers** | ALL_PRIVILEGES | ALL_PRIVILEGES | ALL_PRIVILEGES |
| **Analysts** | USE_CATALOG, SELECT | USE_CATALOG, SELECT | USE_CATALOG, SELECT |
| **Data Scientists** | USE_CATALOG, SELECT | USE_CATALOG, SELECT | USE_CATALOG, SELECT |
| **Service Principal** | ALL_PRIVILEGES | ALL_PRIVILEGES | ALL_PRIVILEGES |

### PII Flow

```
Bronze: customer_email = "john@example.com"        (raw)
  │
  ▼ (SHA-256 hash with salt)
Silver: customer_email_hashed = "a1b2c3d4..."      (pseudonymized)
  │
  ▼ (column dropped)
Gold:   (no PII columns exist)                     (GDPR compliant)
```

## dbt Architecture

### Model Lineage

```
Sources (landing zone files)
  │
  ▼
stg_sales_transactions (bronze — view)
  │
  ▼
sales_transactions_cleaned (silver — incremental, merge)
  │
  ▼
daily_sales_summary (gold — table, partitioned)
customer_360 (gold — table)
```

### Incremental Strategy

Silver models use `MERGE` with `unique_key`:
```sql
{{ config(
    materialized='incremental',
    unique_key='surrogate_key',
    on_schema_change='sync_all_columns'
) }}
```

### Schema Routing

dbt tags determine Unity Catalog placement:
- `tags: ["bronze"]` → `bronze` catalog
- `tags: ["silver"]` → `silver` catalog
- `tags: ["gold"]` → `gold` catalog

Custom macro `generate_schema_name` routes schemas automatically.

## Pipeline Runtime

### Cluster Strategy

| Cluster | Purpose | Node Type | Workers | Cost Profile |
|---------|---------|-----------|---------|-------------|
| All-Purpose | Development | Standard_DS3_v2 | 2-10 | $$$ |
| Jobs | Pipeline runs | Standard_DS3_v2 | 2-10 | $$ |
| Spot | Batch workloads | Standard_DS3_v2 | 2-10 | $ |

### Medallion Workflow Execution

```
06:00 UTC — Cron triggers
  │
  ▼ (3600s timeout)
Bronze Ingestion — Auto Loader reads landing zone, writes to UC
  │
  ▼ (3600s timeout)
Silver Transformation — dbt run --select silver.*
  │
  ▼ (3600s timeout)
Gold Aggregation — dbt run --select gold.*
  │
  ├─────────────────────────┐
  ▼                         ▼
Data Quality (1800s)       dbt Tests (1800s)
  │
  ▼
✅ All Passed → Complete
❌ Any Failed → Email Alert
```

## Cost Optimization

1. **Spot Instances**: 60-80% cheaper for batch workloads (configurable max bid)
2. **Auto-Termination**: Clusters shut down after 30 minutes idle
3. **Autoscaling**: Scale down to 1 worker during off-peak
4. **SQL Warehouse**: Auto-stop after 10-30 minutes
5. **Storage**: ADLS Gen2 Cool tier for bronze layer (infrequently accessed)
6. **Incremental Loads**: Process only new data, not full refreshes

## Disaster Recovery

- **Storage**: GRS replication in prod, LRS in dev
- **State**: Terraform state in Azure Storage with versioning
- **Secrets**: Key Vault soft delete (7-day retention)
- **dbt**: Models in Git — full rebuild capability
- **Delta**: Time travel for rollback (30-day default retention)

## Future Enhancements

- [ ] Delta Sharing for external data consumers
- [ ] Databricks Model Serving for ML inference
- [ ] Azure Event Hubs → Structured Streaming for real-time
- [ ] Databricks Asset Bundles (DABs) for CI/CD deployment
- [ ] Great Expectations integration for schema validation
- [ ] dbt Elementary for data observability
- [ ] Azure Private Link for VNet → ADLS private connectivity
