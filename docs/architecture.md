# Architecture Deep Dive

## Principles

1. **Infrastructure as Code**: Terraform modules for Azure + Databricks — minimal manual steps
2. **Medallion Architecture**: Bronze (raw) → Silver (validated) → Gold (business-ready)
3. **Unity Catalog-First**: All data lives in UC catalogs — no `hive_metastore`
4. **dbt as Transformation Engine**: SQL-based, version-controlled, testable transformations
5. **Security by Default**: VNet isolation, PII masking, least-privilege RBAC

## Two-Phase Deployment

Terraform cannot create workspace-level resources until the workspace exists (chicken-and-egg problem with the Databricks provider). The template handles this with two files:

```
terraform/
├── main.tf         # Phase 1: Azure infra + Databricks workspace
├── phase2.tf       # Phase 2: Clusters, jobs, SQL warehouse (gated by deploy_workspace_resources flag)
├── providers.tf    # Provider config with ARM SP auth for Databricks
├── variables.tf    # All variables including deploy_workspace_resources (default: false)
└── outputs.tf      # Workspace URL, KV URI, SP client ID
```

**Phase 1** (`terraform apply`): Resource Group, VNet, subnets, NSG (empty), ADLS Gen2 (7 containers), Key Vault, Databricks workspace.

**Phase 2** (`deploy_workspace_resources = true` then `terraform apply`): All-purpose cluster, job cluster, spot cluster, single-node cluster, medallion pipeline workflow, SQL warehouse.

## Terraform Module Design

```
terraform/
├── modules/
│   ├── azure-resources/       # VNet, ADLS, KV, NSG, NSG associations
│   ├── databricks-workspace/  # Workspace, service principal
│   └── databricks-cluster/    # All-purpose, jobs, spot, single-node clusters
├── main.tf                    # Phase 1 — azure + databricks modules
├── phase2.tf                  # Phase 2 — clusters + jobs + SQL warehouse
├── providers.tf               # azurerm, databricks, azuread, random, time
├── variables.tf               # Input variables
└── outputs.tf                 # Workspace URL, KV URI, SP client ID
```

### Module Dependencies

```
azure-resources (VNet, ADLS, KV, NSG associations)
        │
        ▼
databricks-workspace (Workspace, service principal)
        │ (after phase 1 apply)
        ▼
databricks-cluster (Clusters, libraries)    ← in phase2.tf
databricks_job (Medallion pipeline)         ← in phase2.tf
databricks_sql_endpoint (SQL warehouse)     ← in phase2.tf
```

## Network Security

### NSG + Network Intent Policies

Databricks VNet-injected workspaces auto-generate **Network Intent Policies** (NIPs) that define required security rules. Defining rules in Terraform's NSG causes `ConflictWithNetworkIntentPolicy` errors.

**Solution:** The NSG is created **empty**. Databricks auto-provisions all required rules via NIPs. Both public and private subnets are associated with this NSG.

Required NIP rules (auto-managed):
- Inbound: AzureDatabricks → VirtualNetwork (443, 22, 5557)
- Outbound: VirtualNetwork → Storage:443, Sql:3306, EventHub:9093, AzureDatabricks:443

### Public Access

- **Dev/Staging**: `no_public_ip = false` — workspace has public endpoint (Terraform can reach it from anywhere)
- **Production**: `no_public_ip = true` — fully private, requires VPN/Private Link

## Unity Catalog Design

### Catalog Hierarchy

Created via Databricks UI (account admin required for managed storage):
- **Bronze**: Raw ingested data — append-only, partitioned by date
- **Silver**: Cleansed data — deduplicated, validated, PII-masked
- **Gold**: Business aggregates — optimized for BI queries

### Why not Terraform?

Unity Catalog catalogs require managed storage or external locations. Both require **Databricks account admin** privileges. The template provides a SQL script in the workspace module comments for post-deployment setup.

### Access Control

| Principal | Bronze | Silver | Gold |
|-----------|--------|--------|------|
| Data Engineers | ALL_PRIVILEGES | ALL_PRIVILEGES | ALL_PRIVILEGES |
| Analysts / Scientists | USE_CATALOG, SELECT | USE_CATALOG, SELECT | USE_CATALOG, SELECT |

### PII Flow

```
Bronze: customer_email = "john@example.com"        (raw)
  │
  ▼ (SHA-256 hash with DBT_PII_SALT env var)
Silver: customer_email_hashed = "a1b2c3d4..."      (pseudonymized)
  │
  ▼ (column dropped)
Gold:   (no PII columns exist)                     (GDPR compliant)
```

## Databricks Provider Authentication

The workspace-level provider uses Azure ARM service principal authentication:

```hcl
provider "databricks" {
  alias                       = "workspace"
  host                        = module.databricks.workspace_url
  azure_workspace_resource_id = module.databricks.workspace_resource_id  # Azure resource ID, not numeric ID
  azure_tenant_id             = module.azure.tenant_id
}
```

This inherits `ARM_CLIENT_ID`/`ARM_CLIENT_SECRET` from the `azurerm` provider — no PAT tokens needed.

## dbt Architecture

### Model Lineage

```
Sources (landing zone parquet files)
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
    on_schema_change='append_new_columns'
) }}
```

### Schema Routing

dbt tags determine Unity Catalog placement:
- `tags: ["bronze"]` → `bronze` catalog
- `tags: ["silver"]` → `silver` catalog
- `tags: ["gold"]` → `gold` catalog

## Cost Optimization

1. **Spot instances** — 60-80% cheaper for batch workloads
2. **Auto-termination** — Clusters shut down after 30 min idle
3. **Autoscaling** — Scale to 1 worker during off-peak
4. **SQL warehouse auto-stop** — 10-30 min
5. **ADLS Gen2** — LRS in dev, GRS in prod
6. **Job clusters** — Ephemeral, spin up/down per pipeline run

## Disaster Recovery

- **Storage**: LRS (dev) / GRS (prod)
- **State**: Local `.tfstate` (dev) / Azure Storage backend (prod — uncomment in providers.tf)
- **Secrets**: Key Vault soft delete (90-day retention)
- **dbt**: Models in Git — full rebuild capability
- **Delta**: Time travel (30-day default retention)

## Known Limitations

| Limitation | Workaround |
|-----------|------------|
| UC catalogs need account admin | Create via Databricks UI (30 seconds) |
| Storage credentials need account admin | Use managed storage (UI-created catalogs) |
| Metastore: 1 per region | Workspace auto-assigns to existing |
| NSG rules conflict with NIPs | Empty NSG — Databricks manages rules |
| `versioning_enabled` incompatible with ADLS Gen2 | Disabled (Delta Lake handles versioning) |
| Provider can't auth before workspace exists | Two-phase deploy with feature flag |
