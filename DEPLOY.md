# Deployment Guide

## Prerequisites

- Azure subscription with Contributor role
- Databricks account (Premium tier for Unity Catalog)
- Terraform ≥ 1.5.0
- Azure CLI logged in

## Quick Start

```bash
cd terraform
cp terraform.tfvars.example terraform.tfvars
# Edit tfvars with your values

az login --tenant <tenant-id> --use-device-code
terraform init
terraform apply
```

## Two-Phase Deployment

Phase 1 (default): Core Azure infrastructure + Databricks workspace
Phase 2: Clusters, jobs, SQL warehouse

```bash
# Phase 1
terraform apply  # deploy_workspace_resources defaults to false

# Phase 2 — after workspace is created and reachable
# Add to terraform.tfvars: deploy_workspace_resources = true
terraform apply
```

## Post-Deployment: Unity Catalog

Catalogs and schemas must be created manually (account admin required for external locations).

1. Go to your Databricks workspace → Catalog
2. Create 3 catalogs: `bronze`, `silver`, `gold` (use "Default Storage")
3. Run in SQL Editor:
```sql
CREATE SCHEMA IF NOT EXISTS bronze.sales;
CREATE SCHEMA IF NOT EXISTS bronze.customers;
CREATE SCHEMA IF NOT EXISTS bronze.operations;
CREATE SCHEMA IF NOT EXISTS bronze.iot;
CREATE SCHEMA IF NOT EXISTS silver.sales;
CREATE SCHEMA IF NOT EXISTS silver.customers;
CREATE SCHEMA IF NOT EXISTS silver.operations;
CREATE SCHEMA IF NOT EXISTS silver.iot;
CREATE SCHEMA IF NOT EXISTS gold.sales;
CREATE SCHEMA IF NOT EXISTS gold.customers;
CREATE SCHEMA IF NOT EXISTS gold.operations;
CREATE SCHEMA IF NOT EXISTS gold.iot;
```

## Known Issues & Workarounds

### 1. Azure Quota Exceeded

**Symptom:** `Azure Quota Exceeded Exception` during cluster creation.

**Fixes (in order of speed):**
- Change region (`location = "eastus"` or `"westeurope"` usually have quota)
- Reduce workers: `autoscale_min_workers = 0`, `autoscale_max_workers = 1`
- Request quota increase: https://aka.ms/ProdportalCRP → `Standard DSv3 Family vCPUs`

### 2. "workspace_id required" Error

**Symptom:** Phase 2 resources fail with `managing workspace-level resources requires a workspace_id`.

**Fix:** Ensure `phase2.tf` resources have `provider = databricks.workspace` and `providers.tf` uses `azure_workspace_resource_id` (Azure resource ID, not numeric ID).

### 3. Metastore Limit Reached

**Symptom:** `account has reached the limit for metastores in region`.

**Fix:** Workspace auto-assigns to the region's existing metastore. No Terraform resource needed. The module already handles this.

### 4. Storage Credential Requires Account Admin

**Symptom:** `Only the account admin can create or update a storage credential`.

**Fix:** Catalogs use managed storage (created via Databricks UI). Storage credentials and external locations are commented out in the module. The deprecated `dbt_snow_mask` package has been removed.

### 5. Key Vault Access Denied

**Symptom:** `does not have secrets get permission on key vault`.

**Fix:** The module now creates an access policy granting the deploying user `Get, List, Set, Delete` on secrets.

### 6. NSG Network Intent Policy Conflicts

**Symptom:** `Found conflicts with NetworkIntentPolicy`.

**Fix:** The NSG is created empty. Databricks auto-provisions required rules via Network Intent Policies. Do not define security rules in the NSG.

### 7. `versioning_enabled` + `is_hns_enabled` Incompatible

**Symptom:** `versioning_enabled can't be true when is_hns_enabled is true`.

**Fix:** ADLS Gen2 (HNS-enabled) doesn't support blob versioning. Set to `false`. The module is already fixed.

### 8. Workspace Unreachable (no_public_ip)

**Symptom:** `Unauthorized network access to workspace` during Terraform apply.

**Fix:** `no_public_ip` is set to `true` only for production (`var.environment == "prod"`). Dev/staging have public endpoints so Terraform can provision UC resources from your local machine.

### 9. CRLF Line Endings (Windows/WSL)

**Symptom:** `terraform fmt` shows every line changed, `Invalid character` errors.

**Fix:** `.gitattributes` enforces LF line endings. Added to both repos.

### 10. Deprecated Azure Provider Attributes

- `storage_account_name` → `storage_account_id` (fixed)
- `spark.databricks.delta.preview.enabled` → removed (Delta is GA)

## Cost Notes

| Scenario | Monthly Estimate |
|----------|-----------------|
| Idle (deployed, nothing running) | $2-3 |
| Dev (1 pipeline run/day + 2h interactive) | $80-150 |
| Prod (24/7 clusters + hourly pipeline) | $800-2,000 |

Auto-termination: Clusters shut down after 30 min idle. SQL warehouse auto-stops after 10 min.

## CI/CD

- **Terraform:** `terraform fmt -check`, `init -backend=false`, `validate`
- **dbt:** `dbt compile --target ci` against live workspace (uses GitHub secrets)
- **tflint:** Linting with `terraform` + `azurerm` plugins

Both workflows use path filters — only trigger on changes to their respective directories.
