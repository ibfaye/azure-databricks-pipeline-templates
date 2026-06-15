# Getting Started Guide

## Pre-flight Checklist

- [ ] Azure subscription with **Contributor** role
- [ ] Databricks account (Premium tier for Unity Catalog)
- [ ] Terraform ≥ 1.5.0 installed
- [ ] Azure CLI logged in
- [ ] Sufficient vCPU quota in your target region (or use `eastus`/`westeurope`)

## Step-by-Step

### 1. Clone & Configure

```bash
git clone https://github.com/ibfaye/azure-databricks-pipeline-templates.git
cd azure-databricks-pipeline-templates/terraform
cp terraform.tfvars.example terraform.tfvars
```

Edit `terraform.tfvars`:

```hcl
azure_subscription_id   = "11111111-2222-3333-4444-555555555555"
azure_tenant_id         = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
databricks_account_id   = "ffffffff-gggg-hhhh-iiii-jjjjjjjjjjjj"  # UUID from accounts.azuredatabricks.net, NOT workspace ID
project_name            = "my-client-project"
environment             = "dev"
location                = "eastus"  # or westeurope — choose a region with free quota
```

### 2. Authenticate

```bash
az login --tenant <tenant-id> --use-device-code
az account set --subscription "<subscription-id>"
```

### 3. Phase 1 — Core Infrastructure

Terraform deploys in two phases to avoid a chicken-and-egg problem with the Databricks provider.

```bash
terraform init

# Phase 1: Azure resources + Databricks workspace (deploy_workspace_resources defaults to false)
terraform apply
```

**What Phase 1 creates (~12 min):**

| Time | Resource |
|------|----------|
| 0-2 min | Resource Group, VNet, Subnets, NSG |
| 2-5 min | ADLS Gen2 (7 containers), Key Vault |
| 5-12 min | Databricks Workspace (Premium, VNet-injected) |

### 4. Create Unity Catalog (30 seconds)

Unity Catalog catalogs require account admin for external locations. Create them via the Databricks UI:

1. Open your workspace at the URL from `terraform output databricks_workspace_url`
2. **Catalog** → **Create Catalog** → Name: `bronze` → select storage → Create
3. Repeat for `silver` and `gold`
4. In **SQL Editor**, run:
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

### 5. Phase 2 — Clusters & Workflows

Add to `terraform.tfvars`:
```hcl
deploy_workspace_resources = true
```

Then:
```bash
terraform apply
```

Creates: all-purpose cluster, job cluster, medallion pipeline workflow (daily 6 AM UTC), SQL warehouse.

### 6. Verify Deployment

```bash
terraform output

# Example:
# databricks_workspace_url = "https://adb-123456789.0.azuredatabricks.net"
# databricks_workspace_id  = "123456789"
# storage_account_name     = "stdevdatabricksdl"
# key_vault_uri            = "https://kv-dev-dbx-abc123.vault.azure.net/"
```

### 7. Configure dbt (optional — for local development)

```bash
cd ../dbt
cp profiles.yml.example profiles.yml

export DATABRICKS_HOST=$(terraform -chdir=../terraform output -raw databricks_workspace_url)

# Create a Personal Access Token in Databricks:
# Workspace → User Settings → Developer → Access Tokens → Generate
export DATABRICKS_TOKEN="dapi..."

dbt deps
dbt compile --target dev
```

## Known Issues & Workarounds

See [`DEPLOY.md`](../DEPLOY.md) for the full list. Quick reference:

| Symptom | Fix |
|---------|-----|
| Azure Quota Exceeded | Change region to `eastus`/`westeurope`, or `autoscale_max_workers = 1` |
| Metastore limit reached | Workspace auto-assigns — no action needed |
| Storage credential "account admin required" | Use Databricks UI (step 4 above) |
| Key Vault access denied | Module now creates access policy for deployer |
| NSG NetworkIntentPolicy conflict | NSG is created empty — Databricks manages rules |
| Workspace "unauthorized network access" | Dev has public endpoint (`no_public_ip` only for prod) |

## Troubleshooting

### Destroy is stuck / RG won't delete

If `terraform destroy` fails with "Resource Group still contains Resources", delete the RG manually from the Azure Portal, then:

```bash
rm terraform.tfstate terraform.tfstate.backup
rm -rf .terraform
terraform init
```

### dbt "Catalog not found"

Catalogs must be created via UI (step 4). Terraform cannot create them without account admin.

### Pipeline timeouts

Increase `timeout_seconds` in `pipelines/workflows/medallion_pipeline.yml` or reduce `autoscale_max_workers`.

## Next Steps

1. **Load sample data** → Copy test CSVs to `abfss://landing@<storage-account>.dfs.core.windows.net/`
2. **Customize dbt models** → Edit `dbt/models/gold/` for your business logic
3. **Set up BI** → Connect Power BI/Tableau to the SQL Warehouse
4. **Add CI/CD** → Configure GitHub Actions secrets (`DATABRICKS_HOST`, `DATABRICKS_TOKEN`)
5. **Enable monitoring** → Azure Monitor alerts on the resource group
