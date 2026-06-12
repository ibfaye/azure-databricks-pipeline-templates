# Getting Started Guide

## Pre-flight Checklist

Before deploying, ensure you have:

- [ ] Azure subscription with **Contributor** role
- [ ] Databricks account (Premium tier required for Unity Catalog)
- [ ] Terraform ≥ 1.5.0 installed
- [ ] Azure CLI logged in (`az login`)
- [ ] A coffee ☕ (deployment takes ~15 minutes)

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
databricks_account_id   = "ffffffff-gggg-hhhh-iiii-jjjjjjjjjjjj"
project_name            = "my-client-project"
environment             = "dev"
location                = "westeurope"  # or northeurope, eastus, etc.
```

### 2. Authenticate

```bash
# Azure CLI
az login
az account set --subscription "$(grep azure_subscription_id terraform.tfvars | cut -d'"' -f2)"

# Set environment variables for Terraform
export ARM_SUBSCRIPTION_ID=$(grep azure_subscription_id terraform.tfvars | cut -d'"' -f2)
export ARM_TENANT_ID=$(grep azure_tenant_id terraform.tfvars | cut -d'"' -f2)
```

### 3. Deploy Infrastructure

```bash
terraform init
terraform plan -out=tfplan
terraform apply tfplan
```

**What happens during apply:**

| Time | Resource |
|------|----------|
| 0-2 min | Resource Group, VNet, Subnets |
| 2-5 min | ADLS Gen2, Key Vault, NSG |
| 5-10 min | Databricks Workspace |
| 10-15 min | Unity Catalog, Service Principal, Grants, SQL Warehouse |

### 4. Verify Deployment

```bash
# Get outputs
terraform output

# Example output:
# databricks_workspace_url = "https://adb-123456789.azuredatabricks.net"
# storage_account_name    = "stdevdatabricksdl"
# key_vault_uri           = "https://kv-dev-dbx-abc123.vault.azure.net/"
# sql_warehouse_id        = "abc123def456"
```

### 5. Configure dbt

```bash
cd ../dbt
cp profiles.yml.example profiles.yml

# Get the workspace URL from Terraform
export DATABRICKS_HOST=$(terraform -chdir=../terraform output -raw databricks_workspace_url)

# Create a Personal Access Token in Databricks
# Workspace → User Settings → Developer → Access Tokens → Generate
export DATABRICKS_TOKEN="dapi..."
export DATABRICKS_HTTP_PATH="/sql/1.0/warehouses/$(terraform -chdir=../terraform output -raw sql_warehouse_id)"
```

### 6. Run dbt

```bash
# Install dbt packages
dbt deps

# Validate models compile
dbt compile

# Run all models (dev target)
dbt run --target dev

# Run tests
dbt test --target dev
```

### 7. Trigger the Pipeline

The Medallion pipeline is scheduled daily at 6 AM UTC. To run it immediately:

```bash
# Via Databricks CLI
databricks jobs run-now --job-id $(databricks jobs list --name "dbx-pipeline-dev-medallion" --output json | jq -r '.[0].job_id')
```

Or via the Databricks UI: **Workflows → Jobs → dbx-pipeline-dev-medallion → Run Now**

### 8. Verify Data

Open your Databricks workspace and run:

```sql
-- Check bronze tables
SELECT COUNT(*) FROM bronze.sales.raw_sales_transactions;

-- Check silver tables
SELECT COUNT(*) FROM silver.sales.sales_transactions_cleaned;

-- Check gold tables
SELECT * FROM gold.sales.daily_sales_summary ORDER BY transaction_date DESC LIMIT 10;

-- Customer 360 segments
SELECT customer_segment, COUNT(*) as cnt
FROM gold.customers.customer_360
GROUP BY customer_segment
ORDER BY cnt DESC;
```

## Environment Promotion

### Dev → Staging → Prod

```bash
# Dev environment
cd terraform/environments/dev
terraform init && terraform apply

# Staging (non-prod but realistic)
cd ../staging
terraform init && terraform apply

# Production (with GRS storage, bigger clusters)
cd ../prod
terraform init && terraform apply
```

### Key Differences Between Environments

| Setting | Dev | Staging | Prod |
|---------|-----|---------|------|
| Storage Replication | LRS | GRS | GRS |
| Cluster Workers | 1-5 | 2-10 | 5-20 |
| SQL Warehouse | X-Small | Small | 2X-Small |
| Auto-stop | 10 min | 20 min | 30 min |
| Key Vault SKU | Standard | Standard | Premium |
| Spot Instances | Yes | Optional | No |

## Troubleshooting

### "403 Forbidden" on Storage

The service principal needs `Storage Blob Data Contributor` on the ADLS account. Wait for the RBAC propagation (~5 minutes) or run:

```bash
terraform apply -target=azurerm_role_assignment.databricks_storage
```

### "Cannot create metastore"

Ensure your Databricks account has Unity Catalog enabled and the workspace is Premium tier.

### dbt "Catalog not found"

```sql
-- Manually create catalogs if Terraform didn't
CREATE CATALOG IF NOT EXISTS bronze;
CREATE CATALOG IF NOT EXISTS silver;
CREATE CATALOG IF NOT EXISTS gold;
```

### Pipeline timeouts

Increase `timeout_seconds` in the workflow YAML or reduce `autoscale_max_workers`.

## Next Steps

1. **Load sample data** → Copy test CSVs to `abfss://landing@<storage-account>.dfs.core.windows.net/`
2. **Customize dbt models** → Edit `dbt/models/gold/` to add your business logic
3. **Set up BI** → Connect Power BI or Tableau to the SQL Warehouse
4. **Add CI/CD** → Configure GitHub Actions secrets for production deployments
5. **Enable monitoring** → Set up Azure Monitor alerts on the resource group
