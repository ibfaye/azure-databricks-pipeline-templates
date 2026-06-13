# Module 1: Foundation — Cloud Infrastructure & Identity

> **Prerequisite module.** Establishes the secure Azure sandbox onto which every subsequent module depends. You will deploy the exact infrastructure the `azure-databricks-pipeline-templates` repository expects, and understand *why* each resource exists before you write a single line of pipeline code.

---

## 1. Learning Objectives

By the end of this module, the learner will:

| # | Conceptual | Practical |
|---|-----------|-----------|
| 1 | Understand the Azure resource hierarchy (Management Groups → Subscriptions → Resource Groups → Resources) and why the repo co-locates all pipeline assets in one resource group | Create and lock down a resource group using both the Azure Portal and `az` CLI |
| 2 | Internalize the Azure networking model: Virtual Networks, subnets, NSGs, service delegation, and why Databricks requires **two** subnets (public + private) even for private-link deployments | Deploy a VNet with correct `Microsoft.Databricks/workspaces` subnet delegation using the exact Terraform module structure from the repo |
| 3 | Master the three Azure identity primitives — Service Principals, Managed Identities, and User Assigned Managed Identities — and when each is appropriate | Create an Entra ID Service Principal, rotate its secret on a 90-day lifecycle, and register it inside Databricks via `databricks_service_principal` |
| 4 | Comprehend Azure RBAC: scope, role definitions, assignments, and the critical distinction between Control Plane (ARM) and Data Plane (ADLS) permissions | Grant `Storage Blob Data Contributor` at the storage account scope using `azurerm_role_assignment` — the exact binding that lets Databricks notebooks read/write ADLS data |
| 5 | Understand secrets lifecycle: why Key Vault exists, RBAC vs. access policy authentication, soft-delete mechanics, and how Databricks Secret Scopes bridge the two platforms | Deploy a Key Vault, store the ADLS access key, and mount a Databricks Secret Scope backed by that vault — the foundation for the repo's `PipelineConfig._get_secret()` method |

---

## 2. Theoretical Foundations

### 2.1 The Azure Resource Hierarchy & Why It Matters

The repository assumes a single resource group containing **everything** — networking, storage, Key Vault, Databricks workspace. This is not arbitrary; it reflects a deliberate design choice:

**`terraform/modules/azure-resources/main.tf` — the source of truth:**
```hcl
resource "azurerm_resource_group" "main" {
  name     = var.resource_group_name    # e.g., "rg-dbx-pipeline-dev"
  location = var.location               # e.g., "westeurope"
  tags     = var.tags
}
```

**Why co-location?** In a medallion architecture, the Bronze layer (ADLS containers) and the compute layer (Databricks workspace) share a tight dependency. Co-locating them in one resource group simplifies:

- **Lifecycle management:** `terraform destroy` tears down everything atomically.
- **RBAC scope:** Role assignments at the resource group level cascade to all children.
- **Cost attribution:** All pipeline resources share the same `cost_center` tag.

**System constraint:** Resource groups are **regional containers**. Resources inside them can span regions, but the RG metadata itself lives in one region. The repo defaults to `westeurope` — pick the region closest to your data sources to minimize egress costs.

### 2.2 Virtual Networks & Databricks Subnet Delegation

Databricks compute (clusters) runs in **your** Azure subscription, not in Databricks'. This is the "VNet injection" model — your VNet hosts the cluster VMs. The repo implements the canonical two-subnet pattern:

**`terraform/modules/azure-resources/main.tf` — subnet architecture:**
```hcl
resource "azurerm_virtual_network" "main" {
  name                = "vnet-${var.environment}-databricks"
  address_space       = var.vnet_address_space   # e.g., ["10.0.0.0/16"]
}

resource "azurerm_subnet" "public" {
  name             = "snet-${var.environment}-public"
  address_prefixes = var.subnet_prefixes.public  # e.g., ["10.0.0.0/22"]
  delegation {
    name = "databricks-delegation"
    service_delegation {
      name    = "Microsoft.Databricks/workspaces"
    }
  }
}

resource "azurerm_subnet" "private" {
  name             = "snet-${var.environment}-private"
  address_prefixes = var.subnet_prefixes.private # e.g., ["10.0.4.0/22"]
  delegation {
    name = "databricks-delegation"
    service_delegation {
      name    = "Microsoft.Databricks/workspaces"
    }
  }
}
```

**Architectural rationale — why TWO subnets?**

- **Public subnet:** Hosts cluster nodes that need outbound internet access (driver node pulling PyPI packages, Spark libraries). The NSG allows `AzureDatabricks` service tag on port 443 for control-plane communication.
- **Private subnet:** Hosts worker nodes for inter-cluster communication. In private-link deployments (the repo's `no_public_ip = true` path), all nodes go here.
- **The delegation is NOT optional:** Without `Microsoft.Databricks/workspaces` delegation, Databricks cannot attach network interfaces to the subnet. The error is obscure: `"Subnet does not have delegation"` at workspace creation time.

**NSG rule semantics (the two inbound rules in the repo):**
```hcl
security_rule {
  name                       = "AllowDatabricksControlPlane"
  priority                   = 100
  direction                  = "Inbound"
  access                     = "Allow"
  protocol                   = "Tcp"
  destination_port_range     = "443"
  source_address_prefix      = "AzureDatabricks"  # ← service tag, not IP
  destination_address_prefix = "*"
}
```

This is NOT opening your cluster to the internet. The `AzureDatabricks` service tag is a Microsoft-managed IP list of Databricks control-plane infrastructure. These rules are mandatory — Databricks validates their presence during workspace creation.

### 2.3 Identity: Service Principals, Managed Identities, and the Databricks Bridge

The repo creates **three distinct identity objects** that serve different roles:

| Identity | Azure Resource | Databricks Equivalent | Purpose |
|----------|---------------|----------------------|---------|
| **Service Principal** | `azuread_application` + `azuread_service_principal` | `databricks_service_principal` | Pipeline execution identity — the "machine user" that notebooks and jobs run as |
| **Managed Identity** | Implicit to Databricks workspace | N/A | Databricks control plane authenticates to Azure for workspace management |
| **User identity** | Entra ID user account | Databricks user (SSO) | Human developers and admins |

**The critical RBAC binding — `Storage Blob Data Contributor`:**
```hcl
resource "azurerm_role_assignment" "databricks_storage" {
  scope                = var.storage_account_id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azuread_service_principal.databricks_sp.object_id
}
```

**Why this specific role?** The repo's readers and writers (`DataLakeReader.read_parquet()`, `DeltaWriter.write()`) use `abfss://` URIs that authenticate via the cluster's attached service principal. `Storage Blob Data Contributor` grants:

- Read, write, delete on blobs and containers
- **NOT** management-plane operations (listing storage accounts, rotating keys)
- This is least-privilege: the pipeline can read/write data but cannot reconfigure storage

**The secret rotation pattern:**
```hcl
resource "time_rotating" "sp_secret" {
  rotation_days = 90
}

resource "azuread_service_principal_password" "databricks_sp" {
  service_principal_id = azuread_service_principal.databricks_sp.id
  lifecycle {
    replace_triggered_by = [time_rotating.sp_secret.id]
  }
}
```

Every 90 days, Terraform's `time_rotating` resource changes its `id`, which triggers `replace_triggered_by` on the password, which forces Terraform to generate a new secret. This is automatic credential rotation — no cron jobs, no manual intervention.

### 2.4 Key Vault & Databricks Secret Scopes

The repo uses **two layers** of secrets management:

**Layer 1: Azure Key Vault** — stores infrastructure secrets:
```hcl
resource "azurerm_key_vault" "main" {
  name                       = "kv-${var.environment}-dbx-${random_string.suffix.result}"
  sku_name                   = "standard"
  soft_delete_retention_days = 90
}

resource "azurerm_key_vault_secret" "adls_key" {
  name         = "adls-access-key"
  value        = azurerm_storage_account.datalake.primary_access_key
  key_vault_id = azurerm_key_vault.main.id
}
```

**Layer 2: Databricks Secret Scope** — bridges Key Vault into the notebook runtime:
```python
# From pipelines/src/config.py — this is what the notebooks actually call:
@staticmethod
def _get_secret(secret_name: str, scope: str = "pipeline-secrets") -> str:
    try:
        return dbutils.secrets.get(scope, secret_name)
    except Exception:
        return os.getenv(secret_name.upper().replace("-", "_"), "")
```

The `scope` is a Databricks construct that maps 1:1 to either a Key Vault instance (AKV-backed) or a Databricks-managed scope. The repo's convention is `"pipeline-secrets"` for AKV-backed scopes, with environment-variable fallback for local development.

**Key design decision:** The repo reads the storage account name from secrets, not from hardcoded config. This means the same notebook code works across dev/staging/prod — only the secret scope contents differ.

---

## 3. Hands-on Execution

### 3.1 Prerequisites Check

Before running a single `terraform apply`, verify:

```bash
# 1. Azure CLI authenticated
az account show
# Expected: { "user": {"name": "you@domain.com"}, "tenantId": "..." }

# 2. Terraform installed (≥ 1.5.0 per the repo's providers.tf)
terraform version
# Expected: Terraform v1.7.x or later

# 3. GitHub CLI (optional, for CI workflow testing)
gh auth status

# 4. Databricks CLI (optional, for workspace operations)
databricks --version
```

### 3.2 Step 1: Fork & Clone the Repository

```bash
# From ~/workspace
gh repo fork ibfaye/azure-databricks-pipeline-templates --clone
cd azure-databricks-pipeline-templates
```

**⚠️ Pre-flight check:** Open `terraform/providers.tf` and **comment out the backend block** for initial deployment (you don't have the state storage account yet):

```hcl
# terraform/providers.tf — lines 27-33
# Temporarily comment for initial deployment:
# backend "azurerm" {
#   resource_group_name  = "rg-terraform-state"
#   storage_account_name = "stterraformstate"
#   container_name       = "tfstate"
#   key                  = "databricks-pipeline.tfstate"
# }
```

### 3.3 Step 2: Create the Terraform State Storage Account

You need a place to store `terraform.tfstate` before Terraform can manage any resources:

```bash
az group create \
  --name rg-terraform-state \
  --location westeurope

az storage account create \
  --name stterraformstate$(openssl rand -hex 4) \
  --resource-group rg-terraform-state \
  --location westeurope \
  --sku Standard_LRS \
  --kind StorageV2 \
  --hierarchical-namespace true

# Get the actual name (with random suffix)
az storage account list \
  --resource-group rg-terraform-state \
  --query "[].name" -o tsv
# Copy this value — you'll need it for the backend block

az storage container create \
  --name tfstate \
  --account-name <the-actual-name-from-above>
```

Now **uncomment** the backend block in `providers.tf` and update the values with your actual resource names.

### 3.4 Step 3: Create the Terraform Variables File

Create `terraform/terraform.tfvars`:

```hcl
# terraform/terraform.tfvars
azure_subscription_id = "00000000-0000-0000-0000-000000000000"  # Replace
azure_tenant_id       = "00000000-0000-0000-0000-000000000000"  # Replace
databricks_account_id = "00000000-0000-0000-0000-000000000000"  # Replace

project_name = "dbx-pipeline"
environment  = "dev"
location     = "westeurope"

databricks_workspace_sku = "premium"      # Required for Unity Catalog
dbr_version              = "14.3.x-scala2.12"
cluster_node_type        = "Standard_DS3_v2"
autoscale_min_workers    = 2
autoscale_max_workers    = 10

tags = {
  cost_center = "data-engineering"
  project     = "medallion-pipeline"
  owner       = "data-team"
}
```

> **⚠️ Never commit `terraform.tfvars` or `.tfstate` files.** The repo's `.gitignore` already excludes them — verify:
> ```bash
> git check-ignore terraform/terraform.tfvars
> # Should output: terraform/terraform.tfvars
> ```

### 3.5 Step 4: Deploy Azure Infrastructure

```bash
cd terraform

# Initialize with backend
terraform init
# Expected: "Terraform has been successfully initialized!"

# Format check (CI enforces this)
terraform fmt -recursive -check

# Validate syntax and provider references
terraform validate
# Expected: "Success! The configuration is valid."

# Plan — review EVERY resource before applying
terraform plan -out=tfplan

# Apply
terraform apply tfplan
# ⏱ ~15-20 minutes for full deployment (Databricks workspace is the bottleneck)
```

**What gets created (in order):**

1. `azurerm_resource_group.main` — `rg-dbx-pipeline-dev`
2. `azurerm_virtual_network.main` + public/private subnets with delegation
3. `azurerm_network_security_group.main` with Databricks rules
4. `azurerm_storage_account.datalake` — ADLS Gen2 with HNS enabled
5. 6 storage containers: `bronze`, `silver`, `gold`, `landing`, `checkpoint`, `dev-metastore`
6. `azurerm_key_vault.main` + `adls-access-key` secret
7. `azurerm_databricks_workspace.main` — VNet-injected workspace
8. `databricks_metastore.main` — Unity Catalog metastore
9. `databricks_metastore_assignment.main` — binds metastore to workspace
10. `azuread_application` + `azuread_service_principal` — pipeline identity
11. `azurerm_role_assignment.databricks_storage` — Storage Blob Data Contributor
12. `databricks_service_principal.sp` — SP registered inside Databricks
13. 3 catalogs: `bronze`, `silver`, `gold` with storage roots
14. 18 schemas (6 domains × 3 layers)
15. External locations + storage credentials
16. Grants: admin (ALL_PRIVILEGES), reader (USE_CATALOG, USE_SCHEMA, SELECT)

### 3.6 Step 5: Configure Databricks Secret Scope (AKV-backed)

This step is **manual** — Terraform cannot create Databricks secret scopes backed by Azure Key Vault (it's a Databricks API limitation for AKV-backed scopes):

```bash
# 1. Get the workspace URL from Terraform output
terraform output databricks_workspace_url
# e.g., "adb-1234567890123456.7.azuredatabricks.net"

# 2. Get the Key Vault URI and resource ID
terraform output key_vault_uri
# e.g., "https://kv-dev-dbx-abc123.vault.azure.net/"

# 3. Create the scope via Databricks CLI
databricks secrets create-scope \
  --scope pipeline-secrets \
  --scope-backend-type AZURE_KEYVAULT \
  --resource-id "/subscriptions/<sub-id>/resourceGroups/rg-dbx-pipeline-dev/providers/Microsoft.KeyVault/vaults/kv-dev-dbx-abc123" \
  --dns-name "https://kv-dev-dbx-abc123.vault.azure.net/"

# 4. Verify
databricks secrets list-scopes
# Expected: pipeline-secrets (AZURE_KEYVAULT)
```

**Alternative — Databricks-backed scope (for simplicity):**
```bash
databricks secrets create-scope --scope pipeline-secrets
databricks secrets put --scope pipeline-secrets --key storage-account-name --string-value "$(terraform output -raw storage_account_name)"
```

### 3.7 Step 6: Verify Permissions — The "Smoke Test"

The ultimate validation: can the Databricks service principal read/write to ADLS?

```python
# From inside a Databricks notebook (after workspace is accessible):
# Test cell 1 — verify secret scope
print(dbutils.secrets.listScopes())
# Should include "pipeline-secrets"

# Test cell 2 — verify ADLS access
storage_account = dbutils.secrets.get("pipeline-secrets", "storage-account-name")
test_path = f"abfss://bronze@{storage_account}.dfs.core.windows.net/test_write/"

df = spark.createDataFrame([(1, "hello")], ["id", "text"])
df.write.format("delta").mode("overwrite").save(test_path)
print(spark.read.format("delta").load(test_path).count())
# Expected: 1
```

---

## 4. Validation & Troubleshooting

### 4.1 Verification Checklist

| ✓ | Check | Command / Assertion |
|---|-------|-------------------|
| ☐ | Terraform plan shows zero changes after apply | `terraform plan` → `"No changes."` |
| ☐ | Resource group exists with all resources | `az resource list --resource-group rg-dbx-pipeline-dev --output table` |
| ☐ | Storage account has HNS enabled | `az storage account show --name stdevdatabricksdl --query isHnsEnabled` → `true` |
| ☐ | Subnets have Databricks delegation | `az network vnet subnet show --name snet-dev-public --vnet-name vnet-dev-databricks --resource-group rg-dbx-pipeline-dev --query delegations` |
| ☐ | Service principal has Storage Blob Data Contributor | `az role assignment list --assignee <sp-object-id> --query "[?roleDefinitionName=='Storage Blob Data Contributor']"` |
| ☐ | Unity Catalog metastore assigned to workspace | Databricks UI → Catalog → metastore shows `metastore-dev` |
| ☐ | Three catalogs visible (bronze, silver, gold) | `databricks catalogs list` → 3 entries |
| ☐ | Secret scope readable from notebook | `dbutils.secrets.listScopes()` includes `pipeline-secrets` |
| ☐ | ADLS write test passes | Notebook cell writes and reads back a Delta table |

### 4.2 Common Failure States

#### Failure 1: `terraform plan` fails — "Provider type mismatch"

```
Error: Provider type mismatch
│ Provider "hashicorp/databricks" is not compatible with
│ provider "databricks/databricks" required by module.clusters
```

**Root cause:** The child module `modules/databricks-cluster` declares `required_providers { databricks = { source = "databricks/databricks" } }` but Terraform hasn't loaded it.

**Fix:** The child module already has the correct `required_providers` block (see `terraform/modules/databricks-cluster/main.tf:1-8`). If you get this error, run `terraform init -upgrade` to refresh provider registrations.

#### Failure 2: Databricks workspace creation hangs or fails — "Subnet does not have delegation"

```
Error: creating Databricks Workspace: network configuration error
```

**Root cause:** The subnet wasn't delegated to `Microsoft.Databricks/workspaces`, or the delegation was applied to the VNet but not the specific subnet.

**Fix:**
```bash
# Verify delegation exists
az network vnet subnet show \
  --name snet-dev-public \
  --vnet-name vnet-dev-databricks \
  --resource-group rg-dbx-pipeline-dev \
  --query "delegations[0].serviceName"
# Must return: "Microsoft.Databricks/workspaces"

# If missing, Terraform should be managing this. Run:
terraform apply -target=azurerm_subnet.public -target=azurerm_subnet.private
```

#### Failure 3: Notebook read fails — "Failure to initialize configuration"

```
Error: This request is not authorized to perform this operation using this permission.
Status code: 403
```

**Root cause:** The service principal executing the notebook doesn't have data-plane access to the ADLS container.

**Diagnosis:**
```bash
# Check role assignments on the storage account
az role assignment list \
  --scope "/subscriptions/<sub-id>/resourceGroups/rg-dbx-pipeline-dev/providers/Microsoft.Storage/storageAccounts/stdevdatabricksdl" \
  --query "[?principalName contains(@, 'sp-databricks')]"
# Must show Storage Blob Data Contributor

# If missing, re-apply:
terraform apply -target=azurerm_role_assignment.databricks_storage
```

#### Failure 4: Secret scope not listing Key Vault secrets

```
Secret does not exist with scope: pipeline-secrets and key: storage-account-name
```

**Root cause (most common):** The Key Vault's access policy hasn't granted `Get`/`List` secrets to the Databricks workspace's managed identity.

**Fix:**
```bash
# Check Key Vault access policies
az keyvault show --name kv-dev-dbx-abc123 --query properties.accessPolicies

# If the Databricks workspace identity is missing, add it explicitly
WORKSPACE_IDENTITY=$(az databricks workspace show \
  --resource-group rg-dbx-pipeline-dev \
  --name dbw-dbx-pipeline-dev \
  --query "identity.principalId" -o tsv)

az keyvault set-policy \
  --name kv-dev-dbx-abc123 \
  --object-id "$WORKSPACE_IDENTITY" \
  --secret-permissions get list
```

### 4.3 Cost Control Guardrails

The repo includes several cost-control mechanisms:

| Mechanism | Where | Effect |
|-----------|-------|--------|
| `autotermination_minutes = 30` | `databricks-cluster/variables.tf:72` | All-purpose clusters shut down after 30 min idle |
| `autoscale { min_workers = 2 }` | `main.tf:63` | Job clusters scale down to 2 workers when idle |
| `spot_bid_max_price` (optional) | `databricks-cluster/variables.tf:42` | Use spot instances at X% of on-demand price |
| `auto_stop_mins = 10` (SQL Warehouse) | `main.tf:150` | Non-prod SQL warehouses stop after 10 min |
| `prevent_deletion_if_contains_resources = true` | `providers.tf:39` | Prevents accidental `terraform destroy` on populated RGs |

Add a **budget alert** immediately after deployment:
```bash
az consumption budget create \
  --resource-group rg-dbx-pipeline-dev \
  --name monthly-budget \
  --amount 500 \
  --time-grain Monthly \
  --time-period "$(date +%Y-%m-01T00:00:00Z)" \
  --contact-emails data-engineering@company.com
```

### 4.4 Post-Deployment: Secure Credentials

After successful deployment, Terraform outputs contain sensitive values. The repo marks them as `sensitive = true`:

```bash
terraform output service_principal_client_id
# This will be masked in the terminal but stored in state

# NEVER echo these to terminal logs in CI
# Store in CI secrets if needed:
# gh secret set DATABRICKS_SP_CLIENT_ID --body "$(terraform output -raw service_principal_client_id)"
```

---

## Module 1 Completion Criteria

You have completed Module 1 when:

1. `terraform apply` produces a **zero-diff plan** (`No changes. Your infrastructure matches the configuration.`)
2. You can log into the Databricks workspace at the URL from `terraform output databricks_workspace_url`
3. A notebook cell running `dbutils.secrets.get("pipeline-secrets", "storage-account-name")` returns the storage account name
4. A notebook cell writing to `abfss://bronze@<account>.dfs.core.windows.net/test/` succeeds and reads back the same data
5. `databricks catalogs list` shows `bronze`, `silver`, and `gold`
6. The Key Vault at `terraform output key_vault_uri` contains the `adls-access-key` secret

**Estimated time:** 3–4 hours for a first-time deployer (most of it waiting for the Databricks workspace to provision).
