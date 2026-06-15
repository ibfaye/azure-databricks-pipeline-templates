terraform {
  required_version = ">= 1.5.0"

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 4.0"
    }
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.50"
    }
    azuread = {
      source  = "hashicorp/azuread"
      version = "~> 2.50"
    }
    time = {
      source  = "hashicorp/time"
      version = "~> 0.12"
    }
  }
}

resource "azurerm_databricks_workspace" "main" {
  name                                  = var.workspace_name
  resource_group_name                   = var.resource_group_name
  location                              = var.location
  sku                                   = var.sku
  managed_resource_group_name           = var.managed_resource_group_name
  public_network_access_enabled         = !var.no_public_ip
  network_security_group_rules_required = var.no_public_ip ? "NoAzureDatabricksRules" : "AllRules"
  tags                                  = var.tags

  custom_parameters {
    no_public_ip                                         = var.no_public_ip
    virtual_network_id                                   = var.public_subnet_id != null ? regex("^(.*?)/subnets/", var.public_subnet_id)[0] : null
    public_subnet_name                                   = var.public_subnet_id != null ? split("/", var.public_subnet_id)[length(split("/", var.public_subnet_id)) - 1] : null
    private_subnet_name                                  = var.private_subnet_id != null ? split("/", var.private_subnet_id)[length(split("/", var.private_subnet_id)) - 1] : null
    public_subnet_network_security_group_association_id  = var.public_nsg_association_id
    private_subnet_network_security_group_association_id = var.private_nsg_association_id
    storage_account_sku_name                             = "Standard_GRS"
    nat_gateway_name                                     = ""
  }
}

# Databricks provider — defined here for module-level usage.
# Root providers.tf overrides this when provider = databricks.workspace is passed.
provider "databricks" {
  alias = "workspace"
  host  = "https://${azurerm_databricks_workspace.main.workspace_url}"
}

# ─── Unity Catalog Metastore ───
# Workspace auto-assigns to the region's existing metastore.
# No explicit metastore creation or assignment needed if one already exists.
# To force a specific metastore, uncomment:
#
# data "databricks_metastore" "main" {
#   provider     = databricks.workspace
#   metastore_id = "your-metastore-id"
# }
#
# resource "databricks_metastore_assignment" "main" {
#   provider     = databricks.workspace
#   metastore_id = data.databricks_metastore.main.metastore_id
#   workspace_id = azurerm_databricks_workspace.main.workspace_id
# }

# ─── Service Principal for Unity Catalog ───
resource "azuread_application" "databricks_sp" {
  display_name = "sp-databricks-${var.environment}-${var.workspace_name}"
}

resource "azuread_service_principal" "databricks_sp" {
  client_id = azuread_application.databricks_sp.client_id
}

resource "azuread_service_principal_password" "databricks_sp" {
  service_principal_id = azuread_service_principal.databricks_sp.id

  lifecycle {
    replace_triggered_by = [time_rotating.sp_secret.id]
  }
}

resource "time_rotating" "sp_secret" {
  rotation_days = 90
}

resource "azurerm_role_assignment" "databricks_storage" {
  scope                = var.storage_account_id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azuread_service_principal.databricks_sp.object_id
}

resource "databricks_service_principal" "sp" {
  provider       = databricks.workspace
  application_id = azuread_application.databricks_sp.client_id
  display_name   = "sp-unity-catalog-${var.environment}"
}

# ═══════════════════════════════════════════════════════════════════
# UNITY CATALOG — Create via Databricks SQL Editor (30 seconds)
# ═══════════════════════════════════════════════════════════════════
# Catalogs require managed storage or external locations, which need
# account admin privileges. Run this SQL in your workspace's SQL Editor:
#
#   CREATE CATALOG IF NOT EXISTS bronze COMMENT 'Raw ingested data (Bronze layer)';
#   CREATE CATALOG IF NOT EXISTS silver COMMENT 'Cleansed data (Silver layer)';
#   CREATE CATALOG IF NOT EXISTS gold   COMMENT 'Business-ready data (Gold layer)';
#
#   CREATE SCHEMA IF NOT EXISTS bronze.sales;
#   CREATE SCHEMA IF NOT EXISTS bronze.marketing;
#   CREATE SCHEMA IF NOT EXISTS bronze.finance;
#   CREATE SCHEMA IF NOT EXISTS bronze.operations;
#   CREATE SCHEMA IF NOT EXISTS bronze.iot;
#   CREATE SCHEMA IF NOT EXISTS bronze.customers;
#
#   CREATE SCHEMA IF NOT EXISTS silver.sales;
#   CREATE SCHEMA IF NOT EXISTS silver.marketing;
#   CREATE SCHEMA IF NOT EXISTS silver.finance;
#   CREATE SCHEMA IF NOT EXISTS silver.operations;
#   CREATE SCHEMA IF NOT EXISTS silver.iot;
#   CREATE SCHEMA IF NOT EXISTS silver.customers;
#
#   CREATE SCHEMA IF NOT EXISTS gold.sales;
#   CREATE SCHEMA IF NOT EXISTS gold.marketing;
#   CREATE SCHEMA IF NOT EXISTS gold.finance;
#   CREATE SCHEMA IF NOT EXISTS gold.operations;
#   CREATE SCHEMA IF NOT EXISTS gold.iot;
#   CREATE SCHEMA IF NOT EXISTS gold.customers;
#
#   GRANT ALL PRIVILEGES ON CATALOG bronze TO admins;
#   GRANT ALL PRIVILEGES ON CATALOG silver TO admins;
#   GRANT ALL PRIVILEGES ON CATALOG gold   TO admins;
# ═══════════════════════════════════════════════════════════════════
