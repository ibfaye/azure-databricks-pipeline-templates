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

# Databricks provider configuration (requires workspace to exist first)
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

# ─── Catalog structure ───
resource "databricks_catalog" "bronze" {
  provider     = databricks.workspace
  name         = "bronze"
  comment      = "Raw ingested data (Bronze layer)"
  storage_root = "abfss://bronze@${var.storage_account_name}.dfs.core.windows.net/"
  properties = {
    layer = "bronze"
  }
}

resource "databricks_catalog" "silver" {
  provider     = databricks.workspace
  name         = "silver"
  comment      = "Cleansed and validated data (Silver layer)"
  storage_root = "abfss://silver@${var.storage_account_name}.dfs.core.windows.net/"
  properties = {
    layer = "silver"
  }
}

resource "databricks_catalog" "gold" {
  provider     = databricks.workspace
  name         = "gold"
  comment      = "Aggregated business-ready data (Gold layer)"
  storage_root = "abfss://gold@${var.storage_account_name}.dfs.core.windows.net/"
  properties = {
    layer = "gold"
  }
}

# ─── Default schemas ───
locals {
  schemas = ["sales", "marketing", "finance", "operations", "iot", "customers"]
}

resource "databricks_schema" "bronze" {
  for_each     = toset(local.schemas)
  provider     = databricks.workspace
  catalog_name = databricks_catalog.bronze.name
  name         = each.key
  properties   = { environment = var.environment }
}

resource "databricks_schema" "silver" {
  for_each     = toset(local.schemas)
  provider     = databricks.workspace
  catalog_name = databricks_catalog.silver.name
  name         = each.key
  properties   = { environment = var.environment }
}

resource "databricks_schema" "gold" {
  for_each     = toset(local.schemas)
  provider     = databricks.workspace
  catalog_name = databricks_catalog.gold.name
  name         = each.key
  properties   = { environment = var.environment }
}

# ─── External Locations ───
# Disabled — catalogs use storage_root directly. Uncomment if direct ADLS access is needed.
#
# resource "databricks_storage_credential" "main" {
#   provider = databricks.workspace
#   name     = "storage-cred-${var.environment}"
#   azure_managed_identity {
#     access_connector_id = azurerm_databricks_workspace.main.storage_account_identity[0].managed_identity_id
#   }
# }
#
# resource "databricks_external_location" "datalake" {
#   provider        = databricks.workspace
#   name            = "datalake-${var.environment}"
#   url             = "abfss://bronze@${var.storage_account_name}.dfs.core.windows.net/"
#   credential_name = databricks_storage_credential.main.name
#   comment         = "External location for ADLS Gen2 data lake"
# }

# ─── Grants (admin group) ───

# ─── Grants (admin group) ───
# Grants on metastore require metastore_id — use Databricks UI or uncomment
# the metastore data source above to get the ID.
#
# resource "databricks_grants" "metastore" {
#   provider  = databricks.workspace
#   metastore = data.databricks_metastore.main.metastore_id
#   grant {
#     principal  = var.admin_group_name
#     privileges = ["CREATE_CATALOG", "CREATE_CONNECTION", "CREATE_EXTERNAL_LOCATION", "CREATE_STORAGE_CREDENTIAL"]
#   }
# }

resource "databricks_grants" "bronze_catalog" {
  provider = databricks.workspace
  catalog  = databricks_catalog.bronze.name
  grant {
    principal  = var.admin_group_name
    privileges = ["ALL_PRIVILEGES"]
  }
  grant {
    principal  = var.reader_group_name
    privileges = ["USE_CATALOG", "USE_SCHEMA", "SELECT"]
  }
}

resource "databricks_grants" "silver_catalog" {
  provider = databricks.workspace
  catalog  = databricks_catalog.silver.name
  grant {
    principal  = var.admin_group_name
    privileges = ["ALL_PRIVILEGES"]
  }
  grant {
    principal  = var.reader_group_name
    privileges = ["USE_CATALOG", "USE_SCHEMA", "SELECT"]
  }
}

resource "databricks_grants" "gold_catalog" {
  provider = databricks.workspace
  catalog  = databricks_catalog.gold.name
  grant {
    principal  = var.admin_group_name
    privileges = ["ALL_PRIVILEGES"]
  }
  grant {
    principal  = var.reader_group_name
    privileges = ["USE_CATALOG", "USE_SCHEMA", "SELECT"]
  }
}
