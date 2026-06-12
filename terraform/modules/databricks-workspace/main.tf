resource "azurerm_databricks_workspace" "main" {
  name                         = var.workspace_name
  resource_group_name          = var.resource_group_name
  location                     = var.location
  sku                          = var.sku
  managed_resource_group_name  = var.managed_resource_group_name
  public_network_access_enabled = !var.no_public_ip
  network_security_group_rules_required = var.no_public_ip ? "NoAzureDatabricksRules" : "AllRules"
  tags                         = var.tags

  custom_parameters {
    no_public_ip                                         = var.no_public_ip
    virtual_network_id                                   = var.public_subnet_id != null ? regex("^(.*?)/subnets/", var.public_subnet_id)[0] : null
    public_subnet_name                                   = var.public_subnet_id != null ? split("/", var.public_subnet_id)[length(split("/", var.public_subnet_id)) - 1] : null
    private_subnet_name                                  = var.private_subnet_id != null ? split("/", var.private_subnet_id)[length(split("/", var.private_subnet_id)) - 1] : null
    storage_account_name                                 = ""
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
resource "databricks_metastore" "main" {
  provider      = databricks.workspace
  name          = "metastore-${var.environment}"
  storage_root  = "abfss://${var.environment}-metastore@${var.storage_account_name}.dfs.core.windows.net/"
  owner         = var.databricks_account_id
  region        = var.location
  force_destroy = true
}

resource "databricks_metastore_assignment" "main" {
  provider     = databricks.workspace
  metastore_id = databricks_metastore.main.id
  workspace_id = azurerm_databricks_workspace.main.workspace_id
}

# ─── Service Principal for Unity Catalog ───
resource "azuread_application" "databricks_sp" {
  display_name = "sp-databricks-${var.environment}-${var.workspace_name}"
}

resource "azuread_service_principal" "databricks_sp" {
  client_id = azuread_application.databricks_sp.client_id
}

resource "azuread_service_principal_password" "databricks_sp" {
  service_principal_id = azuread_service_principal.databricks_sp.id
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
  depends_on = [databricks_metastore_assignment.main]
}

resource "databricks_catalog" "silver" {
  provider     = databricks.workspace
  name         = "silver"
  comment      = "Cleansed and validated data (Silver layer)"
  storage_root = "abfss://silver@${var.storage_account_name}.dfs.core.windows.net/"
  properties = {
    layer = "silver"
  }
  depends_on = [databricks_metastore_assignment.main]
}

resource "databricks_catalog" "gold" {
  provider     = databricks.workspace
  name         = "gold"
  comment      = "Aggregated business-ready data (Gold layer)"
  storage_root = "abfss://gold@${var.storage_account_name}.dfs.core.windows.net/"
  properties = {
    layer = "gold"
  }
  depends_on = [databricks_metastore_assignment.main]
}

# ─── Default schemas ───
locals {
  schemas = ["sales", "marketing", "finance", "operations", "iot", "customers"]
}

resource "databricks_schema" "bronze" {
  for_each    = toset(local.schemas)
  provider    = databricks.workspace
  catalog_name = databricks_catalog.bronze.name
  name         = each.key
  properties   = { environment = var.environment }
}

resource "databricks_schema" "silver" {
  for_each    = toset(local.schemas)
  provider    = databricks.workspace
  catalog_name = databricks_catalog.silver.name
  name         = each.key
  properties   = { environment = var.environment }
}

resource "databricks_schema" "gold" {
  for_each    = toset(local.schemas)
  provider    = databricks.workspace
  catalog_name = databricks_catalog.gold.name
  name         = each.key
  properties   = { environment = var.environment }
}

# ─── External Locations ───
resource "databricks_storage_credential" "main" {
  provider = databricks.workspace
  name     = "storage-cred-${var.environment}"
  azure_service_principal {
    directory_id   = var.tenant_id
    application_id = azuread_application.databricks_sp.client_id
    client_secret   = azuread_service_principal_password.databricks_sp.value
  }
}

resource "databricks_external_location" "datalake" {
  provider          = databricks.workspace
  name              = "datalake-${var.environment}"
  url               = "abfss://bronze@${var.storage_account_name}.dfs.core.windows.net/"
  credential_name   = databricks_storage_credential.main.name
  comment           = "External location for ADLS Gen2 data lake"
}

resource "databricks_external_location" "silver_loc" {
  provider          = databricks.workspace
  name              = "silver-${var.environment}"
  url               = "abfss://silver@${var.storage_account_name}.dfs.core.windows.net/"
  credential_name   = databricks_storage_credential.main.name
}

resource "databricks_external_location" "gold_loc" {
  provider          = databricks.workspace
  name              = "gold-${var.environment}"
  url               = "abfss://gold@${var.storage_account_name}.dfs.core.windows.net/"
  credential_name   = databricks_storage_credential.main.name
}

# ─── Grants (admin group) ───
resource "databricks_grants" "metastore" {
  provider     = databricks.workspace
  metastore    = databricks_metastore.main.id
  grant {
    principal  = var.admin_group_name
    privileges = ["CREATE_CATALOG", "CREATE_CONNECTION", "CREATE_EXTERNAL_LOCATION", "CREATE_STORAGE_CREDENTIAL"]
  }
}

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
