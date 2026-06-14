# ─── Core Azure Resources ───
module "azure" {
  source = "./modules/azure-resources"

  resource_group_name = "rg-${var.project_name}-${var.environment}"
  location            = var.location
  environment         = var.environment
  tags                = var.tags
}

# ─── Databricks Workspace + Unity Catalog ───
module "databricks" {
  source = "./modules/databricks-workspace"

  resource_group_name   = module.azure.resource_group_name
  location              = module.azure.location
  environment           = var.environment
  workspace_name        = "dbw-${var.project_name}-${var.environment}"
  sku                   = var.databricks_workspace_sku
  public_subnet_id      = module.azure.public_subnet_id
  private_subnet_id     = module.azure.private_subnet_id
  storage_account_name  = module.azure.storage_account_name
  storage_account_id    = module.azure.storage_account_id
  tenant_id             = module.azure.tenant_id
  databricks_account_id = var.databricks_account_id
  admin_group_name      = var.admin_group_name
  reader_group_name     = var.reader_group_name
  tags                  = var.tags
}
