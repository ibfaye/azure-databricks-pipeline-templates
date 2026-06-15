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
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
    time = {
      source  = "hashicorp/time"
      version = "~> 0.12"
    }
  }

  # backend "azurerm" {
  #   resource_group_name  = "rg-terraform-state"
  #   storage_account_name = "stterraformstate"
  #   container_name       = "tfstate"
  #   key                  = "databricks-pipeline.tfstate"
  # }
}

# ─── Azure Provider ───
provider "azurerm" {
  features {
    resource_group {
      prevent_deletion_if_contains_resources = true
    }
    key_vault {
      purge_soft_delete_on_destroy    = false
      recover_soft_deleted_key_vaults = true
    }
  }
  subscription_id = var.azure_subscription_id
  tenant_id       = var.azure_tenant_id
}

# ─── Databricks providers ───
provider "databricks" {
  host       = "https://accounts.azuredatabricks.net"
  account_id = var.databricks_account_id
}

provider "databricks" {
  alias = "workspace"

  # Resolved at apply time (workspace must exist first)
  host                        = module.databricks.workspace_url
  azure_workspace_resource_id = module.databricks.workspace_id
  azure_tenant_id             = module.azure.tenant_id

  # Auth: inherits ARM_CLIENT_ID / ARM_CLIENT_SECRET from azurerm provider (SP auth)
}

# ─── Azure AD ───
provider "azuread" {
  tenant_id = var.azure_tenant_id
}
