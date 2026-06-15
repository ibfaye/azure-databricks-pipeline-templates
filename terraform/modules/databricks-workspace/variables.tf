variable "resource_group_name" {
  description = "Resource group name"
  type        = string
}

variable "location" {
  description = "Azure region"
  type        = string
}

variable "environment" {
  description = "Environment name"
  type        = string
}

variable "workspace_name" {
  description = "Databricks workspace name"
  type        = string
}

variable "sku" {
  description = "Databricks workspace SKU (standard, premium, trial)"
  type        = string
  default     = "premium"
}

variable "managed_resource_group_name" {
  description = "Name for the managed resource group"
  type        = string
  default     = null
}

variable "public_subnet_id" {
  description = "Public subnet ID"
  type        = string
}

variable "private_subnet_id" {
  description = "Private subnet ID"
  type        = string
}

variable "no_public_ip" {
  description = "Deploy Databricks with no public IP (secure cluster connectivity)"
  type        = bool
  default     = true
}

variable "tags" {
  description = "Resource tags"
  type        = map(string)
  default     = {}
}

variable "storage_account_name" {
  description = "ADLS Gen2 storage account name"
  type        = string
}

variable "storage_account_id" {
  description = "ADLS Gen2 storage account resource ID"
  type        = string
}

variable "tenant_id" {
  description = "Azure tenant ID"
  type        = string
}

variable "databricks_account_id" {
  description = "Databricks account ID for metastore ownership"
  type        = string
}

variable "admin_group_name" {
  description = "Databricks admin group for Unity Catalog grants"
  type        = string
  default     = "admins"
}

variable "reader_group_name" {
  description = "Databricks reader group for read-only access"
  type        = string
  default     = "analysts"
}

variable "public_nsg_association_id" {
  description = "NSG association ID for public subnet (required for VNet injection)"
  type        = string
}
