variable "azure_subscription_id" {
  description = "Azure subscription ID"
  type        = string
  sensitive   = true
}

variable "azure_tenant_id" {
  description = "Azure tenant ID"
  type        = string
}

variable "databricks_account_id" {
  description = "Databricks account ID"
  type        = string
}

variable "project_name" {
  description = "Project name used for resource naming"
  type        = string
  default     = "dbx-pipeline"
}

variable "environment" {
  description = "Environment name (dev, staging, prod)"
  type        = string
  default     = "dev"
}

variable "location" {
  description = "Azure region for all resources"
  type        = string
  default     = "westeurope"
}

variable "databricks_workspace_sku" {
  description = "Databricks workspace SKU"
  type        = string
  default     = "premium"
}

variable "cluster_node_type" {
  description = "Databricks cluster node type"
  type        = string
  default     = "Standard_DS3_v2"
}

variable "dbr_version" {
  description = "Databricks Runtime version"
  type        = string
  default     = "14.3.x-scala2.12"
}

variable "autoscale_min_workers" {
  description = "Minimum workers for Databricks autoscaling"
  type        = number
  default     = 2
}

variable "autoscale_max_workers" {
  description = "Maximum workers for Databricks autoscaling"
  type        = number
  default     = 10
}

variable "admin_group_name" {
  description = "Databricks admin group name"
  type        = string
  default     = "admins"
}

variable "reader_group_name" {
  description = "Databricks reader group name"
  type        = string
  default     = "analysts"
}

variable "tags" {
  description = "Additional tags for all resources"
  type        = map(string)
  default     = {}
}
