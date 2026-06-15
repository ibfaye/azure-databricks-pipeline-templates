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

variable "deploy_workspace_resources" {
  description = "Set to true after Phase 1 (workspace created) to deploy clusters, jobs, SQL warehouse"
  type        = bool
  default     = false
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

# ─── Cost Optimization ─────────────────────────────────────────────────────

variable "single_node_enabled" {
  description = "Deploy a single-node (0 workers) cluster for conceptual validation. Cuts compute costs by >=50%."
  type        = bool
  default     = true
}

variable "single_node_driver_type" {
  description = "Driver node type for single-node cluster (memory-optimized for dbt compilation)"
  type        = string
  default     = "Standard_DS4_v2"
}

variable "spot_bid_max_price" {
  description = "Max price for spot instances as % of on-demand (e.g., 80 = 80%). Uses SPOT_WITH_FALLBACK_AZURE. Null disables spot cluster."
  type        = number
  default     = 80
}

variable "cost_center_tag" {
  description = "Cost center tag for budget tracking and attribution"
  type        = string
  default     = "data-engineering"
}

variable "tags" {
  description = "Additional tags for all resources"
  type        = map(string)
  default     = {}
}
