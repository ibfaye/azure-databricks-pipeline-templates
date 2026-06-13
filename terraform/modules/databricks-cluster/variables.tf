variable "environment" {
  description = "Environment name"
  type        = string
}

variable "cluster_name" {
  description = "Cluster name prefix"
  type        = string
}

variable "spark_version" {
  description = "Databricks Runtime version"
  type        = string
  default     = "14.3.x-scala2.12"
}

variable "node_type_id" {
  description = "Worker node type"
  type        = string
  default     = "Standard_DS3_v2"
}

variable "driver_node_type_id" {
  description = "Driver node type (defaults to worker type)"
  type        = string
  default     = null
}

variable "autoscale_min_workers" {
  description = "Minimum workers for autoscaling"
  type        = number
  default     = 2
}

variable "autoscale_max_workers" {
  description = "Maximum workers for autoscaling"
  type        = number
  default     = 10
}

variable "spot_bid_max_price" {
  description = "Max price for spot instances (% of on-demand). Null disables spot."
  type        = number
  default     = null
}

variable "policy_override" {
  description = "Custom cluster policy overrides (JSON)"
  type        = string
  default     = null
}

variable "init_scripts" {
  description = "DBFS paths for init scripts"
  type = list(object({
    dbfs = object({
      destination = string
    })
  }))
  default = []
}

variable "custom_tags" {
  description = "Additional cluster tags"
  type        = map(string)
  default     = {}
}

variable "autotermination_minutes" {
  description = "Auto-termination after N minutes of inactivity"
  type        = number
  default     = 30
}

variable "spark_conf" {
  description = "Spark configuration overrides"
  type        = map(string)
  default = {
    "spark.databricks.delta.optimizeWrite.enabled"  = "true"
    "spark.databricks.delta.autoCompact.enabled"    = "true"
    "spark.sql.adaptive.enabled"                    = "true"
    "spark.sql.adaptive.coalescePartitions.enabled" = "true"
    "spark.sql.shuffle.partitions"                  = "auto"
    "spark.databricks.io.cache.enabled"             = "true"
  }
}

variable "library_pypi" {
  description = "PyPI packages to install on the cluster"
  type = list(object({
    pypi = object({
      package = string
      repo    = optional(string)
    })
  }))
  default = [
    { pypi = { package = "dbt-databricks>=1.8.0" } },
    { pypi = { package = "delta-spark>=3.1.0" } },
    { pypi = { package = "pandas>=2.0" } },
    { pypi = { package = "pyarrow>=15.0" } },
    { pypi = { package = "great-expectations>=1.0" } },
    { pypi = { package = "pyspark-stubs>=3.0.0" } },
  ]
}
