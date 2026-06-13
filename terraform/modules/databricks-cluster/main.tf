terraform {
  required_version = ">= 1.5.0"

  required_providers {
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.50"
    }
  }
}

resource "databricks_cluster" "all_purpose" {
  cluster_name            = "${var.cluster_name}-ap"
  spark_version           = var.spark_version
  node_type_id            = var.node_type_id
  driver_node_type_id     = var.driver_node_type_id != null ? var.driver_node_type_id : var.node_type_id
  autotermination_minutes = var.autotermination_minutes
  spark_conf              = var.spark_conf
  policy_id               = var.policy_override

  autoscale {
    min_workers = var.autoscale_min_workers
    max_workers = var.autoscale_max_workers
  }

  # All-purpose cluster — shared interactive use
  data_security_mode = "SINGLE_USER"
  single_user_name   = data.databricks_current_user.main.user_name

  dynamic "library" {
    for_each = var.library_pypi
    content {
      pypi {
        package = library.value.pypi.package
        repo    = library.value.pypi.repo
      }
    }
  }

  dynamic "init_scripts" {
    for_each = var.init_scripts
    content {
      dbfs {
        destination = init_scripts.value.dbfs.destination
      }
    }
  }

  custom_tags = merge(
    {
      Environment = var.environment
      ClusterType = "all-purpose"
      ManagedBy   = "terraform"
    },
    var.custom_tags
  )
}

resource "databricks_cluster" "jobs" {
  cluster_name            = "${var.cluster_name}-jobs"
  spark_version           = var.spark_version
  node_type_id            = var.node_type_id
  driver_node_type_id     = var.driver_node_type_id != null ? var.driver_node_type_id : var.node_type_id
  autotermination_minutes = var.autotermination_minutes

  autoscale {
    min_workers = var.autoscale_min_workers
    max_workers = var.autoscale_max_workers
  }

  data_security_mode = "SINGLE_USER"
  single_user_name   = data.databricks_current_user.main.user_name
  spark_conf         = var.spark_conf

  dynamic "library" {
    for_each = var.library_pypi
    content {
      pypi {
        package = library.value.pypi.package
        repo    = library.value.pypi.repo
      }
    }
  }

  custom_tags = merge(
    {
      Environment = var.environment
      ClusterType = "jobs"
      ManagedBy   = "terraform"
    },
    var.custom_tags
  )
}

# Spot / cost-optimized cluster for batch workloads
resource "databricks_cluster" "spot" {
  count = var.spot_bid_max_price != null ? 1 : 0

  cluster_name            = "${var.cluster_name}-spot"
  spark_version           = var.spark_version
  node_type_id            = var.node_type_id
  driver_node_type_id     = var.driver_node_type_id != null ? var.driver_node_type_id : var.node_type_id
  autotermination_minutes = var.autotermination_minutes
  spark_conf              = var.spark_conf

  autoscale {
    min_workers = var.autoscale_min_workers
    max_workers = var.autoscale_max_workers
  }

  data_security_mode = "SINGLE_USER"
  single_user_name   = data.databricks_current_user.main.user_name

  dynamic "library" {
    for_each = var.library_pypi
    content {
      pypi {
        package = library.value.pypi.package
        repo    = library.value.pypi.repo
      }
    }
  }

  custom_tags = merge(
    {
      Environment = var.environment
      ClusterType = "spot"
      ManagedBy   = "terraform"
    },
    var.custom_tags
  )

  dynamic "azure_attributes" {
    for_each = var.spot_bid_max_price != null ? [1] : []
    content {
      availability       = "SPOT_WITH_FALLBACK_AZURE"
      spot_bid_max_price = var.spot_bid_max_price
    }
  }
}

# --- Cost Optimization: Single-Node Cluster ---
# Eliminates worker nodes entirely for conceptual validation and dbt compilation.
# Cuts compute costs by at least 50% compared to a multi-node cluster.
# Use for: Module 2 (data modeling), Module 4 (template deconstruction),
#          Module 5 (dbt compilation, governance validation).
# Do NOT use for: Module 3 shuffle mechanics testing (needs workers).
resource "databricks_cluster" "single_node" {
  count = var.single_node_enabled ? 1 : 0

  cluster_name            = "${var.cluster_name}-single"
  spark_version           = var.spark_version
  node_type_id            = var.single_node_driver_type
  autotermination_minutes = var.autotermination_minutes

  # Single-node: 0 workers, all computation on the driver
  num_workers = 0

  spark_conf = merge(
    var.spark_conf,
    {
      "spark.databricks.cluster.profile" = "singleNode"
      "spark.master"                     = "local[*]"
    }
  )

  data_security_mode = "SINGLE_USER"
  single_user_name   = data.databricks_current_user.main.user_name

  dynamic "library" {
    for_each = var.library_pypi
    content {
      pypi {
        package = library.value.pypi.package
        repo    = library.value.pypi.repo
      }
    }
  }

  custom_tags = merge(
    {
      Environment   = var.environment
      ClusterType   = "single-node"
      ResourceClass = "SingleNode"
      ManagedBy     = "terraform"
    },
    var.custom_tags
  )
}

data "databricks_current_user" "main" {}
