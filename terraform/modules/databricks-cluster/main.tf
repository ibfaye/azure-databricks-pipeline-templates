terraform {
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

  # Enable Unity Catalog by default
  data_security_mode = "USER_ISOLATION"
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

  data_security_mode      = "SINGLE_USER"
  single_user_name        = data.databricks_current_user.main.user_name
  spark_conf              = merge(var.spark_conf, {
    "spark.databricks.cluster.profile" = "singleNode"
  })

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

  data_security_mode      = "SINGLE_USER"
  single_user_name        = data.databricks_current_user.main.user_name

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
      availability     = "SPOT_AZURE"
      spot_bid_max_price = var.spot_bid_max_price
    }
  }
}

data "databricks_current_user" "main" {}
