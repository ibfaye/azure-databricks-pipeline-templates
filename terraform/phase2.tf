# ═══════════════════════════════════════════════════════════
# PHASE 2 — Workspace-level resources
# ═══════════════════════════════════════════════════════════
# Controlled by var.deploy_workspace_resources (default: false).
#
# Phase 1:  terraform apply     → creates workspace + infra
# Phase 2:  set deploy_workspace_resources = true in tfvars
#           terraform apply     → creates clusters, jobs, SQL warehouse
# ═══════════════════════════════════════════════════════════

# ─── Databricks Clusters ───
# Cost-safe defaults: 15-min auto-termination, single-node available,
# spot with on-demand fallback, job clusters for pipelines.
module "clusters" {
  count  = var.deploy_workspace_resources ? 1 : 0
  source = "./modules/databricks-cluster"

  environment             = var.environment
  cluster_name            = "${var.project_name}-${var.environment}"
  spark_version           = var.dbr_version
  node_type_id            = var.cluster_node_type
  autoscale_min_workers   = var.autoscale_min_workers
  autoscale_max_workers   = var.autoscale_max_workers
  spot_bid_max_price      = var.spot_bid_max_price
  single_node_enabled     = var.single_node_enabled
  single_node_driver_type = var.single_node_driver_type
  custom_tags = merge(var.tags, {
    cost_center = var.cost_center_tag
  })

  providers = {
    databricks = databricks.workspace
  }
}

# ─── Databricks Workflows (Jobs) ───
# NOTE: Workflows use job_cluster { new_cluster {} } — NOT existing clusters.
# Job clusters spin up on-demand and terminate when the pipeline finishes.
# This is 40-50% cheaper than All-Purpose compute (job DBUs vs interactive DBUs).
#
# COST OPTIMIZATION — State Management:
#   - `terraform destroy` when stepping away for multiple days.
#   - To retain Unity Catalog metadata across destroys:
#       1. Comment out `module "databricks"` (preserves workspace + metastore)
#       2. Destroy everything else: `terraform apply -target=module.azure`
#       3. Ensure all clusters are TERMINATED (not just idle)
#   - Static infrastructure (ADLS, Key Vault) costs ~$1-2/month when idle.

resource "databricks_job" "medallion_pipeline" {
  provider = databricks.workspace
  count    = var.deploy_workspace_resources ? 1 : 0
  name     = "${var.project_name}-${var.environment}-medallion"

  job_cluster {
    job_cluster_key = "default"
    new_cluster {
      spark_version      = var.dbr_version
      node_type_id       = var.cluster_node_type
      data_security_mode = "SINGLE_USER"
      spark_conf = {
        "spark.databricks.delta.optimizeWrite.enabled" = "true"
        "spark.databricks.delta.autoCompact.enabled"   = "true"
      }
      autoscale {
        min_workers = var.autoscale_min_workers
        max_workers = var.autoscale_max_workers
      }
    }
  }

  # Task 1: Bronze ingestion
  task {
    task_key        = "bronze_ingestion"
    job_cluster_key = "default"

    notebook_task {
      notebook_path = "/Shared/pipelines/medallion/bronze_ingestion"
      base_parameters = {
        catalog         = "bronze"
        landing_path    = "abfss://landing@${module.azure.storage_account_name}.dfs.core.windows.net/"
        checkpoint_path = "abfss://checkpoint@${module.azure.storage_account_name}.dfs.core.windows.net/bronze/"
      }
    }
  }

  # Task 2: Silver transformation (dbt)
  task {
    task_key = "silver_transformation"
    depends_on {
      task_key = "bronze_ingestion"
    }
    job_cluster_key = "default"

    notebook_task {
      notebook_path = "/Shared/pipelines/medallion/silver_transformation"
      base_parameters = {
        dbt_command = "run --select silver.* --target ${var.environment}"
      }
    }
  }

  # Task 3: Gold aggregation (dbt)
  task {
    task_key = "gold_aggregation"
    depends_on {
      task_key = "silver_transformation"
    }
    job_cluster_key = "default"

    notebook_task {
      notebook_path = "/Shared/pipelines/medallion/gold_aggregation"
      base_parameters = {
        dbt_command = "run --select gold.* --target ${var.environment}"
      }
    }
  }

  # Task 4: Data quality checks
  task {
    task_key = "data_quality"
    depends_on {
      task_key = "gold_aggregation"
    }
    job_cluster_key = "default"

    notebook_task {
      notebook_path = "/Shared/pipelines/medallion/data_quality"
    }
  }

  schedule {
    quartz_cron_expression = "0 0 6 * * ?" # Daily at 6 AM UTC
    timezone_id            = "UTC"
  }

  email_notifications {
    on_failure                = ["data-engineering@company.com"]
    no_alert_for_skipped_runs = false
  }

  tags = merge(var.tags, {
    pipeline = "medallion"
    layer    = "etl"
  })
}

# ─── SQL Warehouse for BI ───
resource "databricks_sql_endpoint" "main" {
  provider         = databricks.workspace
  count            = var.deploy_workspace_resources ? 1 : 0
  name             = "sql-warehouse-${var.environment}"
  cluster_size     = var.environment == "prod" ? "2X-Small" : "X-Small"
  min_num_clusters = 1
  max_num_clusters = var.environment == "prod" ? 3 : 1
  auto_stop_mins   = var.environment == "prod" ? 30 : 10

  channel {
    name = "CHANNEL_NAME_CURRENT"
  }

  tags {
    custom_tags {
      key   = "Environment"
      value = var.environment
    }
  }
}
