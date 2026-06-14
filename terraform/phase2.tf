# ═══════════════════════════════════════════════════════════
# PHASE 2 — Workspace-level resources
# ═══════════════════════════════════════════════════════════
# These resources require the Databricks workspace to already
# exist (workspace_url must be known). Apply AFTER phase 1:
#
#   terraform apply -target=module.azure -target=module.databricks
#   terraform apply    # picks up phase2 resources automatically
# ═══════════════════════════════════════════════════════════

# ─── Databricks Clusters ───
module "clusters" {
  source = "./modules/databricks-cluster"

  environment            = var.environment
  cluster_name           = "${var.project_name}-${var.environment}"
  spark_version          = var.dbr_version
  node_type_id           = var.cluster_node_type
  autoscale_min_workers  = var.autoscale_min_workers
  autoscale_max_workers  = var.autoscale_max_workers

  providers = {
    databricks = databricks.workspace
  }
}

# ─── Databricks Workflows (Jobs) ───
resource "databricks_job" "medallion_pipeline" {
  name = "${var.project_name}-${var.environment}-medallion"

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

  task {
    task_key = "silver_transformation"
    depends_on { task_key = "bronze_ingestion" }
    job_cluster_key = "default"
    notebook_task {
      notebook_path = "/Shared/pipelines/medallion/silver_transformation"
      base_parameters = {
        dbt_command = "run --select silver.* --target ${var.environment}"
      }
    }
  }

  task {
    task_key = "gold_aggregation"
    depends_on { task_key = "silver_transformation" }
    job_cluster_key = "default"
    notebook_task {
      notebook_path = "/Shared/pipelines/medallion/gold_aggregation"
      base_parameters = {
        dbt_command = "run --select gold.* --target ${var.environment}"
      }
    }
  }

  task {
    task_key = "data_quality"
    depends_on { task_key = "gold_aggregation" }
    job_cluster_key = "default"
    notebook_task {
      notebook_path = "/Shared/pipelines/medallion/data_quality"
    }
  }

  schedule {
    quartz_cron_expression = "0 0 6 * * ?"
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
