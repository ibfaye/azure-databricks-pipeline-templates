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

# ─── Databricks Clusters ───
module "clusters" {
  source = "./modules/databricks-cluster"

  environment            = var.environment
  cluster_name           = "${var.project_name}-${var.environment}"
  spark_version          = var.dbr_version
  node_type_id           = var.cluster_node_type
  autoscale_min_workers  = var.autoscale_min_workers
  autoscale_max_workers  = var.autoscale_max_workers
  custom_tags            = var.tags

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
      spark_version           = var.dbr_version
      node_type_id            = var.cluster_node_type
      data_security_mode      = "SINGLE_USER"
      spark_conf = {
        "spark.databricks.delta.preview.enabled"      = "true"
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
    task_key = "bronze_ingestion"
    job_cluster_key = "default"

    notebook_task {
      notebook_path = "/Shared/pipelines/medallion/bronze_ingestion"
      base_parameters = {
        catalog       = "bronze"
        landing_path  = "abfss://landing@${module.azure.storage_account_name}.dfs.core.windows.net/"
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
    quartz_cron_expression = "0 0 6 * * ?"  # Daily at 6 AM UTC
    timezone_id            = "UTC"
  }

  email_notifications {
    on_failure = ["data-engineering@company.com"]
    no_alert_for_skipped_runs = false
  }

  tags = merge(var.tags, {
    pipeline = "medallion"
    layer    = "etl"
  })
}

# ─── SQL Warehouse for BI ───
resource "databricks_sql_endpoint" "main" {
  name            = "sql-warehouse-${var.environment}"
  cluster_size    = var.environment == "prod" ? "2X-Small" : "X-Small"
  min_num_clusters = 1
  max_num_clusters = var.environment == "prod" ? 3 : 1
  auto_stop_mins   = var.environment == "prod" ? 30 : 10

  channel {
    name = "CHANNEL_NAME_PREVIEW"
  }

  tags {
    custom_tags {
      key   = "Environment"
      value = var.environment
    }
  }
}
