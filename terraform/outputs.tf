output "resource_group_name" {
  description = "Resource group name"
  value       = module.azure.resource_group_name
}

output "databricks_workspace_url" {
  description = "Databricks workspace URL"
  value       = module.databricks.workspace_url
}

output "databricks_workspace_id" {
  description = "Databricks workspace ID"
  value       = module.databricks.workspace_id
}

output "storage_account_name" {
  description = "ADLS Gen2 storage account name"
  value       = module.azure.storage_account_name
}

output "key_vault_uri" {
  description = "Key Vault URI"
  value       = module.azure.key_vault_uri
}

output "sql_warehouse_id" {
  description = "SQL Warehouse endpoint ID"
  value       = databricks_sql_endpoint.main.id
}

output "medallion_job_url" {
  description = "Databricks job URL for the medallion pipeline"
  value       = "${module.databricks.workspace_url}/#job/${databricks_job.medallion_pipeline.id}"
}

output "service_principal_client_id" {
  description = "Service principal client ID for pipeline authentication"
  value       = module.databricks.service_principal_client_id
  sensitive   = true
}
