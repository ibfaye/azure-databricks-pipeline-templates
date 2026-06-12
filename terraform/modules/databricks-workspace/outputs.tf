output "workspace_id" {
  description = "Databricks workspace ID"
  value       = azurerm_databricks_workspace.main.workspace_id
}

output "workspace_url" {
  description = "Databricks workspace URL"
  value       = "https://${azurerm_databricks_workspace.main.workspace_url}"
}

output "metastore_id" {
  description = "Unity Catalog metastore ID"
  value       = databricks_metastore.main.id
}

output "service_principal_client_id" {
  description = "Service principal client ID for pipeline authentication"
  value       = azuread_application.databricks_sp.client_id
}

output "service_principal_object_id" {
  description = "Service principal object ID"
  value       = azuread_service_principal.databricks_sp.object_id
}
