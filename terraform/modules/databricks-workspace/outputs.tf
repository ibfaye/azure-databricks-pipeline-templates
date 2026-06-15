output "workspace_id" {
  description = "Databricks workspace numeric ID"
  value       = azurerm_databricks_workspace.main.workspace_id
}

output "workspace_resource_id" {
  description = "Azure resource ID (for provider auth)"
  value       = azurerm_databricks_workspace.main.id
}

output "workspace_url" {
  description = "Databricks workspace URL"
  value       = "https://${azurerm_databricks_workspace.main.workspace_url}"
}

output "metastore_id" {
  description = "Unity Catalog metastore ID (auto-assigned by region)"
  value       = null  # Auto-assigned — use Databricks UI to find the ID
}

output "service_principal_client_id" {
  description = "Service principal client ID for pipeline authentication"
  value       = azuread_application.databricks_sp.client_id
}

output "service_principal_object_id" {
  description = "Service principal object ID"
  value       = azuread_service_principal.databricks_sp.object_id
}
