output "all_purpose_cluster_id" {
  description = "All-purpose cluster ID"
  value       = databricks_cluster.all_purpose.cluster_id
}

output "jobs_cluster_id" {
  description = "Jobs cluster ID"
  value       = databricks_cluster.jobs.cluster_id
}

output "spot_cluster_id" {
  description = "Spot cluster ID (if enabled)"
  value       = try(databricks_cluster.spot[0].cluster_id, null)
}
