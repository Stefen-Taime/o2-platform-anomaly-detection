output "database_name" {
  description = "Snowflake database name"
  value       = snowflake_database.o2.name
}

output "storage_integration_name" {
  description = "Storage integration name (empty if not configured)"
  value       = length(snowflake_storage_integration.s3_integration) > 0 ? snowflake_storage_integration.s3_integration[0].name : ""
}

output "pipe_transactions_name" {
  description = "Transactions Snowpipe name (empty if not configured)"
  value       = length(snowflake_pipe.transactions) > 0 ? snowflake_pipe.transactions[0].name : ""
}

output "pipe_anomaly_scores_name" {
  description = "Anomaly scores Snowpipe name (empty if not configured)"
  value       = length(snowflake_pipe.anomaly_scores) > 0 ? snowflake_pipe.anomaly_scores[0].name : ""
}
