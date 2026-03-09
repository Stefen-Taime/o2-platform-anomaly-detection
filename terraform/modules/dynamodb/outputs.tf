output "feature_store_name" {
  description = "Feature store table name"
  value       = aws_dynamodb_table.feature_store.name
}

output "feature_store_arn" {
  description = "Feature store table ARN"
  value       = aws_dynamodb_table.feature_store.arn
}

output "anomaly_scores_name" {
  description = "Anomaly scores table name"
  value       = aws_dynamodb_table.anomaly_scores.name
}

output "anomaly_scores_arn" {
  description = "Anomaly scores table ARN"
  value       = aws_dynamodb_table.anomaly_scores.arn
}
