output "sns_topic_arn" {
  description = "SNS topic ARN for anomaly alerts"
  value       = aws_sns_topic.anomaly_alerts.arn
}
