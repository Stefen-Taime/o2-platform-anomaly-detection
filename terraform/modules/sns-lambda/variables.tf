variable "project" {
  description = "Project name prefix"
  type        = string
}

variable "slack_webhook_url" {
  description = "Slack webhook URL for anomaly alerts"
  type        = string
  sensitive   = true
}
