# =============================================================================
# Variables — Root
# =============================================================================

# --- AWS ---
variable "region" {
  description = "AWS region"
  type        = string
  default     = "us-east-1"
}

variable "project" {
  description = "Project name prefix"
  type        = string
  default     = "o2-platform"
}

variable "vpc_cidr" {
  description = "VPC CIDR block"
  type        = string
  default     = "10.0.0.0/16"
}

variable "eks_node_instance_type" {
  description = "EKS node instance type"
  type        = string
  default     = "t3.small"
}

variable "eks_node_count" {
  description = "Number of EKS nodes"
  type        = number
  default     = 3
}

variable "mlflow_instance_type" {
  description = "MLflow EC2 instance type"
  type        = string
  default     = "t3.micro"
}

variable "jenkins_instance_type" {
  description = "Jenkins EC2 instance type"
  type        = string
  default     = "t3.medium"
}

variable "my_ip" {
  description = "Your public IP for SSH/UI access (CIDR format, e.g. 1.2.3.4/32)"
  type        = string
  default     = "0.0.0.0/0"
}

variable "slack_webhook_url" {
  description = "Slack webhook URL for anomaly alerts (leave empty to skip)"
  type        = string
  default     = ""
  sensitive   = true
}

# --- Snowflake ---
variable "snowflake_account" {
  description = "Snowflake account identifier (e.g. ORGID-ACCOUNTNAME)"
  type        = string
}

variable "snowflake_user" {
  description = "Snowflake username"
  type        = string
}

variable "snowflake_password" {
  description = "Snowflake password"
  type        = string
  sensitive   = true
}

variable "snowflake_role" {
  description = "Snowflake role"
  type        = string
  default     = "ACCOUNTADMIN"
}

variable "snowflake_storage_role_arn" {
  description = "IAM role ARN for Snowflake S3 storage integration (configure after initial apply)"
  type        = string
  default     = ""
}
