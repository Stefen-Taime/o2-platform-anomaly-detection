variable "project" {
  description = "Project name prefix"
  type        = string
}

variable "public_subnet_ids" {
  description = "Public subnet IDs"
  type        = list(string)
}

variable "private_subnet_ids" {
  description = "Private subnet IDs"
  type        = list(string)
}

variable "eks_additional_sg_id" {
  description = "EKS additional security group ID"
  type        = string
}

variable "eks_node_instance_type" {
  description = "EKS node instance type"
  type        = string
}

variable "eks_node_count" {
  description = "Number of EKS nodes"
  type        = number
}

variable "dynamodb_table_arns" {
  description = "List of DynamoDB table ARNs for node IAM policy"
  type        = list(string)
}

variable "s3_bucket_arns" {
  description = "List of S3 bucket ARNs (bucket + bucket/*) for node IAM policy"
  type        = list(string)
}

variable "sns_topic_arn" {
  description = "SNS topic ARN for anomaly alerts"
  type        = string
}
