variable "project" {
  description = "Project name prefix"
  type        = string
}

variable "region" {
  description = "AWS region"
  type        = string
}

variable "mlflow_instance_type" {
  description = "MLflow EC2 instance type"
  type        = string
}

variable "jenkins_instance_type" {
  description = "Jenkins EC2 instance type"
  type        = string
}

variable "public_subnet_ids" {
  description = "Public subnet IDs"
  type        = list(string)
}

variable "mlflow_sg_id" {
  description = "MLflow security group ID"
  type        = string
}

variable "jenkins_sg_id" {
  description = "Jenkins security group ID"
  type        = string
}

variable "artifacts_bucket_id" {
  description = "Artifacts S3 bucket ID"
  type        = string
}

variable "artifacts_bucket_arn" {
  description = "Artifacts S3 bucket ARN"
  type        = string
}
