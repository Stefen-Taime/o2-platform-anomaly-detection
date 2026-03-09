# =============================================================================
# Outputs
# =============================================================================

# --- EKS ---
output "eks_cluster_name" {
  description = "EKS cluster name"
  value       = module.eks.cluster_name
}

output "eks_cluster_endpoint" {
  description = "EKS cluster endpoint"
  value       = module.eks.cluster_endpoint
}

output "eks_update_kubeconfig" {
  description = "Command to update kubeconfig"
  value       = "aws eks update-kubeconfig --name ${module.eks.cluster_name} --region ${var.region}"
}

# --- ECR ---
output "ecr_repository_url" {
  description = "ECR repository URL for anomaly detector image"
  value       = module.eks.ecr_repository_url
}

# --- EC2 ---
output "mlflow_public_ip" {
  description = "MLflow server public IP"
  value       = module.ec2.mlflow_public_ip
}

output "mlflow_url" {
  description = "MLflow UI URL"
  value       = "http://${module.ec2.mlflow_public_ip}:5000"
}

output "jenkins_public_ip" {
  description = "Jenkins server public IP"
  value       = module.ec2.jenkins_public_ip
}

output "jenkins_url" {
  description = "Jenkins UI URL"
  value       = "http://${module.ec2.jenkins_public_ip}:8080"
}

# --- DynamoDB ---
output "dynamodb_table_name" {
  description = "DynamoDB feature store table name"
  value       = module.dynamodb.feature_store_name
}

output "dynamodb_anomaly_scores_table" {
  description = "DynamoDB anomaly scores table name"
  value       = module.dynamodb.anomaly_scores_name
}

# --- SNS ---
output "sns_anomaly_topic_arn" {
  description = "SNS topic ARN for anomaly alerts"
  value       = module.sns_lambda.sns_topic_arn
}

# --- S3 ---
output "s3_audit_trail_bucket" {
  description = "S3 bucket for audit trail (Parquet)"
  value       = module.s3.audit_trail_bucket_id
}

output "s3_artifacts_bucket" {
  description = "S3 bucket for ML artifacts"
  value       = module.s3.artifacts_bucket_id
}

# --- Snowflake ---
output "snowflake_database" {
  description = "Snowflake database name"
  value       = module.snowflake.database_name
}

# --- Summary ---
output "summary" {
  description = "Quick access summary"
  value = <<-EOT

    ╔══════════════════════════════════════════════════════╗
    ║         O2 Platform — Deployment Summary            ║
    ╠══════════════════════════════════════════════════════╣
    ║                                                      ║
    ║  EKS Cluster : ${module.eks.cluster_name}
    ║  MLflow      : http://${module.ec2.mlflow_public_ip}:5000
    ║  Jenkins     : http://${module.ec2.jenkins_public_ip}:8080
    ║  Jenkins Creds: admin / <set in user-data>
    ║                                                      ║
    ║  ECR Repo    : ${module.eks.ecr_repository_url}
    ║  DynamoDB    : ${module.dynamodb.feature_store_name}
    ║  S3 Audit    : ${module.s3.audit_trail_bucket_id}
    ║  S3 Artifacts: ${module.s3.artifacts_bucket_id}
    ║  Snowflake   : ${module.snowflake.database_name}
    ║                                                      ║
    ╚══════════════════════════════════════════════════════╝

  EOT
}
