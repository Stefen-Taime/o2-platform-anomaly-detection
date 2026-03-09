output "cluster_name" {
  description = "EKS cluster name"
  value       = aws_eks_cluster.main.name
}

output "cluster_endpoint" {
  description = "EKS cluster endpoint"
  value       = aws_eks_cluster.main.endpoint
}

output "ecr_repository_url" {
  description = "ECR repository URL"
  value       = aws_ecr_repository.anomaly_detector.repository_url
}

output "eks_nodes_role_id" {
  description = "EKS nodes IAM role ID (for additional policies)"
  value       = aws_iam_role.eks_nodes.id
}
