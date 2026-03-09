output "mlflow_public_ip" {
  description = "MLflow server public IP"
  value       = aws_instance.mlflow.public_ip
}

output "mlflow_private_ip" {
  description = "MLflow server private IP"
  value       = aws_instance.mlflow.private_ip
}

output "jenkins_public_ip" {
  description = "Jenkins server public IP"
  value       = aws_instance.jenkins.public_ip
}
