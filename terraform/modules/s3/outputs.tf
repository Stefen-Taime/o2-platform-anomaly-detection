output "audit_trail_bucket_id" {
  description = "Audit trail S3 bucket ID"
  value       = aws_s3_bucket.audit_trail.id
}

output "audit_trail_bucket_arn" {
  description = "Audit trail S3 bucket ARN"
  value       = aws_s3_bucket.audit_trail.arn
}

output "artifacts_bucket_id" {
  description = "Artifacts S3 bucket ID"
  value       = aws_s3_bucket.artifacts.id
}

output "artifacts_bucket_arn" {
  description = "Artifacts S3 bucket ARN"
  value       = aws_s3_bucket.artifacts.arn
}
