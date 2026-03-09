variable "audit_trail_bucket_id" {
  description = "S3 audit trail bucket ID"
  type        = string
}

variable "snowflake_storage_role_arn" {
  description = "IAM role ARN for Snowflake storage integration (create after getting Snowflake external ID)"
  type        = string
  default     = ""
}
