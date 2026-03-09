# =============================================================================
# Module: S3 Buckets
# =============================================================================

resource "random_string" "bucket_suffix" {
  length  = 8
  special = false
  upper   = false
}

# Audit Trail — Parquet files from Kafka S3 Sink Connector
resource "aws_s3_bucket" "audit_trail" {
  bucket        = "${var.project}-audit-trail-${random_string.bucket_suffix.result}"
  force_destroy = true

  tags = {
    Name = "${var.project}-audit-trail"
  }
}

resource "aws_s3_bucket_versioning" "audit_trail" {
  bucket = aws_s3_bucket.audit_trail.id
  versioning_configuration {
    status = "Disabled"
  }
}

# Artifacts — ML models, wheels, configs
resource "aws_s3_bucket" "artifacts" {
  bucket        = "${var.project}-artifacts-${random_string.bucket_suffix.result}"
  force_destroy = true

  tags = {
    Name = "${var.project}-artifacts"
  }
}

resource "aws_s3_bucket_versioning" "artifacts" {
  bucket = aws_s3_bucket.artifacts.id
  versioning_configuration {
    status = "Enabled"
  }
}
