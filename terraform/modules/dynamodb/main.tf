# =============================================================================
# Module: DynamoDB — Feature Store + Anomaly Scores
# =============================================================================

resource "aws_dynamodb_table" "feature_store" {
  name         = "${var.project}-feature-store"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "account_id"

  attribute {
    name = "account_id"
    type = "S"
  }

  ttl {
    attribute_name = ""
    enabled        = false
  }

  point_in_time_recovery {
    enabled = false
  }

  tags = {
    Name = "${var.project}-feature-store"
  }
}

resource "aws_dynamodb_table" "anomaly_scores" {
  name         = "${var.project}-anomaly-scores"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "account_id"
  range_key    = "detected_at"

  attribute {
    name = "account_id"
    type = "S"
  }

  attribute {
    name = "detected_at"
    type = "N"
  }

  ttl {
    attribute_name = "ttl"
    enabled        = true
  }

  point_in_time_recovery {
    enabled = false
  }

  tags = {
    Name = "${var.project}-anomaly-scores"
  }
}
