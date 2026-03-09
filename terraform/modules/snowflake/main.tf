# =============================================================================
# Module: Snowflake — Database, Schemas, Tables, Storage Integration, Snowpipe
# =============================================================================

# --- Database ---
resource "snowflake_database" "o2" {
  name    = "O2_PLATFORM"
  comment = "Coinbase O2 anomaly detection analytics"
}

# --- Schemas ---
resource "snowflake_schema" "raw" {
  database = snowflake_database.o2.name
  name     = "RAW"
  comment  = "Raw event data ingested via Snowpipe"
}

resource "snowflake_schema" "mart" {
  database = snowflake_database.o2.name
  name     = "MART"
  comment  = "Aggregated analytics tables"
}

# --- Warehouse ---
resource "snowflake_warehouse" "o2_wh" {
  name           = "O2_WH"
  warehouse_size = "X-Small"
  auto_suspend   = 60
  auto_resume    = true
  comment        = "O2 Platform warehouse"
}

# --- RAW Tables ---
resource "snowflake_table" "raw_transactions" {
  database = snowflake_database.o2.name
  schema   = snowflake_schema.raw.name
  name     = "TRANSACTIONS"

  column {
    name = "DATA"
    type = "VARIANT"
  }
  column {
    name    = "LOADED_AT"
    type    = "TIMESTAMP_NTZ"
    default {
      expression = "CURRENT_TIMESTAMP()"
    }
  }
}

resource "snowflake_table" "raw_claims" {
  database = snowflake_database.o2.name
  schema   = snowflake_schema.raw.name
  name     = "CLAIMS"

  column {
    name = "DATA"
    type = "VARIANT"
  }
  column {
    name    = "LOADED_AT"
    type    = "TIMESTAMP_NTZ"
    default {
      expression = "CURRENT_TIMESTAMP()"
    }
  }
}

resource "snowflake_table" "raw_logins" {
  database = snowflake_database.o2.name
  schema   = snowflake_schema.raw.name
  name     = "LOGINS"

  column {
    name = "DATA"
    type = "VARIANT"
  }
  column {
    name    = "LOADED_AT"
    type    = "TIMESTAMP_NTZ"
    default {
      expression = "CURRENT_TIMESTAMP()"
    }
  }
}

resource "snowflake_table" "raw_transfers" {
  database = snowflake_database.o2.name
  schema   = snowflake_schema.raw.name
  name     = "TRANSFERS"

  column {
    name = "DATA"
    type = "VARIANT"
  }
  column {
    name    = "LOADED_AT"
    type    = "TIMESTAMP_NTZ"
    default {
      expression = "CURRENT_TIMESTAMP()"
    }
  }
}

resource "snowflake_table" "raw_anomaly_scores" {
  database = snowflake_database.o2.name
  schema   = snowflake_schema.raw.name
  name     = "ANOMALY_SCORES"

  column {
    name = "DATA"
    type = "VARIANT"
  }
  column {
    name    = "LOADED_AT"
    type    = "TIMESTAMP_NTZ"
    default {
      expression = "CURRENT_TIMESTAMP()"
    }
  }
}

# --- MART Tables ---
resource "snowflake_table" "fct_anomalies" {
  database = snowflake_database.o2.name
  schema   = snowflake_schema.mart.name
  name     = "FCT_ANOMALIES"

  column {
    name = "ANOMALY_ID"
    type = "VARCHAR(36)"
  }
  column {
    name = "ACCOUNT_ID"
    type = "VARCHAR(20)"
  }
  column {
    name = "EVENT_TYPE"
    type = "VARCHAR(20)"
  }
  column {
    name = "RULE"
    type = "VARCHAR(50)"
  }
  column {
    name = "ANOMALY_SCORE"
    type = "FLOAT"
  }
  column {
    name = "RISK_LEVEL"
    type = "VARCHAR(10)"
  }
  column {
    name = "RECOMMENDED_ACTION"
    type = "VARCHAR(20)"
  }
  column {
    name = "EVENT_TIME"
    type = "TIMESTAMP_NTZ"
  }
  column {
    name = "DETECTED_AT"
    type = "TIMESTAMP_NTZ"
  }
}

resource "snowflake_table" "rpt_daily_fraud" {
  database = snowflake_database.o2.name
  schema   = snowflake_schema.mart.name
  name     = "RPT_DAILY_FRAUD"

  column {
    name = "REPORT_DATE"
    type = "DATE"
  }
  column {
    name = "EVENT_TYPE"
    type = "VARCHAR(20)"
  }
  column {
    name = "RISK_LEVEL"
    type = "VARCHAR(10)"
  }
  column {
    name = "ANOMALY_COUNT"
    type = "NUMBER(10,0)"
  }
  column {
    name = "AVG_SCORE"
    type = "FLOAT"
  }
  column {
    name = "MAX_SCORE"
    type = "FLOAT"
  }
}

# --- Storage Integration (S3 → Snowflake) ---
# Only created when snowflake_storage_role_arn is provided
resource "snowflake_storage_integration" "s3_integration" {
  count = var.snowflake_storage_role_arn != "" ? 1 : 0

  name    = "O2_S3_INTEGRATION"
  type    = "EXTERNAL_STAGE"
  enabled = true

  storage_allowed_locations = ["s3://${var.audit_trail_bucket_id}/"]
  storage_provider          = "S3"

  storage_aws_role_arn = var.snowflake_storage_role_arn

  comment = "S3 integration for O2 audit trail Parquet ingestion"
}

# --- External Stage ---
resource "snowflake_stage" "audit_trail" {
  count = var.snowflake_storage_role_arn != "" ? 1 : 0

  database            = snowflake_database.o2.name
  schema              = snowflake_schema.raw.name
  name                = "AUDIT_TRAIL_STAGE"
  storage_integration = snowflake_storage_integration.s3_integration[0].name
  url                 = "s3://${var.audit_trail_bucket_id}/topics/"

  file_format = "TYPE = PARQUET"

  comment = "External stage for S3 audit trail Parquet files"
}

# --- Snowpipes (only created when storage integration is available) ---
resource "snowflake_pipe" "transactions" {
  count = var.snowflake_storage_role_arn != "" ? 1 : 0

  database = snowflake_database.o2.name
  schema   = snowflake_schema.raw.name
  name     = "TRANSACTIONS_PIPE"

  auto_ingest = true

  copy_statement = <<-SQL
    COPY INTO ${snowflake_database.o2.name}.${snowflake_schema.raw.name}.${snowflake_table.raw_transactions.name}
    FROM (SELECT $1, CURRENT_TIMESTAMP() FROM @${snowflake_database.o2.name}.${snowflake_schema.raw.name}.${snowflake_stage.audit_trail[0].name}/transactions/)
    FILE_FORMAT = (TYPE = PARQUET)
  SQL

  comment = "Auto-ingest pipe for transactions topic"
}

resource "snowflake_pipe" "claims" {
  count = var.snowflake_storage_role_arn != "" ? 1 : 0

  database = snowflake_database.o2.name
  schema   = snowflake_schema.raw.name
  name     = "CLAIMS_PIPE"

  auto_ingest = true

  copy_statement = <<-SQL
    COPY INTO ${snowflake_database.o2.name}.${snowflake_schema.raw.name}.${snowflake_table.raw_claims.name}
    FROM (SELECT $1, CURRENT_TIMESTAMP() FROM @${snowflake_database.o2.name}.${snowflake_schema.raw.name}.${snowflake_stage.audit_trail[0].name}/claims/)
    FILE_FORMAT = (TYPE = PARQUET)
  SQL

  comment = "Auto-ingest pipe for claims topic"
}

resource "snowflake_pipe" "logins" {
  count = var.snowflake_storage_role_arn != "" ? 1 : 0

  database = snowflake_database.o2.name
  schema   = snowflake_schema.raw.name
  name     = "LOGINS_PIPE"

  auto_ingest = true

  copy_statement = <<-SQL
    COPY INTO ${snowflake_database.o2.name}.${snowflake_schema.raw.name}.${snowflake_table.raw_logins.name}
    FROM (SELECT $1, CURRENT_TIMESTAMP() FROM @${snowflake_database.o2.name}.${snowflake_schema.raw.name}.${snowflake_stage.audit_trail[0].name}/logins/)
    FILE_FORMAT = (TYPE = PARQUET)
  SQL

  comment = "Auto-ingest pipe for logins topic"
}

resource "snowflake_pipe" "transfers" {
  count = var.snowflake_storage_role_arn != "" ? 1 : 0

  database = snowflake_database.o2.name
  schema   = snowflake_schema.raw.name
  name     = "TRANSFERS_PIPE"

  auto_ingest = true

  copy_statement = <<-SQL
    COPY INTO ${snowflake_database.o2.name}.${snowflake_schema.raw.name}.${snowflake_table.raw_transfers.name}
    FROM (SELECT $1, CURRENT_TIMESTAMP() FROM @${snowflake_database.o2.name}.${snowflake_schema.raw.name}.${snowflake_stage.audit_trail[0].name}/transfers/)
    FILE_FORMAT = (TYPE = PARQUET)
  SQL

  comment = "Auto-ingest pipe for transfers topic"
}

resource "snowflake_pipe" "anomaly_scores" {
  count = var.snowflake_storage_role_arn != "" ? 1 : 0

  database = snowflake_database.o2.name
  schema   = snowflake_schema.raw.name
  name     = "ANOMALY_SCORES_PIPE"

  auto_ingest = true

  copy_statement = <<-SQL
    COPY INTO ${snowflake_database.o2.name}.${snowflake_schema.raw.name}.${snowflake_table.raw_anomaly_scores.name}
    FROM (SELECT $1, CURRENT_TIMESTAMP() FROM @${snowflake_database.o2.name}.${snowflake_schema.raw.name}.${snowflake_stage.audit_trail[0].name}/anomalies/)
    FILE_FORMAT = (TYPE = PARQUET)
  SQL

  comment = "Auto-ingest pipe for anomaly scores topic"
}
