-- =============================================================================
-- O2 Platform — Snowpipe Configuration
-- PRD §4.1 : S3 Parquet → Snowpipe → Snowflake (auto-ingestion)
-- =============================================================================

USE DATABASE O2_PLATFORM;

-- =============================================================================
-- Storage Integration (S3 → Snowflake trust)
-- Must be created by ACCOUNTADMIN
-- =============================================================================

CREATE OR REPLACE STORAGE INTEGRATION o2_s3_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::ACCOUNT_ID:role/o2-platform-snowflake-role'
  STORAGE_ALLOWED_LOCATIONS = ('s3://REPLACE_WITH_AUDIT_BUCKET/');

-- Retrieve the external ID and IAM user ARN for AWS trust policy
-- DESC INTEGRATION o2_s3_integration;

-- =============================================================================
-- External Stage (points to S3 audit trail bucket)
-- =============================================================================

CREATE OR REPLACE STAGE RAW.O2_AUDIT_STAGE
  STORAGE_INTEGRATION = o2_s3_integration
  URL = 's3://REPLACE_WITH_AUDIT_BUCKET/'
  FILE_FORMAT = (TYPE = 'PARQUET');

-- =============================================================================
-- Snowpipes — one per topic (auto-ingest from S3 event notifications)
-- =============================================================================

-- Transactions Snowpipe
CREATE OR REPLACE PIPE RAW.PIPE_TRANSACTIONS
  AUTO_INGEST = TRUE
AS
COPY INTO RAW.TRANSACTIONS (
    event_id, event_type, event_time, account_id, amount, currency,
    merchant, location, country, device_id, channel, category,
    is_online, night_transaction
)
FROM (
    SELECT
        $1:event_id::VARCHAR,
        $1:event_type::VARCHAR,
        TO_TIMESTAMP_NTZ($1:event_time::NUMBER / 1000),
        $1:account_id::VARCHAR,
        $1:amount::NUMBER(18,2),
        $1:currency::VARCHAR,
        $1:merchant::VARCHAR,
        $1:location::VARCHAR,
        $1:country::VARCHAR,
        $1:device_id::VARCHAR,
        $1:channel::VARCHAR,
        $1:category::VARCHAR,
        $1:is_online::BOOLEAN,
        $1:night_transaction::BOOLEAN
    FROM @RAW.O2_AUDIT_STAGE/topics/transactions/
)
FILE_FORMAT = (TYPE = 'PARQUET')
MATCH_BY_COLUMN_NAME = NONE;

-- Claims Snowpipe
CREATE OR REPLACE PIPE RAW.PIPE_CLAIMS
  AUTO_INGEST = TRUE
AS
COPY INTO RAW.CLAIMS (
    event_id, event_type, event_time, account_id, claim_id,
    claim_amount, claim_type, policy_id, policy_start_ts,
    description, location
)
FROM (
    SELECT
        $1:event_id::VARCHAR,
        $1:event_type::VARCHAR,
        TO_TIMESTAMP_NTZ($1:event_time::NUMBER / 1000),
        $1:account_id::VARCHAR,
        $1:claim_id::VARCHAR,
        $1:claim_amount::NUMBER(18,2),
        $1:claim_type::VARCHAR,
        $1:policy_id::VARCHAR,
        TO_TIMESTAMP_NTZ($1:policy_start_ts::NUMBER / 1000),
        $1:description::VARCHAR,
        $1:location::VARCHAR
    FROM @RAW.O2_AUDIT_STAGE/topics/claims/
)
FILE_FORMAT = (TYPE = 'PARQUET')
MATCH_BY_COLUMN_NAME = NONE;

-- Logins Snowpipe
CREATE OR REPLACE PIPE RAW.PIPE_LOGINS
  AUTO_INGEST = TRUE
AS
COPY INTO RAW.LOGINS (
    event_id, event_type, event_time, account_id, device_id,
    ip_address, user_agent, location, geo_country, geo_city,
    success, night_transaction
)
FROM (
    SELECT
        $1:event_id::VARCHAR,
        $1:event_type::VARCHAR,
        TO_TIMESTAMP_NTZ($1:event_time::NUMBER / 1000),
        $1:account_id::VARCHAR,
        $1:device_id::VARCHAR,
        $1:ip_address::VARCHAR,
        $1:user_agent::VARCHAR,
        $1:location::VARCHAR,
        $1:geo_country::VARCHAR,
        $1:geo_city::VARCHAR,
        $1:success::BOOLEAN,
        $1:night_transaction::BOOLEAN
    FROM @RAW.O2_AUDIT_STAGE/topics/logins/
)
FILE_FORMAT = (TYPE = 'PARQUET')
MATCH_BY_COLUMN_NAME = NONE;

-- Transfers Snowpipe
CREATE OR REPLACE PIPE RAW.PIPE_TRANSFERS
  AUTO_INGEST = TRUE
AS
COPY INTO RAW.TRANSFERS (
    event_id, event_type, event_time, account_id, dest_account_id,
    amount, currency, transfer_type, reference, institution_code,
    location, country, night_transaction
)
FROM (
    SELECT
        $1:event_id::VARCHAR,
        $1:event_type::VARCHAR,
        TO_TIMESTAMP_NTZ($1:event_time::NUMBER / 1000),
        $1:account_id::VARCHAR,
        $1:dest_account_id::VARCHAR,
        $1:amount::NUMBER(18,2),
        $1:currency::VARCHAR,
        $1:transfer_type::VARCHAR,
        $1:reference::VARCHAR,
        $1:institution_code::VARCHAR,
        $1:location::VARCHAR,
        $1:country::VARCHAR,
        $1:night_transaction::BOOLEAN
    FROM @RAW.O2_AUDIT_STAGE/topics/transfers/
)
FILE_FORMAT = (TYPE = 'PARQUET')
MATCH_BY_COLUMN_NAME = NONE;

-- Anomaly Scores Snowpipe
CREATE OR REPLACE PIPE RAW.PIPE_ANOMALY_SCORES
  AUTO_INGEST = TRUE
AS
COPY INTO RAW.ANOMALY_SCORES (
    account_id, event_type, rule, anomaly_score, risk_level,
    recommended_action, description, model_version, event_time,
    detected_at, metadata
)
FROM (
    SELECT
        $1:account_id::VARCHAR,
        $1:event_type::VARCHAR,
        $1:rule::VARCHAR,
        $1:anomaly_score::NUMBER(6,4),
        $1:risk_level::VARCHAR,
        $1:recommended_action::VARCHAR,
        $1:description::VARCHAR,
        $1:model_version::VARCHAR,
        TO_TIMESTAMP_NTZ($1:event_time::NUMBER / 1000),
        TO_TIMESTAMP_NTZ($1:detected_at::NUMBER / 1000),
        $1:metadata::VARIANT
    FROM @RAW.O2_AUDIT_STAGE/topics/anomalies/
)
FILE_FORMAT = (TYPE = 'PARQUET')
MATCH_BY_COLUMN_NAME = NONE;

-- =============================================================================
-- Verification queries
-- =============================================================================

-- Check pipe status:
-- SELECT SYSTEM$PIPE_STATUS('RAW.PIPE_TRANSACTIONS');

-- Check ingestion history:
-- SELECT * FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
--   TABLE_NAME => 'RAW.TRANSACTIONS', START_TIME => DATEADD(HOUR, -1, CURRENT_TIMESTAMP())
-- ));
