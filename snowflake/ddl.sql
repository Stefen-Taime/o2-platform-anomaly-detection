-- =============================================================================
-- O2 Platform — Snowflake DDL
-- PRD §4.1 Flux analytique : S3 Parquet → Snowpipe → Snowflake → Superset
-- =============================================================================

-- Database
CREATE DATABASE IF NOT EXISTS O2_PLATFORM;
USE DATABASE O2_PLATFORM;

-- Schemas
CREATE SCHEMA IF NOT EXISTS RAW;
CREATE SCHEMA IF NOT EXISTS MART;

-- =============================================================================
-- RAW Layer — ingested from S3 via Snowpipe
-- =============================================================================

-- RAW.TRANSACTIONS — bank transactions
CREATE OR REPLACE TABLE RAW.TRANSACTIONS (
    event_id          VARCHAR(64),
    event_type        VARCHAR(32),
    event_time        TIMESTAMP_NTZ,
    account_id        VARCHAR(32),
    amount            NUMBER(18,2),
    currency          VARCHAR(3),
    merchant          VARCHAR(128),
    location          VARCHAR(128),
    country           VARCHAR(8),
    device_id         VARCHAR(64),
    channel           VARCHAR(16),
    category          VARCHAR(32),
    is_online         BOOLEAN,
    night_transaction BOOLEAN,
    _loaded_at        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- RAW.CLAIMS — insurance claims
CREATE OR REPLACE TABLE RAW.CLAIMS (
    event_id        VARCHAR(64),
    event_type      VARCHAR(32),
    event_time      TIMESTAMP_NTZ,
    account_id      VARCHAR(32),
    claim_id        VARCHAR(32),
    claim_amount    NUMBER(18,2),
    claim_type      VARCHAR(32),
    policy_id       VARCHAR(32),
    policy_start_ts TIMESTAMP_NTZ,
    description     VARCHAR(256),
    location        VARCHAR(128),
    _loaded_at      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- RAW.LOGINS — user login events
CREATE OR REPLACE TABLE RAW.LOGINS (
    event_id          VARCHAR(64),
    event_type        VARCHAR(32),
    event_time        TIMESTAMP_NTZ,
    account_id        VARCHAR(32),
    device_id         VARCHAR(64),
    ip_address        VARCHAR(45),
    user_agent        VARCHAR(256),
    location          VARCHAR(128),
    geo_country       VARCHAR(8),
    geo_city          VARCHAR(64),
    success           BOOLEAN,
    night_transaction BOOLEAN,
    _loaded_at        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- RAW.TRANSFERS — inter-account transfers
CREATE OR REPLACE TABLE RAW.TRANSFERS (
    event_id         VARCHAR(64),
    event_type       VARCHAR(32),
    event_time       TIMESTAMP_NTZ,
    account_id       VARCHAR(32),
    dest_account_id  VARCHAR(32),
    amount           NUMBER(18,2),
    currency         VARCHAR(3),
    transfer_type    VARCHAR(32),
    reference        VARCHAR(64),
    institution_code VARCHAR(8),
    location         VARCHAR(128),
    country          VARCHAR(8),
    night_transaction BOOLEAN,
    _loaded_at       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- RAW.ANOMALY_SCORES — anomaly detection results (from DynamoDB export or Kafka anomalies topic)
CREATE OR REPLACE TABLE RAW.ANOMALY_SCORES (
    account_id         VARCHAR(32),
    event_type         VARCHAR(32),
    rule               VARCHAR(64),
    anomaly_score      NUMBER(6,4),
    risk_level         VARCHAR(16),
    recommended_action VARCHAR(16),
    description        VARCHAR(256),
    model_version      VARCHAR(32),
    event_time         TIMESTAMP_NTZ,
    detected_at        TIMESTAMP_NTZ,
    metadata           VARIANT,
    _loaded_at         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- =============================================================================
-- MART Layer — aggregated views for Superset dashboards
-- =============================================================================

-- MART.FCT_ANOMALIES — aggregated anomalies per account + risk level
CREATE OR REPLACE TABLE MART.FCT_ANOMALIES AS
SELECT
    a.account_id,
    a.risk_level,
    a.event_type,
    a.rule,
    COUNT(*)                          AS anomaly_count,
    AVG(a.anomaly_score)              AS avg_anomaly_score,
    MAX(a.anomaly_score)              AS max_anomaly_score,
    MIN(a.detected_at)                AS first_detected,
    MAX(a.detected_at)                AS last_detected,
    CURRENT_TIMESTAMP()               AS refreshed_at
FROM RAW.ANOMALY_SCORES a
GROUP BY a.account_id, a.risk_level, a.event_type, a.rule;

-- MART.RPT_DAILY_FRAUD — daily fraud report
CREATE OR REPLACE TABLE MART.RPT_DAILY_FRAUD AS
SELECT
    DATE_TRUNC('DAY', a.detected_at)  AS report_date,
    a.event_type,
    a.risk_level,
    COUNT(*)                          AS alert_count,
    COUNT(DISTINCT a.account_id)      AS unique_accounts,
    AVG(a.anomaly_score)              AS avg_score,
    SUM(CASE WHEN a.risk_level = 'CRITICAL' THEN 1 ELSE 0 END) AS critical_count,
    SUM(CASE WHEN a.risk_level = 'HIGH' THEN 1 ELSE 0 END)     AS high_count,
    SUM(CASE WHEN a.risk_level = 'MEDIUM' THEN 1 ELSE 0 END)   AS medium_count,
    CURRENT_TIMESTAMP()               AS refreshed_at
FROM RAW.ANOMALY_SCORES a
GROUP BY DATE_TRUNC('DAY', a.detected_at), a.event_type, a.risk_level;
