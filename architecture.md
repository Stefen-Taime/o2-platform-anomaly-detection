# O2 Platform — Technical Architecture

## Overview

Real-time anomaly detection platform inspired by Coinbase's O2 system.
Targets banking transactions (BNC), insurance claims (Intact Financial),
and suspicious behaviors (CDPQ) with a complete event-driven architecture.

Current POC throughput: ~100 events/second across 4 Kafka topics.

---

## The 3 Core Coinbase Principles

### 1. Logic-on-Write
Immediate enrichment at ingestion in Kafka Streams.
The account's historical profile is retrieved from DynamoDB (online feature store)
**before** any scoring. No lazy enrichment at read time.

```
Raw event --> Kafka --> Kafka Streams (DynamoDB enrichment) --> Enriched event --> Scoring
```

Implementation: `DynamoDBEnricher.java` performs a DynamoDB `GetItem` for each event,
then an `UpdateItem` after scoring (profile update).

### 2. Logical Clocks
Processing by `event_time` (event timestamp), not `processing_time`
(arrival time on Kafka). Deterministic output guaranteed.

Implementation: `EventTimeExtractor.java` — custom `TimestampExtractor` that reads the
`event_time` field (epoch ms) from the event's JSON. Falls back to `timestamp` then `partitionTime`.

### 3. In-Memory Processing
Minimal state per `account_id` in DynamoDB (online feature store):
- `tx_count_2min` — transaction count in the last 2 minutes
- `tx_sum_1h` / `tx_sum_24h` — sum of amounts over 1h / 24h
- `avg_tx_amount` — historical average transaction amount
- `last_device_id` — last known device
- `last_login_ts` — last login timestamp
- `claim_count_30d` — claim count over 30 days
- `policy_start_ts` — insurance policy start date

RocksDB used as local state store in Kafka Streams (10 MB cache, 1s commit).

---

## Physical Architecture

```
+------------------------------------------------------------------------+
|                           AWS (us-east-1)                               |
|  Account: <YOUR_AWS_ACCOUNT_ID>                                         |
|                                                                         |
|  +-------------------------------------------------------------------+ |
|  |                    VPC 10.0.0.0/16                                 | |
|  |                                                                    | |
|  |  +--------------------------------------------------------------+ | |
|  |  |         EKS Cluster "o2-platform" (3x t3.medium)             | | |
|  |  |         Namespace: kafka                                      | | |
|  |  |                                                               | | |
|  |  |  +--------------------+  +-------------------------------+    | | |
|  |  |  | Kafka 4.2.0        |  | Kafka Streams App             |    | | |
|  |  |  | (Strimzi 0.51.0)   |  | o2-anomaly-detector           |    | | |
|  |  |  | KRaft mode         |  | Image: ECR v2-glibc           |    | | |
|  |  |  | 1 broker combined  |  |                               |    | | |
|  |  |  |                    |  | - AnomalyDetectorApp.java     |    | | |
|  |  |  | Topics:            |  | - DynamoDBEnricher.java       |    | | |
|  |  |  |  transactions (3p) |  | - AnomalyScorer.java          |    | | |
|  |  |  |  claims       (3p) |  | - EventTimeExtractor.java     |    | | |
|  |  |  |  logins       (3p) |  |                               |    | | |
|  |  |  |  transfers    (3p) |  | Input:  4 topics              |    | | |
|  |  |  |  anomalies    (3p) |  | Output: anomalies topic       |    | | |
|  |  |  +--------------------+  +-------------------------------+    | | |
|  |  |                                                               | | |
|  |  |  +-------------------------------+  +--------------------+    | | |
|  |  |  | Kafka Connect S3 Sink         |  | Kafka UI           |    | | |
|  |  |  | Strimzi KafkaConnect          |  | provectuslabs      |    | | |
|  |  |  | Image: ECR v2                 |  | :8080              |    | | |
|  |  |  | Format: JSON (NDJSON)         |  +--------------------+    | | |
|  |  |  | Flush: 100 records / 60s      |                            | | |
|  |  |  | Partition: TimeBasedPartitioner|                            | | |
|  |  |  +-------------------------------+                            | | |
|  |  +--------------------------------------------------------------+ | |
|  |                                                                    | |
|  |  +----------------+  +----------------+  +--------------------+    | |
|  |  | EC2 t3.micro   |  | EC2 t3.small   |  | DynamoDB           |    | |
|  |  | MLflow 2.14.0  |  | Jenkins 2.541  |  | o2-platform-       |    | |
|  |  | Python 3.11    |  | Java 17        |  |  feature-store     |    | |
|  |  | SQLite backend |  | Blue-Green     |  | PK: account_id     |    | |
|  |  | S3 artifacts   |  | Pipeline       |  | Mode: on-demand    |    | |
|  |  | :5000          |  | :8080          |  | Latency < 10ms     |    | |
|  |  | IP: <MLFLOW_IP> | | IP: <JENKINS_IP> |                     |    | |
|  |  | Priv: <PRIV_IP> | |                |  |                    |    | |
|  |  +----------------+  +----------------+  +--------------------+    | |
|  |                                                                    | |
|  |  +--------------------------------------------------------------+ | |
|  |  | S3 Buckets                                                    | | |
|  |  |  - o2-platform-audit-trail-<random>/    (JSON via S3 Sink)    | | |
|  |  |    topics/{transactions,claims,logins,transfers,anomalies}/   | | |
|  |  |  - o2-platform-artifacts-<random>/     (ML models, wheels)    | | |
|  |  |    mlflow-artifacts/  models/  features/                      | | |
|  |  +--------------------------------------------------------------+ | |
|  |                                                                    | |
|  |  +--------------------------------------------------------------+ | |
|  |  | SNS + Lambda                                                  | | |
|  |  |  SNS: o2-platform-anomaly-alerts                              | | |
|  |  |  Lambda: o2-platform-slack-notifier (Python, index.py)        | | |
|  |  |  --> Slack Webhook (real-time alerts)                         | | |
|  |  +--------------------------------------------------------------+ | |
|  +-------------------------------------------------------------------+ |
+------------------------------------------------------------------------+

        ^                                    ^
        | Kafka bootstrap                    | MLflow API
        | NodePort :9094                     | Databricks CE
        | + broker :31095                    |
        |                                    |
+-------+--------+                  +--------+---------+
| Mac Local      |                  | Databricks CE    |
| Go Generator   |                  | (free tier)      |
| franz-go/kgo   |                  | 01_feature_eng.py|
| 4 goroutines   |                  | 02_xgboost.py   |
| ~100 evt/s     |                  | boto3 + pandas   |
| Snappy compress|                  | XGBoost 2.x      |
+----------------+                  +------------------+

        ^
        | Snowpipe SQS
        | Auto-ingest
        |
+-------+--------+
| Snowflake      |
| <YOUR_ACCOUNT> |
| DB: O2_PLATFORM|
| RAW (5 tables) |
| MART (2 tables)|
+----------------+
```

---

## Data Flow

### 1. Ingestion (Logic-on-Write)
```
Go Generator (Mac local, ~100 evt/s)
    |
    |-- transactions (40%) --+
    |-- logins      (25%) ---+
    |-- transfers   (20%) ---+--> Kafka 4.2.0 (Strimzi/KRaft/EKS)
    |-- claims      (15%) ---+         |
                                       v
                              Kafka Streams App
                                       |
                              +--------+--------+
                              | 1. Parse JSON    |
                              | 2. Extract       |
                              |    event_time    |
                              |    (Logical      |
                              |     Clocks)      |
                              | 3. GetItem       |
                              |    DynamoDB      |
                              |    (Logic-on-    |
                              |     Write)       |
                              | 4. Enrich event  |
                              |    + profile     |
                              | 5. Score rules   |
                              |    (AnomalyScorer|
                              |     .java)       |
                              | 6. UpdateItem    |
                              |    DynamoDB      |
                              +--------+--------+
                                       |
                          +------------+------------+
                          v            v            v
                    anomalies     DynamoDB      SNS Topic
                    (topic)       (profile      --> Lambda
                       |          update)       --> Slack
                       v
               Kafka Connect S3 Sink
               (JSON, all 5 topics)
                       |
                       v
               S3 Audit Trail Bucket
               topics/{topic}/year=.../month=.../day=.../hour=.../
                       |
                       v
               Snowflake Snowpipe (SQS auto-ingest)
               RAW.TRANSACTIONS / CLAIMS / LOGINS / TRANSFERS / ANOMALY_SCORES
                       |
                       v
               MART.FCT_ANOMALIES  +  MART.RPT_DAILY_FRAUD
```

### 2. ML Training (Databricks CE)
```
S3 Audit Trail (topics/*)
        |
        v
01_feature_engineering.py (Databricks notebook)
  - boto3 reads JSON files from S3
  - Computes: velocity_2min, amount_zscore, device_risk_score,
    location_anomaly, claim_delay_days, night_transaction
  - Saves features.parquet to S3
        |
        v
02_xgboost_training.py (Databricks notebook)
  - Reads features.parquet from S3
  - Labels: rule-based (same logic as AnomalyScorer.java, threshold 2+ signals)
  - Train: XGBClassifier(n_estimators=200, max_depth=6, lr=0.1)
  - Metrics: AUC, Precision, Recall, F1, FPR
  - Log MLflow (Databricks CE internal)
  - Save: model/xgboost_model.json as artifact
```

### 3. Blue-Green Deployment (Jenkins)
```
Jenkins (EC2, port 8080)
        |
        v
Jenkinsfile — 4 stages:
  Stage 1: BUILD
    - python3.11 -m build --wheel
    - deploy-config.yaml (thresholds: AUC>=0.82, Precision>=0.85, Recall>=0.80)
    - Upload wheel + config --> S3 artifacts
        |
  Stage 2: DEPLOY GREEN
    - Python script (writeFile --> /tmp/) to query MLflow EC2
    - Retrieves run_id of the latest FINISHED run
    - Copies xgboost_model.json from MLflow artifacts to S3 artifacts
    - kubectl set env deployment/o2-anomaly-detector MODEL_VERSION=...
    - kubectl rollout status (waits for rolling update to complete)
        |
  Stage 3: VALIDATE
    - Python script (writeFile --> /tmp/) to compare metrics
    - Green AUC vs Blue AUC (max 2% regression)
    - Absolute thresholds: AUC>=0.82, Precision>=0.85, Recall>=0.80
    - On failure: error() --> stage 4 rollback
        |
  Stage 4: SWITCH BLUE-GREEN
    - kubectl annotate deployment (model-version, deployment-color=blue)
    - Save deployment-manifest.json to S3
    - Slack notification (success or failure + rollback)
```

---

## Tech Stack

| Layer | Choice | Version | Justification |
|-------|--------|---------|---------------|
| Cloud | AWS (us-east-1) | - | Consistent with BNC/CDPQ/Intact |
| IaC | Terraform (modular) | ~5.0 | 7 modules: vpc, s3, dynamodb, sns_lambda, eks, ec2, snowflake |
| Streaming | Kafka (Strimzi/KRaft) on EKS | 4.2.0 | No ZooKeeper, combined controller+broker mode |
| Stream Processing | Kafka Streams (Java) | - | Same EKS cluster, real-time enrichment + scoring |
| Generator | Go (franz-go/kgo) | - | Parallel goroutines, Snappy compression, StickyKeyPartitioner |
| ML Training | Databricks CE + XGBoost | 2.x | Free tier, boto3+pandas (no Spark S3 on CE) |
| Feature Store | DynamoDB (on-demand) | - | Latency < 10ms, PK = account_id |
| Experiment Tracking | MLflow on EC2 | 2.14.0 | SQLite backend, S3 artifact root, persists outside Databricks |
| CI/CD | Jenkins Blue-Green | 2.541 | 4-stage pipeline, EKS deployment, MLflow validation |
| Audit Trail | S3 JSON via Kafka Connect | - | Confluent S3 Sink 10.5.25, TimeBasedPartitioner |
| Data Warehouse | Snowflake | - | Snowpipe auto-ingest SQS, RAW + MART schemas |
| Alerting | SNS + Lambda + Slack | - | Real-time anomaly notifications |

---

## Kafka Topics

| Topic | Partitions | Retention | Content | Detected Anomalies |
|-------|-----------|-----------|---------|---------------------|
| `transactions` | 3 | 24h | Banking operations | HIGH_VELOCITY, AMOUNT_OUT_OF_PROFILE, NEW_ACCOUNT_HIGH_AMOUNT |
| `claims` | 3 | 24h | Insurance claims | EARLY_CLAIM (<7d), MULTIPLE_CLAIMS (3+/30d) |
| `logins` | 3 | 24h | User sessions | UNKNOWN_DEVICE |
| `transfers` | 3 | 24h | Inter-account transfers | HIGH_VALUE_TRANSFER (>$50K), TRANSFER_BURST (layering) |
| `anomalies` | 3 | 48h | All detected anomalies | Scoring output (sink to S3 + Snowflake) |

---

## Snowflake Schema

### RAW (raw ingestion via Snowpipe)
- `RAW.TRANSACTIONS` — event_id, account_id, amount, currency, merchant, location, country, device_id, channel, category, is_online, night_transaction
- `RAW.CLAIMS` — event_id, account_id, claim_id, claim_amount, claim_type, policy_id, policy_start_ts, description, location
- `RAW.LOGINS` — event_id, account_id, device_id, ip_address, user_agent, location, geo_country, geo_city, success, night_transaction
- `RAW.TRANSFERS` — event_id, account_id, dest_account_id, amount, currency, transfer_type, reference, institution_code, location, country, night_transaction
- `RAW.ANOMALY_SCORES` — account_id, event_type, rule, anomaly_score, risk_level, recommended_action, description, model_version, event_time, detected_at, metadata

### MART (analytical tables)
- `MART.FCT_ANOMALIES` — aggregation by account_id / risk_level / event_type / rule (count, avg/max score, first/last detected)
- `MART.RPT_DAILY_FRAUD` — daily report by event_type / risk_level (alert_count, unique_accounts, critical/high/medium counts)

---

## Estimated Costs (4h session)

| Resource | Type | Cost/hour |
|----------|------|-----------|
| EKS control plane | managed | $0.10 |
| 3x t3.medium | EKS nodes | $0.12 |
| 1x t3.micro | MLflow EC2 | $0.01 |
| 1x t3.small | Jenkins EC2 | $0.02 |
| DynamoDB | on-demand | ~$0.00 |
| S3 | storage + requests | ~$0.00 |
| Snowflake | CE (credits) | ~$0.00 |
| Go generator | Local Mac | $0.00 |
| **Total 4h** | | **~$1.00** |
