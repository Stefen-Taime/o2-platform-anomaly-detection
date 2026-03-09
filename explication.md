# O2 Platform — Complete Project Walkthrough

Document detailing the entire O2 platform construction process,
from start to finish: context, decisions, commands, problems encountered,
solutions applied, data, and results.

---

## 1. Context and Objective

### Why this project?

The O2 Platform is a POC (Proof of Concept) for real-time anomaly detection
inspired by Coinbase's O2 architecture. Coinbase published their O2 system which
processes 1 billion events per day to detect fraud, money laundering,
and suspicious behaviors.

### For whom?

The project targets three potential clients:
- **BNC** (Banque Nationale du Canada) — transaction fraud
- **Intact Financial Data Lab** — insurance claim fraud
- **CDPQ** (Caisse de depot) — suspicious behavior detection

### The 3 core principles

Coinbase defined 3 architectural principles that we reproduce:

1. **Logic-on-Write**: enrich each event at ingestion time
   (no lazy loading). The DynamoDB profile for each account is fetched
   **before** scoring the anomaly.

2. **Logical Clocks**: process by `event_time` (event timestamp),
   not by `processing_time` (when Kafka receives the message). This guarantees
   deterministic output: the same events always produce the same result.

3. **In-Memory Processing**: maintain minimal state per account (counters,
   averages, last device) in a fast store (DynamoDB < 10ms).

---

## 2. AWS Infrastructure (Terraform)

### What we provision

All infrastructure is defined in modular Terraform (7 modules).
Region: `us-east-1`.

#### VPC Module (`terraform/modules/vpc/`)
- VPC `10.0.0.0/16`
- 2 public subnets (for EC2 MLflow, EC2 Jenkins)
- 2 private subnets (for EKS nodes)
- Internet Gateway + NAT Gateway
- Security Groups: MLflow (port 5000), Jenkins (port 8080), EKS nodes

#### S3 Module (`terraform/modules/s3/`)
- `o2-platform-audit-trail-<random>`: JSON storage via Kafka Connect S3 Sink
  (structure: `topics/{topic}/year=YYYY/month=MM/day=DD/hour=HH/`)
- `o2-platform-artifacts-<random>`: ML models, Python wheels, deployment configs,
  MLflow artifacts (`mlflow-artifacts/`)

#### DynamoDB Module (`terraform/modules/dynamodb/`)
- Table `o2-platform-feature-store`
- Partition key: `account_id` (String)
- On-demand mode (no throughput to manage)
- Serves as the online feature store for Logic-on-Write enrichment

#### SNS-Lambda Module (`terraform/modules/sns_lambda/`)
- SNS topic `o2-platform-anomaly-alerts`
- Lambda `o2-platform-slack-notifier` (Python, `terraform/lambda/index.py`)
- The Lambda receives SNS notifications and posts to Slack with an emoji
  per risk level (CRITICAL, HIGH, MEDIUM, LOW)

#### EKS Module (`terraform/modules/eks/`)
- EKS cluster `o2-platform` (Kubernetes 1.29)
- Node group: 3x `t3.medium` (desiredSize=3, min=1, max=5)
- OIDC provider for IRSA (IAM Roles for Service Accounts)
- Addons: CoreDNS, kube-proxy, vpc-cni, aws-ebs-csi-driver

#### EC2 Module (`terraform/modules/ec2/`)
- **MLflow**: `t3.micro`, Amazon Linux 2023
  - Public IP: `<from terraform output>`
  - Private IP: `<from terraform output>` (used by Jenkins)
  - User-data installs Python 3.11, pip, MLflow 2.14.0, boto3
  - Systemd service `mlflow.service`: listens on `0.0.0.0:5000`
  - Backend: SQLite (`/opt/mlflow/backend/mlflow.db`)
  - Artifact root: `s3://<ARTIFACTS_BUCKET>/mlflow-artifacts`

- **Jenkins**: `t3.small`, Amazon Linux 2023
  - Public IP: `<from terraform output>`
  - User-data installs Java 17, Jenkins, AWS CLI v2, kubectl, Python 3.11, MLflow, Docker
  - Default credentials: configured in the Groovy init script
  - Init Groovy script creates the admin user and disables the setup wizard
  - Environment variables: `MLFLOW_TRACKING_URI`, `ARTIFACTS_BUCKET`, `AWS_DEFAULT_REGION`

#### Snowflake Module (`terraform/modules/snowflake/`)
- Database `O2_PLATFORM`
- Schemas `RAW` and `MART`
- Storage integration `o2_s3_integration` (cross-account AWS/Snowflake trust)

### Terraform Commands

```bash
cd terraform
ssh-keygen -t ed25519 -f o2-key -N ""    # Generate SSH key
terraform init -upgrade
terraform plan -out=tfplan
terraform apply tfplan
```

Important outputs:
- `eks_cluster_name` = `o2-platform`
- `ecr_repository_url` = `<ACCOUNT_ID>.dkr.ecr.us-east-1.amazonaws.com/o2-platform/anomaly-detector`
- `mlflow_public_ip` = `<from terraform output>`
- `jenkins_public_ip` = `<from terraform output>`

---

## 3. Kafka on EKS (Strimzi)

### Why Strimzi + KRaft?

Strimzi is a Kubernetes operator for Kafka. We use version 0.51.0
with Kafka 4.2.0 in **KRaft** mode (no ZooKeeper). KRaft mode simplifies
the architecture by combining controller and broker roles in a single process.

### Strimzi Installation

```bash
kubectl create namespace kafka
kubectl create -f 'https://strimzi.io/install/latest?namespace=kafka' -n kafka
kubectl wait --for=condition=ready pod -l name=strimzi-cluster-operator -n kafka --timeout=300s
```

### Kafka Cluster Deployment

File: `k8s/kafka-cluster.yaml`

- **KafkaNodePool** `o2-pool`: 1 replica, roles `[controller, broker]`, 10Gi storage, 512Mi-1Gi memory
- **Kafka** `o2-kafka`: Kafka 4.2.0, listeners:
  - `plain`: port 9092, type internal (used by apps within the cluster)
  - `external`: port 9094, type nodeport
    - Bootstrap NodePort: 31094
    - Broker 0: advertisedHost=localhost, advertisedPort=31095
- Configuration: replication.factor=1, retention 24h, timestamps CreateTime

```bash
kubectl apply -f k8s/kafka-cluster.yaml
kubectl wait kafka/o2-kafka --for=condition=Ready --timeout=600s -n kafka
```

### Kafka Topics

File: `k8s/kafka-topics.yaml`

5 topics, all 3 partitions, 1 replica:
- `transactions` (retention 24h) — banking operations
- `claims` (retention 24h) — insurance claims
- `logins` (retention 24h) — user sessions
- `transfers` (retention 24h) — inter-account transfers
- `anomalies` (retention 48h) — scoring output

```bash
kubectl apply -f k8s/kafka-topics.yaml
```

### Issue: Port-forward for local access

For the local Go generator to write to Kafka on EKS, two port-forwards
are needed:

```bash
# 1. Bootstrap (broker discovery)
kubectl port-forward svc/o2-kafka-kafka-external-bootstrap 9094:9094 -n kafka &

# 2. Broker 0 (actual message production)
kubectl port-forward svc/o2-kafka-o2-pool-0 31095:9094 -n kafka &
```

**Why 2 port-forwards?** The bootstrap responds with the broker's advertised address
(localhost:31095). If only the bootstrap is port-forwarded, the Go client cannot
reach the broker. The 2nd port-forward maps local 31095 to the broker pod's port 9094.

---

## 4. Kafka Streams Application (Java)

### Application Architecture

The `o2-anomaly-detector` application is a Kafka Streams processor that:
1. Consumes the 4 input topics (transactions, claims, logins, transfers)
2. Enriches each event with the DynamoDB profile (Logic-on-Write)
3. Scores anomalies according to 7 rules
4. Produces detected anomalies to the `anomalies` topic
5. Updates the DynamoDB profile (minimal state)

### Java Files

#### `AnomalyDetectorApp.java` (main)
- Configures Kafka Streams (application-id, bootstrap-servers, timestamp extractor)
- Builds the topology: `stream(4 topics) -> mapValues(enrich) -> flatMapValues(score) -> to(anomalies)`
- Starts an HTTP health check server on port 8080 (`/health`)
- Environment variables: `KAFKA_BOOTSTRAP_SERVERS`, `INPUT_TOPICS`, `OUTPUT_TOPIC`, `DYNAMODB_TABLE`

#### `EventTimeExtractor.java` (Principle 2: Logical Clocks)
- Custom Kafka Streams `TimestampExtractor`
- Reads `event_time` (epoch ms) from each record's JSON
- Falls back to `timestamp`, then to partition time
- Ensures aggregation windows use the event's time

#### `DynamoDBEnricher.java` (Principle 1: Logic-on-Write)
- `getProfile(accountId)`: DynamoDB GetItem, returns a JsonNode with features
  (tx_count_2min, tx_sum_1h, avg_tx_amount, last_device_id, claim_count_30d, etc.)
- `updateProfile(accountId, enrichedEvent)`: DynamoDB UpdateItem
  - transaction -> increments tx_count_2min, tx_sum_1h, tx_sum_24h
  - login -> updates last_device_id, last_login_ts
  - claim -> increments claim_count_30d
  - transfer -> updates last_transfer_ts
- Unknown profile = `known: false` (new account, features at zero)

#### `AnomalyScorer.java` (Rule-based scoring)
7 detection rules:

| Rule | Type | Threshold | Risk Level |
|------|------|-----------|------------|
| `HIGH_VELOCITY` | transaction | >5 tx in 2 min | CRITICAL/HIGH |
| `AMOUNT_OUT_OF_PROFILE` | transaction | >3x the average | CRITICAL/HIGH |
| `NEW_ACCOUNT_HIGH_AMOUNT` | transaction | new account + >$10K | CRITICAL |
| `EARLY_CLAIM` | claim | <7 days after policy | HIGH |
| `MULTIPLE_CLAIMS` | claim | 3+ claims in 30d | HIGH |
| `UNKNOWN_DEVICE` | login | device != last known | MEDIUM |
| `HIGH_VALUE_TRANSFER` | transfer | >$50K | HIGH |
| `TRANSFER_BURST` | transfer | >3 in 2 min (layering) | HIGH |

The `riskScore` is a float 0-1 computed dynamically (e.g., HIGH_VELOCITY = 0.5 + 0.1 per additional tx).

#### `Anomaly.java` (output model)
- POJO with Builder pattern
- Fields: account_id, event_type, rule, anomaly_score, risk_level,
  recommended_action (automatically derived from risk_level), description,
  model_version, event_time, detected_at, metadata (JSON string)
- `recommended_action`:
  - CRITICAL -> BLOCK
  - HIGH -> REVIEW
  - MEDIUM -> MONITOR
  - LOW/other -> ALLOW

### Deployment on EKS

File: `k8s/kafka-streams-app.yaml`

- Deployment `o2-anomaly-detector`, 1 replica
- Image: `<ACCOUNT_ID>.dkr.ecr.us-east-1.amazonaws.com/o2-platform/anomaly-detector:v2-glibc`
- Health checks: readiness (`/health`, 30s initial), liveness (`/health`, 60s initial)
- Env vars: KAFKA_BOOTSTRAP_SERVERS, DYNAMODB_TABLE, INPUT_TOPICS, OUTPUT_TOPIC, SNS_TOPIC_ARN

---

## 5. Go Event Generator

### Why Go?

Go offers lightweight goroutines for parallel generation across 4 topics,
compilation to a native binary (no JVM), and the franz-go
(kgo) library is the most performant Kafka client in Go.

### How it works

File: `generator/main.go`

- CLI flags: `-bootstrap` (default localhost:9094), `-rate` (default 100 evt/s), `-duration` (0 = infinite)
- 4 goroutines, throughput distribution:
  - `transactions`: 40% (40 evt/s at rate=100)
  - `logins`: 25%
  - `transfers`: 20%
  - `claims`: 15% (remainder)
- Each goroutine has a `time.Ticker` at a fixed interval
- Stats displayed every 5 seconds (total events, avg/s)

### Simulated data pool

File: `generator/internal/common.go`

- **500 accounts** (ACC-000001 to ACC-000500)
- **10% suspicious** (ACC-000001, ACC-000011, ACC-000021...) — generates abnormal patterns
- 9 devices (6 normal + 3 "unknown")
- 3 currencies: CAD, USD, EUR
- 12 locations (8 Canadian + NY + London + Lagos + Moscow)
- Suspicious accounts have 40% chance of using an unknown device
  and 30% chance of transacting from a foreign country

### Transactions (`internal/transactions.go`)

Each transaction contains:
- event_id (UUID v4), event_type="transaction", event_time (epoch ms)
- account_id, amount, currency, merchant, location, country
- device_id, channel (POS/ATM/ONLINE/MOBILE/WIRE), category
- is_online (70% true), night_transaction (true between 00h-05h)

Amounts:
- Normal: $10 - $2,000
- Suspicious (30% chance): $5,000 - $100,000

### Build and run

```bash
cd generator
go build -o /tmp/o2-generator .
/tmp/o2-generator -bootstrap localhost:9094 -rate 100
```

Typical output:
```
O2 Event Generator — bootstrap=localhost:9094 rate=100/s
Connected to Kafka
  [transactions] generating at 40 events/sec
  [logins] generating at 25 events/sec
  [transfers] generating at 20 events/sec
  [claims] generating at 15 events/sec
[21:04:05] Total: 500 events | Avg: 100/s
```

---

## 6. Kafka Connect S3 Sink

### Role

The S3 Sink copies all messages from the 5 Kafka topics to S3 in JSON (NDJSON) format.
Files are partitioned by hour (TimeBasedPartitioner).

### Custom Docker image

File: `kafka-connect-s3/Dockerfile`

- Base: `quay.io/strimzi/kafka:0.51.0-kafka-4.2.0`
- Installs the Confluent S3 Sink Connector 10.5.25 and its dependencies
- Uses Maven to resolve dependencies, excluding Kafka libs
  already present in the base image (avoids version conflicts)

### Connector configuration

File: `k8s/kafka-connect-s3.yaml`

- KafkaConnect `o2-s3-connect`: custom ECR image, 1 replica, JSON config
- KafkaConnector `s3-sink`:
  - Topics: transactions, claims, logins, transfers, anomalies
  - Bucket: `o2-platform-audit-trail-<random>`
  - Format: `JsonFormat` (one JSON object per line)
  - Partitioner: `TimeBasedPartitioner` (hourly, UTC)
  - Flush: 100 records OR 60 seconds (whichever comes first)
  - Path: `topics/{topic}/year=YYYY/month=MM/day=DD/hour=HH/`

### Result in S3

Example files created:
```
s3://o2-platform-audit-trail-<random>/
  topics/transactions/year=2026/month=03/day=09/hour=20/
    transactions+0+0000000000.json
    transactions+0+0000000100.json
    ...
  topics/anomalies/year=2026/month=03/day=09/hour=20/
    anomalies+0+0000000000.json
    ...
```

---

## 7. Snowflake — Data Warehouse

### Why Snowflake?

Snowflake enables analysis of the audit trail data stored in S3
with standard SQL queries. Snowpipes with auto-ingestion automatically load
new JSON files via SQS notifications.

### Database and schemas

File: `snowflake/ddl.sql`

- Database: `O2_PLATFORM`
- Schema `RAW`: 5 tables (one per Kafka topic)
  - `TRANSACTIONS`: 14 columns (event_id PK, account_id, amount, currency, merchant, location, country, device_id, channel, category, is_online, night_transaction, event_time, loaded_at)
  - `CLAIMS`: 12 columns
  - `LOGINS`: 13 columns
  - `TRANSFERS`: 14 columns
  - `ANOMALY_SCORES`: 12 columns (account_id, event_type, rule, anomaly_score, risk_level, recommended_action, description, model_version, event_time, detected_at, metadata VARIANT, loaded_at)
- Schema `MART`: 2 analytical tables
  - `FCT_ANOMALIES`: aggregation by account/risk_level/event_type/rule
  - `RPT_DAILY_FRAUD`: daily report with counts by risk level

### Snowpipe Configuration

File: `snowflake/snowpipe.sql`

1. **Storage Integration** `o2_s3_integration`:
   - Cross-account trust: the Snowflake IAM role accesses the S3 bucket
   - Snowflake IAM user: `arn:aws:iam::<SNOWFLAKE_AWS_ACCOUNT>:user/<SNOWFLAKE_USER>`
   - External ID: `<from DESC INTEGRATION output>`
   - Trust policy configuration on the AWS IAM role

2. **Stage** `RAW.O2_AUDIT_STAGE`:
   - Points to `s3://o2-platform-audit-trail-<random>/`
   - File format: JSON (NDJSON, strip outer array=false, ignore UTF-8 errors)

3. **5 Snowpipes** (SQS auto-ingest):
   - `PIPE_TRANSACTIONS`: reads from `@O2_AUDIT_STAGE/topics/transactions/`
   - `PIPE_CLAIMS`: reads from `@O2_AUDIT_STAGE/topics/claims/`
   - `PIPE_LOGINS`: reads from `@O2_AUDIT_STAGE/topics/logins/`
   - `PIPE_TRANSFERS`: reads from `@O2_AUDIT_STAGE/topics/transfers/`
   - `PIPE_ANOMALY_SCORES`: reads from `@O2_AUDIT_STAGE/topics/anomalies/`

   Each pipe performs a `COPY INTO` with JSON transformations (`$1:field::TYPE`),
   including epoch ms timestamp conversion to `TIMESTAMP_NTZ` and parsing
   of the metadata field as VARIANT.

4. **SQS Notification**:
   - SQS ARN generated by Snowflake: `<from SHOW PIPES output>`
   - Configured as event notification on the S3 bucket (ObjectCreated)

### Issue and solution: Cross-account trust

**Problem**: The Snowflake account and the AWS account are different accounts.
The storage integration `DESC INTEGRATION o2_s3_integration` returns a
`STORAGE_AWS_IAM_USER_ARN` and a `STORAGE_AWS_EXTERNAL_ID` specific to Snowflake.

**Solution**: Configure a trust policy on the AWS IAM role
`o2-platform-snowflake-role` to authorize the Snowflake principal to assume this role
with the provided external ID.

### Issue and solution: JSON vs Parquet format

**Initial problem**: The Snowpipe template used PARQUET format,
but the Kafka Connect S3 Sink writes in JSON (NDJSON).

**Solution**: Recreated the file format and stage with `TYPE = 'JSON'`,
and adapted the COPY INTO statements to use `$1:field` notation instead
of Parquet columns.

### Loaded data

After approximately 2-3 hours of generation at 100 evt/s:

| Table | Rows |
|-------|------|
| RAW.TRANSACTIONS | ~370K |
| RAW.CLAIMS | ~139K |
| RAW.LOGINS | ~232K |
| RAW.TRANSFERS | ~186K |
| RAW.ANOMALY_SCORES | ~872K |
| MART.FCT_ANOMALIES | 2,554 |
| MART.RPT_DAILY_FRAUD | 5 |

### Snowflake queries used

```sql
-- Verify data
SELECT 'TRANSACTIONS' AS tbl, COUNT(*) AS cnt FROM RAW.TRANSACTIONS
UNION ALL SELECT 'ANOMALY_SCORES', COUNT(*) FROM RAW.ANOMALY_SCORES;

-- Volume per hour
SELECT DATE_TRUNC('HOUR', event_time) AS hour_bucket,
       COUNT(*) AS tx_count, ROUND(AVG(amount), 2) AS avg_amount
FROM RAW.TRANSACTIONS GROUP BY hour_bucket ORDER BY hour_bucket DESC LIMIT 10;

-- Top anomalies
SELECT * FROM MART.FCT_ANOMALIES ORDER BY anomaly_count DESC LIMIT 10;

-- Daily report
SELECT * FROM MART.RPT_DAILY_FRAUD ORDER BY report_date DESC;
```

---

## 8. Machine Learning — Databricks CE + XGBoost

### Databricks CE Constraint

Databricks Community Edition (free tier) has a limitation: serverless mode
does not provide access to `sparkContext` or `hadoopConfiguration`.
It is impossible to read S3 directly with Spark.

**Solution**: Use `boto3` (AWS SDK for Python) + `pandas` instead
of PySpark. The notebook accesses S3 directly via AWS credentials.

### Notebook 1: Feature Engineering

File: `databricks/notebooks/01_feature_engineering.py`

This notebook reads raw JSON files from S3 and computes 6 features:

1. **velocity_2min**: transaction count per account within a 2-minute window
   (naive O(n^2) loop over timestamps grouped by account_id)
2. **amount_zscore**: z-score of amount = (amount - mean) / std per account
3. **device_risk_score**: 1 if the transaction's device_id is not in
   the list of known devices (logins), 0 otherwise
4. **location_anomaly**: 1 if the country is not CA or US
5. **claim_delay_days**: number of days between event_time and policy_start_ts
6. **night_transaction**: cast of boolean to integer (0/1)

Final features are saved as Parquet in S3:
`s3://o2-platform-audit-trail-<random>/features/training/features.parquet`

### Notebook 2: XGBoost Training

File: `databricks/notebooks/02_xgboost_training.py`

1. Reads features.parquet from S3 (synthetic fallback if S3 not available)
2. **Label generation** (rule-based, same logic as AnomalyScorer.java):
   - 6 binary signals: high_velocity (>=5), high_zscore (>=2.5),
     unknown_device (==1), foreign_location (==1), night_tx (==1),
     rapid_claim (<=7 days)
   - `is_anomaly = 1` if 2+ active signals (same threshold as Kafka Streams scoring)
3. **Train/test split**: 80/20, stratified on is_anomaly
4. **XGBoost**: XGBClassifier with parameters:
   - n_estimators=200, max_depth=6, learning_rate=0.1
   - subsample=0.8, colsample_bytree=0.8
   - scale_pos_weight = n_normal / n_anomaly (imbalance compensation)
5. **Metrics** logged in MLflow:
   - AUC, Precision, Recall, F1, false_positive_rate
   - classification_report (text)
   - feature_importance (JSON)
6. **Artifact**: `model/xgboost_model.json` saved in MLflow

### Databricks CE Workaround: Registry URI

Databricks CE has a bug: `spark.mlflow.modelRegistryUri is not available`.

**Solution** (line 31 of the notebook):
```python
mlflow.tracking._model_registry.utils._get_registry_uri_from_spark_session = lambda: "databricks-uc"
```
This monkey-patch bypasses the bug by forcing the registry URI.

### Training Results

| Metric | Value | PRD Threshold |
|--------|-------|---------------|
| AUC | 1.0000 | >= 0.82 |
| Precision | 1.0000 | >= 0.85 |
| Recall | 1.0000 | >= 0.80 |
| F1 | 1.0000 | - |
| FPR | 0.0000 | - |

Note: The perfect metrics (1.0) are explained by the fact that labels
are generated by the same rules the generator uses to create anomalous patterns.
In production, manually annotated labels would be used.

---

## 9. MLflow on EC2

### Role

MLflow serves as the central registry for ML experiments. It persists
**outside** of Databricks CE (which loses its data when the cluster shuts down).

### Configuration

- EC2 `t3.micro`, public IP `<from terraform output>`, private IP `<from terraform output>`
- MLflow 2.14.0, installed via pip (Python 3.11, system-wide, no venv)
- Backend: SQLite (`/opt/mlflow/backend/mlflow.db`)
- Artifact root: `s3://<ARTIFACTS_BUCKET>/mlflow-artifacts`
- Port: 5000
- Systemd service `mlflow.service`

### Issue: MLflow venv not found

**Problem**: The Terraform user-data created a virtualenv in `/opt/mlflow/venv/`,
but after reboot, the service tried to activate a venv that didn't exist.

**Solution**: MLflow was reinstalled system-wide (`python3.11 -m pip install mlflow`).
The systemd service was updated to use `/usr/local/bin/mlflow` directly.

### Issue: S3 Artifacts

**Problem**: MLflow artifacts were initially stored locally
(`/opt/mlflow/artifacts/`). Jenkins could not access them.

**Solution**: Restart MLflow with `--default-artifact-root s3://...` and
`--serve-artifacts` for proxy mode. Fix script: `/tmp/fix-mlflow.sh`.

### MLflow Data

- Experiment: `/O2-Anomaly-Detection`
- 1 FINISHED run
- Metrics: AUC=1.0, Precision=1.0, Recall=1.0, F1=1.0
- Artifact: `model/xgboost_model.json`

---

## 10. Jenkins Blue-Green Pipeline

### Role

Jenkins orchestrates the Blue-Green deployment of the ML model:
- Retrieves the latest MLflow model
- Deploys it on EKS (Green)
- Validates the metrics
- Switches to production (Blue) or rolls back

### Jenkins Configuration

- EC2 `t3.small`, IP `<from terraform output>:8080`
- Jenkins 2.541.2, Java 17 (Corretto)
- Credentials:
  - `ARTIFACTS_BUCKET`: `<from terraform output>`
  - `MLFLOW_TRACKING_URI`: `http://<MLFLOW_PRIVATE_IP>:5000` (MLflow private IP)
  - `SLACK_WEBHOOK`: `<your Slack webhook URL>`
- Job: `o2-blue-green-deploy` (inline Pipeline script, not SCM)

### The 4 Jenkinsfile Stages

#### Stage 1: BUILD
- Generates a version number: `v{BUILD_NUMBER}-{yyyyMMdd-HHmmss}`
- Builds the Python wheel: `python3.11 -m build --wheel`
- Generates `deploy-config.yaml` with validation thresholds
- Uploads wheel + config to S3

#### Stage 2: DEPLOY GREEN
- Executes a Python script (written via `writeFile` to `/tmp/mlflow_get_run.py`)
  to retrieve the run_id of the latest FINISHED MLflow run
- Copies the model artifact from MLflow artifacts (S3) to the version folder
- Updates the EKS deployment: `kubectl set env deployment/o2-anomaly-detector MODEL_VERSION=... MODEL_S3_PATH=... MODEL_RUN_ID=...`
- Waits for the rolling update to complete: `kubectl rollout status`

#### Stage 3: VALIDATE
- Executes a Python script (`/tmp/mlflow_validate.py`) that:
  - Retrieves the 2 latest MLflow runs (Green = latest, Blue = previous)
  - Compares metrics against thresholds:
    - AUC >= 0.82 (absolute)
    - AUC Green >= 98% of AUC Blue (no regression > 2%)
    - Precision >= 0.85
    - Recall >= 0.80
  - Returns a JSON with `approved: true/false`
- On failure: `error()` triggers the post-failure (rollback)

#### Stage 4: SWITCH BLUE-GREEN
- Annotates the K8s deployment with version metadata
- Saves a `deployment-manifest.json` to S3 (version, metrics, timestamp)
- Green becomes Blue (production)

#### Post-actions
- **Success**: Slack notification with metrics
- **Failure**: `kubectl rollout undo` + Slack failure notification

### Major issue: filter_string quoting

**Problem**: Builds #3 through #6 failed with the MLflow error:
```
INVALID_PARAMETER_VALUE: Parameter value is either not quoted or
unidentified quote types used for string value FINISHED
```

The cause: the `filter_string="attributes.status = 'FINISHED'"` in the Python
script required single quotes around `FINISHED`. But the Groovy GString interpolation
+ shell execution (`sh "python3.11 -c '...'"`) consumed the quotes.

**Failed attempts**:
- Build #3: `filter_string="attributes.status = 'FINISHED'"` (quotes consumed by shell)
- Build #4: Same problem, different syntax
- Build #5: Same
- Build #6: `chr(39)` to generate quotes (Python SyntaxError)

**Solution (Build #7)**:
Use Jenkins `writeFile` to write the Python script to `/tmp/`,
then execute it separately. The script is no longer interpolated by Groovy or the shell:

```groovy
writeFile file: '/tmp/mlflow_get_run.py', text: """import mlflow
...
filter_string="attributes.status = 'FINISHED'"
..."""

sh "python3.11 /tmp/mlflow_get_run.py"
```

This approach completely avoids quoting issues.

### Build History

| Build | Status | Issue |
|-------|--------|-------|
| #1 | SUCCESS | Empty (test) |
| #2 | FAILURE | AccessDeniedException on workspace @tmp |
| #3 | FAILURE | filter_string quoting (FINISHED without quotes) |
| #4 | FAILURE | Same quoting problem |
| #5 | FAILURE | Same quoting problem |
| #6 | FAILURE | chr(39) SyntaxError |
| #7 | SUCCESS | writeFile fix. First complete deploy. v7-20260309-202133 |
| #8 | SUCCESS | Second deploy. v8-20260309-211240. Slack notification OK |

### Build #8 — Final Result

```
=== BUILD: Model package v8-20260309-211240 ===
=== DEPLOY GREEN: v8-20260309-211240 ===
  Latest MLflow run: <run_id>
  Green deployment rolled out successfully
=== VALIDATE: Checking MLflow metrics ===
  Green AUC:       1.0
  Blue AUC:        0
  Green F1:        1.0
  Approved:        true
  Reason:          All thresholds met
=== SWITCH: Blue -> Green ===
  Switch complete. v8-20260309-211240 is now Blue (production).
Pipeline SUCCESS: v8-20260309-211240 deployed to production
```

Slack notification received:
```
O2 Model Deployed
Version: v8-20260309-211240
AUC: 1.0 (was 0)
F1: 1.0
Status: Blue-Green switch complete
```

---

## 11. Alerting (SNS + Lambda + Slack)

### Alert Flow

1. The anomaly-detector (Kafka Streams) detects an anomaly
2. Publishes to the SNS topic `o2-platform-anomaly-alerts`
3. The Lambda `o2-platform-slack-notifier` is invoked
4. The Lambda formats a Slack message with an emoji per risk level:
   - CRITICAL -> rotating_light
   - HIGH -> warning
   - MEDIUM -> large_yellow_circle
   - LOW -> white_circle
5. POST to the Slack webhook

### Lambda (`terraform/lambda/index.py`)

- Python runtime
- Reads SNS records, parses the message JSON
- Formats a Slack message with details: account_id, event_type, rule, risk level, description
- Error handling: if no SLACK_WEBHOOK_URL, logs to console

---

## 12. Complete Flow Summary

```
1. Terraform creates all AWS infrastructure
          |
2. Strimzi deploys Kafka 4.2.0 (KRaft) on EKS
          |
3. Go Generator produces ~100 evt/s across 4 topics
          |
4. Kafka Streams enriches (DynamoDB) + scores (7 rules) + publishes anomalies
          |
5. Kafka Connect S3 Sink copies all 5 topics as JSON to S3
          |
6. Snowflake Snowpipe automatically ingests JSON into 5 RAW tables
          |
7. MART tables are refreshed (FCT_ANOMALIES, RPT_DAILY_FRAUD)
          |
8. Databricks CE reads features from S3, trains XGBoost, logs to MLflow
          |
9. Jenkins Blue-Green deploys the new model on EKS
   - Retrieves MLflow run
   - Deploy Green (kubectl set env)
   - Validates metrics (AUC, Precision, Recall)
   - Switch Blue/Green or Rollback
   - Slack notification
          |
10. Real-time anomalies -> SNS -> Lambda -> Slack
```

---

## 13. Lessons Learned and Key Considerations

### Kafka on EKS
- **KRaft mode** (Kafka 4.2.0) greatly simplifies: no ZooKeeper, single combined process
- **Dual port-forward** required for local access (bootstrap + broker)
- **Strimzi NodePool**: the external listener must specify `advertisedHost: localhost` and `advertisedPort: 31095` for the local client to reach the broker

### Databricks CE
- **No Spark S3** in serverless mode: use boto3 + pandas
- **Registry URI bug**: monkey-patch required
- **Ephemeral**: clusters shut down after inactivity, hence the importance of persistent MLflow on EC2

### Jenkins + Groovy + Shell
- **Triple quoting** (Groovy GString -> shell -> Python) is a nightmare
- **Universal solution**: `writeFile` + separate execution
- **Jenkins on EC2**: the jenkins user needs kubectl permissions (aws eks update-kubeconfig)

### Snowflake
- **Cross-account trust**: 2 different AWS accounts (Snowflake vs project)
- **Format**: properly align the S3 Sink format (JSON) with the Snowflake file format
- **Snowpipe SQS**: verify that S3 notifications are properly configured with the correct SQS ARN

### MLflow
- **System-wide vs venv**: simpler without venv on a dedicated instance
- **S3 artifact root**: configure `--serve-artifacts` for proxy mode

---

## 14. Resources and Links

| Resource | URL |
|----------|-----|
| MLflow UI | http://<MLFLOW_IP>:5000 |
| Jenkins UI | http://<JENKINS_IP>:8080 |
| Databricks CE | https://community.cloud.databricks.com |
| Snowflake | Account `<YOUR_ACCOUNT>`, User `<YOUR_USER>` |
| AWS Console | `<YOUR_ACCOUNT_ID>`, us-east-1 |
| EKS Cluster | o2-platform |
| S3 Audit Trail | `<from terraform output>` |
| S3 Artifacts | `<from terraform output>` |
| DynamoDB Table | o2-platform-feature-store |
| SNS Topic | o2-platform-anomaly-alerts |
| ECR Repo | `<ACCOUNT_ID>.dkr.ecr.us-east-1.amazonaws.com/o2-platform/anomaly-detector` |
| SSH Key | terraform/o2-key (generated locally, not committed) |
