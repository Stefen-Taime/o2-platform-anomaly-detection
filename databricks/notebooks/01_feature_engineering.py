# Databricks notebook source
# MAGIC %md
# MAGIC # O2 Platform — Feature Engineering
# MAGIC PRD §5.1 — Features d'entree pour le modele XGBoost
# MAGIC
# MAGIC Ce notebook lit les evenements depuis S3 JSON (Kafka Connect S3 Sink)
# MAGIC et calcule les features necessaires au training du modele.
# MAGIC
# MAGIC **Note**: Databricks CE serverless blocks Spark S3 access (no sparkContext, no hadoopConfiguration).
# MAGIC We use boto3 + pandas instead.

# COMMAND ----------

# MAGIC %pip install boto3

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration — AWS S3 Access via boto3

# COMMAND ----------

import os
import boto3
import json
import io
import pandas as pd
import numpy as np

AWS_ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY_ID", "REPLACE_WITH_YOUR_ACCESS_KEY")
AWS_SECRET_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "REPLACE_WITH_YOUR_SECRET_KEY")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
S3_AUDIT_BUCKET = os.environ.get("S3_AUDIT_BUCKET", "REPLACE_WITH_YOUR_BUCKET")

s3 = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name=AWS_REGION,
)

print(f"S3 bucket: {S3_AUDIT_BUCKET}")
print("boto3 S3 client configured.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Load raw events from S3 JSON

# COMMAND ----------

def load_topic_from_s3(bucket, topic, max_files=50):
    """Load JSON files for a Kafka topic from S3, return as pandas DataFrame."""
    prefix = f"topics/{topic}/"
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1000)

    all_records = []
    file_count = 0
    for obj in response.get("Contents", []):
        if not obj["Key"].endswith(".json"):
            continue
        if file_count >= max_files:
            break

        resp = s3.get_object(Bucket=bucket, Key=obj["Key"])
        body = resp["Body"].read().decode("utf-8")

        for line in body.strip().split("\n"):
            if line:
                try:
                    all_records.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
        file_count += 1

    df = pd.DataFrame(all_records)
    print(f"  {topic}: {len(df)} records from {file_count} files")
    return df

print("Loading topics from S3...")
transactions_df = load_topic_from_s3(S3_AUDIT_BUCKET, "transactions")
claims_df = load_topic_from_s3(S3_AUDIT_BUCKET, "claims")
logins_df = load_topic_from_s3(S3_AUDIT_BUCKET, "logins")
transfers_df = load_topic_from_s3(S3_AUDIT_BUCKET, "transfers")

print(f"\nTotal: {len(transactions_df) + len(claims_df) + len(logins_df) + len(transfers_df)} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1b. Inspect schemas

# COMMAND ----------

print("--- Transactions columns ---")
print(transactions_df.dtypes)
print(f"\nSample:\n{transactions_df.head(3)}")

print("\n--- Claims columns ---")
print(claims_df.dtypes)
print(f"\nSample:\n{claims_df.head(3)}")

print("\n--- Logins columns ---")
print(logins_df.dtypes)
print(f"\nSample:\n{logins_df.head(3)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Feature Engineering — PRD §5.1

# COMMAND ----------

# Sort by account_id and event_time for windowing
transactions_df = transactions_df.sort_values(["account_id", "event_time"]).reset_index(drop=True)

# velocity_2min — count transactions per account_id in 2-minute rolling windows
def compute_velocity(group):
    """Count transactions within 2-minute window for each row."""
    times = group["event_time"].values
    counts = []
    for i, t in enumerate(times):
        window_start = t - 120000  # 2 minutes in ms
        count = ((times >= window_start) & (times <= t)).sum()
        counts.append(count)
    group["velocity_2min"] = counts
    return group

print("Computing velocity_2min...")
transactions_df = transactions_df.groupby("account_id", group_keys=False).apply(compute_velocity)

# amount_zscore — (amount - mean) / std per account
print("Computing amount_zscore...")
account_stats = transactions_df.groupby("account_id")["amount"].agg(["mean", "std"]).reset_index()
account_stats.columns = ["account_id", "avg_amount", "std_amount"]
transactions_df = transactions_df.merge(account_stats, on="account_id", how="left")
transactions_df["amount_zscore"] = np.where(
    transactions_df["std_amount"] > 0,
    (transactions_df["amount"] - transactions_df["avg_amount"]) / transactions_df["std_amount"],
    0
)

# device_risk_score — 0 if device known (seen in logins), 1 if unknown
print("Computing device_risk_score...")
known_devices = logins_df[["account_id", "device_id"]].drop_duplicates()
known_devices["_known"] = 1
transactions_df = transactions_df.merge(
    known_devices, on=["account_id", "device_id"], how="left"
)
transactions_df["device_risk_score"] = np.where(
    transactions_df["_known"] == 1, 0.0, 1.0
)
transactions_df.drop(columns=["_known"], inplace=True)

# location_anomaly — 1 if country not in usual list (CA, US)
print("Computing location_anomaly...")
transactions_df["location_anomaly"] = np.where(
    transactions_df["country"].isin(["CA", "US"]), 0, 1
)

# night_transaction — cast to integer
print("Computing night_transaction...")
transactions_df["night_transaction"] = transactions_df["night_transaction"].astype(int)

print(f"\nTransaction features computed: {len(transactions_df)} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Claims features

# COMMAND ----------

# claim_delay_days — event_time - policy_start_ts (in days)
claims_df["claim_delay_days"] = (
    (claims_df["event_time"] - claims_df["policy_start_ts"]) / (1000 * 86400)
).astype(int)

# Average claim_delay_days per account
avg_claim_delay = claims_df.groupby("account_id")["claim_delay_days"].mean().reset_index()
avg_claim_delay.columns = ["account_id", "claim_delay_days"]

print(f"Claims features: {len(avg_claim_delay)} unique accounts")
print(f"Average claim delay: {avg_claim_delay['claim_delay_days'].mean():.1f} days")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Combine all features

# COMMAND ----------

# Join transaction features with claim delay
combined = transactions_df.merge(avg_claim_delay, on="account_id", how="left")
combined["claim_delay_days"] = combined["claim_delay_days"].fillna(180).astype(int)

# Select final feature columns — PRD §5.1
feature_cols = [
    "account_id", "event_id", "event_time",
    "velocity_2min", "amount_zscore", "device_risk_score",
    "location_anomaly", "claim_delay_days", "night_transaction", "amount",
]
final_features = combined[feature_cols].copy()

print(f"Final feature rows: {len(final_features)}")
print(f"\nSample:")
print(final_features.head(10))
print(f"\nFeature stats:")
print(final_features[["velocity_2min", "amount_zscore", "device_risk_score", "location_anomaly", "claim_delay_days", "night_transaction"]].describe())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Save features for model training

# COMMAND ----------

# Save as Parquet to S3 for the training notebook to read
output_key = "features/training/features.parquet"
buffer = io.BytesIO()
final_features.to_parquet(buffer, index=False)
buffer.seek(0)

s3.put_object(Bucket=S3_AUDIT_BUCKET, Key=output_key, Body=buffer.getvalue())

print(f"Features saved to s3://{S3_AUDIT_BUCKET}/{output_key}")
print(f"Total rows: {len(final_features)}")
