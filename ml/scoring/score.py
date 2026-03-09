"""
O2 Platform — Model Scoring Script

This runs as a Databricks job (deployed via Jenkins Blue-Green pipeline).
It loads the latest XGBoost model from MLflow and scores a batch of events.

In the POC, scoring is done:
  1. Real-time: Rule-based in Kafka Streams (AnomalyScorer.java)
  2. Batch: XGBoost via this script (scheduled or triggered by Jenkins)

The batch scoring results are written back to DynamoDB and logged to MLflow.
"""

import os
import sys
import json
import argparse
from datetime import datetime

import mlflow
import mlflow.xgboost
import numpy as np
import pandas as pd
import boto3


def load_model(mlflow_uri: str, model_name: str, experiment: str):
    """Load the latest production model from MLflow."""
    mlflow.set_tracking_uri(mlflow_uri)

    # Get the latest version
    client = mlflow.tracking.MlflowClient()
    versions = client.search_model_versions(f"name='{model_name}'")

    if not versions:
        raise RuntimeError(f"No model versions found for '{model_name}'")

    latest = sorted(versions, key=lambda v: int(v.version))[-1]
    model_uri = f"models:/{model_name}/{latest.version}"

    print(f"Loading model: {model_uri}")
    model = mlflow.xgboost.load_model(model_uri)
    return model, latest.version


def fetch_events_from_dynamodb(table_name: str, region: str, limit: int = 1000):
    """
    Fetch recent events from DynamoDB feature store for batch scoring.
    In production, this would query Snowflake or read from S3 Parquet.
    """
    dynamodb = boto3.resource("dynamodb", region_name=region)
    table = dynamodb.Table(table_name)

    response = table.scan(Limit=limit)
    items = response.get("Items", [])

    if not items:
        print("No items found in DynamoDB, generating synthetic batch")
        return generate_synthetic_batch(limit)

    # Convert to DataFrame with expected feature columns
    records = []
    for item in items:
        records.append({
            "account_id": item.get("account_id", ""),
            "tx_count_2min": float(item.get("tx_count_2min", 0)),
            "tx_sum_24h": float(item.get("tx_sum_24h", 0)),
            "amount": float(item.get("last_amount", 0)),
            "account_age_days": int(item.get("account_age_days", 0)),
            "claim_count_30d": int(item.get("claim_count_30d", 0)),
            "device_changed": 1 if item.get("device_changed", False) else 0,
            "time_since_last_login_min": float(item.get("time_since_last_login_min", 0)),
            "policy_age_days": int(item.get("policy_age_days", 0)),
        })

    return pd.DataFrame(records)


def generate_synthetic_batch(n: int = 1000) -> pd.DataFrame:
    """Generate synthetic batch for scoring when no DynamoDB data is available."""
    np.random.seed(42)
    return pd.DataFrame({
        "account_id": [f"ACC-{i:05d}" for i in range(n)],
        "tx_count_2min": np.random.poisson(3, n),
        "tx_sum_24h": np.random.lognormal(7, 1.5, n).clip(0, 100000),
        "amount": np.random.lognormal(6, 2, n).clip(10, 100000),
        "account_age_days": np.random.randint(0, 3650, n),
        "claim_count_30d": np.random.poisson(0.5, n),
        "device_changed": np.random.binomial(1, 0.15, n),
        "time_since_last_login_min": np.random.exponential(60, n).clip(0, 10000),
        "policy_age_days": np.random.randint(0, 1825, n),
    })


def score(args):
    """Main scoring function."""
    print(f"O2 Batch Scoring — version={args.model_version}")

    # Load model
    model, version = load_model(args.mlflow_uri, "o2-anomaly-xgboost", args.experiment)
    print(f"Model version: {version}")

    # Fetch data
    dynamo_table = os.getenv("DYNAMODB_TABLE", "o2-platform-feature-store")
    dynamo_region = os.getenv("DYNAMODB_REGION", "us-east-1")
    df = fetch_events_from_dynamodb(dynamo_table, dynamo_region)
    print(f"Scoring {len(df)} accounts")

    # Feature columns (must match training)
    feature_cols = [
        "tx_count_2min", "tx_sum_24h", "amount",
        "account_age_days", "claim_count_30d", "device_changed",
        "time_since_last_login_min", "policy_age_days",
    ]

    X = df[feature_cols]

    # Score
    probabilities = model.predict_proba(X)[:, 1]
    predictions = (probabilities > 0.5).astype(int)

    df["anomaly_score"] = probabilities
    df["is_anomaly"] = predictions
    df["model_version"] = version
    df["scored_at"] = datetime.utcnow().isoformat()

    # Summary
    n_anomalies = predictions.sum()
    avg_score = probabilities.mean()
    print(f"\nResults:")
    print(f"  Total accounts scored: {len(df)}")
    print(f"  Anomalies detected:    {n_anomalies} ({n_anomalies / len(df) * 100:.1f}%)")
    print(f"  Average anomaly score: {avg_score:.4f}")

    # Log to MLflow
    mlflow.set_tracking_uri(args.mlflow_uri)
    mlflow.set_experiment(args.experiment)

    with mlflow.start_run(run_name=f"batch-scoring-{args.model_version}"):
        mlflow.log_param("model_version", version)
        mlflow.log_param("n_scored", len(df))
        mlflow.log_metric("n_anomalies", n_anomalies)
        mlflow.log_metric("anomaly_rate", n_anomalies / len(df))
        mlflow.log_metric("avg_anomaly_score", avg_score)

        # Log top anomalies
        top_anomalies = df.nlargest(20, "anomaly_score")[
            ["account_id", "anomaly_score", "tx_count_2min", "amount"]
        ]
        mlflow.log_text(
            top_anomalies.to_string(index=False),
            "top_anomalies.txt"
        )

    print(f"\nTop 10 anomalous accounts:")
    print(df.nlargest(10, "anomaly_score")[
        ["account_id", "anomaly_score", "tx_count_2min", "tx_sum_24h", "amount"]
    ].to_string(index=False))

    return df


def main():
    parser = argparse.ArgumentParser(description="O2 Batch Scoring")
    parser.add_argument("--model-version", required=True, help="Model version tag")
    parser.add_argument("--mlflow-uri", default="http://localhost:5000", help="MLflow tracking URI")
    parser.add_argument("--experiment", default="/O2-Anomaly-Detection", help="MLflow experiment name")
    args = parser.parse_args()

    score(args)


if __name__ == "__main__":
    main()
