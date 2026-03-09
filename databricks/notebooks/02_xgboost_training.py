# Databricks notebook source
# MAGIC %md
# MAGIC # O2 Platform — XGBoost Model Training
# MAGIC PRD §5 — Train anomaly detection model on Databricks Community Edition
# MAGIC
# MAGIC This notebook trains an XGBoost classifier using real features from S3
# MAGIC (computed by 01_feature_engineering), logs results to MLflow,
# MAGIC and saves the model artifact for Blue-Green deployment via Jenkins.
# MAGIC
# MAGIC **Note**: Uses boto3 + pandas (no Spark S3 access on serverless CE).

# COMMAND ----------

# MAGIC %pip install xgboost boto3

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

import json
import os
import io
import mlflow
import mlflow.tracking._model_registry.utils
from datetime import datetime

# Databricks CE bug: "spark.mlflow.modelRegistryUri is not available"
mlflow.tracking._model_registry.utils._get_registry_uri_from_spark_session = lambda: "databricks-uc"

EXPERIMENT_NAME = "/O2-Anomaly-Detection"
MODEL_NAME = "o2-anomaly-xgboost"

mlflow.set_experiment(EXPERIMENT_NAME)

print(f"MLflow: Databricks CE (with registry URI workaround)")
print(f"Experiment: {EXPERIMENT_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Load training features from S3
# MAGIC Features computed by 01_feature_engineering.py and saved as Parquet.
# MAGIC Fallback to synthetic data if S3 features are not available.

# COMMAND ----------

import boto3
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split

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

try:
    resp = s3.get_object(Bucket=S3_AUDIT_BUCKET, Key="features/training/features.parquet")
    pdf = pd.read_parquet(io.BytesIO(resp["Body"].read()))
    data_source = "S3 (real data)"
    print(f"Loaded {len(pdf)} rows from S3 features")
except Exception as e:
    print(f"S3 features not available ({e})")
    print("Falling back to synthetic dataset...")
    data_source = "synthetic"
    np.random.seed(42)
    N = 50000
    n_anom = int(N * 0.15)
    n_norm = N - n_anom
    normal = pd.DataFrame({
        "velocity_2min": np.random.poisson(2, n_norm),
        "amount_zscore": np.random.normal(0, 1, n_norm).clip(-2, 2),
        "device_risk_score": np.random.binomial(1, 0.05, n_norm).astype(float),
        "location_anomaly": np.random.binomial(1, 0.03, n_norm),
        "claim_delay_days": np.random.randint(30, 365, n_norm),
        "night_transaction": np.random.binomial(1, 0.08, n_norm),
        "is_anomaly": 0,
    })
    anomalous = pd.DataFrame({
        "velocity_2min": np.random.poisson(8, n_anom).clip(0, 50),
        "amount_zscore": np.random.normal(4, 1.5, n_anom).clip(2, 10),
        "device_risk_score": np.random.binomial(1, 0.6, n_anom).astype(float),
        "location_anomaly": np.random.binomial(1, 0.5, n_anom),
        "claim_delay_days": np.random.randint(0, 14, n_anom),
        "night_transaction": np.random.binomial(1, 0.4, n_anom),
        "is_anomaly": 1,
    })
    pdf = pd.concat([normal, anomalous], ignore_index=True).sample(frac=1, random_state=42)

print(f"Data source: {data_source}")
print(f"Dataset: {len(pdf)} rows")
print(f"Columns: {list(pdf.columns)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1b. Label generation for real data
# MAGIC Real data from S3 has no `is_anomaly` column.
# MAGIC We use rule-based labeling aligned with the Kafka Streams AnomalyScorer logic:
# MAGIC - velocity_2min >= 5 (high transaction velocity)
# MAGIC - amount_zscore >= 2.5 (unusual amount)
# MAGIC - device_risk_score == 1 (unknown device)
# MAGIC - location_anomaly == 1 (unusual country)
# MAGIC - night_transaction == 1 (between 00h-05h)
# MAGIC - claim_delay_days <= 7 (suspicious rapid claim)
# MAGIC
# MAGIC An event is labeled anomalous if 2+ signals fire (same logic as AnomalyScorer.java)

# COMMAND ----------

feature_cols = [
    "velocity_2min",
    "amount_zscore",
    "device_risk_score",
    "location_anomaly",
    "claim_delay_days",
    "night_transaction",
]

if "is_anomaly" not in pdf.columns:
    # Rule-based labeling (mirrors AnomalyScorer.java thresholds)
    signals = pd.DataFrame()
    signals["high_velocity"] = (pdf["velocity_2min"] >= 5).astype(int)
    signals["high_zscore"] = (pdf["amount_zscore"] >= 2.5).astype(int)
    signals["unknown_device"] = (pdf["device_risk_score"] == 1.0).astype(int)
    signals["foreign_location"] = (pdf["location_anomaly"] == 1).astype(int)
    signals["night_tx"] = (pdf["night_transaction"] == 1).astype(int)
    signals["rapid_claim"] = (pdf["claim_delay_days"] <= 7).astype(int)

    signal_count = signals.sum(axis=1)
    pdf["is_anomaly"] = (signal_count >= 2).astype(int)

    print(f"Labels generated using rule-based approach (threshold: 2+ signals)")
    print(f"Signal distribution:")
    for col in signals.columns:
        print(f"  {col}: {signals[col].sum()} ({signals[col].mean():.2%})")
    print(f"\nTotal anomalies: {pdf['is_anomaly'].sum()} ({pdf['is_anomaly'].mean():.2%})")
else:
    print(f"Labels already present in dataset")
    print(f"Anomaly rate: {pdf['is_anomaly'].mean():.2%}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Train XGBoost — PRD §5

# COMMAND ----------

from xgboost import XGBClassifier
from sklearn.metrics import roc_auc_score, precision_score, recall_score, f1_score, classification_report

X = pdf[feature_cols]
y = pdf["is_anomaly"]

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42, stratify=y
)

print(f"Train: {len(X_train)}, Test: {len(X_test)}")
print(f"Train anomaly rate: {y_train.mean():.2%}")

with mlflow.start_run(run_name=f"xgboost-real-{datetime.now().strftime('%Y%m%d-%H%M%S')}"):

    params = {
        "n_estimators": 200,
        "max_depth": 6,
        "learning_rate": 0.1,
        "subsample": 0.8,
        "colsample_bytree": 0.8,
        "scale_pos_weight": (y_train == 0).sum() / max((y_train == 1).sum(), 1),
        "eval_metric": "auc",
        "random_state": 42,
        "use_label_encoder": False,
    }

    mlflow.log_params(params)
    mlflow.log_param("n_samples", len(pdf))
    mlflow.log_param("data_source", data_source)
    mlflow.log_param("features", json.dumps(feature_cols))

    model = XGBClassifier(**params)
    model.fit(X_train, y_train, eval_set=[(X_test, y_test)], verbose=False)

    y_pred = model.predict(X_test)
    y_proba = model.predict_proba(X_test)[:, 1]

    auc = roc_auc_score(y_test, y_proba)
    precision = precision_score(y_test, y_pred, zero_division=0)
    recall = recall_score(y_test, y_pred, zero_division=0)
    f1 = f1_score(y_test, y_pred, zero_division=0)
    fpr = ((y_pred == 1) & (y_test == 0)).sum() / max((y_test == 0).sum(), 1)

    mlflow.log_metric("auc", auc)
    mlflow.log_metric("precision", precision)
    mlflow.log_metric("recall", recall)
    mlflow.log_metric("f1", f1)
    mlflow.log_metric("false_positive_rate", fpr)

    print(f"AUC:       {auc:.4f}  (target >= 0.82)")
    print(f"Precision: {precision:.4f}  (target >= 0.85)")
    print(f"Recall:    {recall:.4f}  (target >= 0.80)")
    print(f"F1:        {f1:.4f}")
    print(f"FPR:       {fpr:.4f}")

    report = classification_report(y_test, y_pred, zero_division=0)
    print(f"\n{report}")
    mlflow.log_text(report, "classification_report.txt")

    importance = dict(zip(feature_cols, model.feature_importances_.tolist()))
    mlflow.log_dict(importance, "feature_importance.json")

    # Save model as artifact (Model Registry not available on Databricks CE)
    import tempfile
    with tempfile.TemporaryDirectory() as tmpdir:
        model_path = os.path.join(tmpdir, "xgboost_model.json")
        model.save_model(model_path)
        mlflow.log_artifact(model_path, artifact_path="model")

        example_path = os.path.join(tmpdir, "input_example.csv")
        X_test.iloc[:5].to_csv(example_path, index=False)
        mlflow.log_artifact(example_path, artifact_path="model")

    run_id = mlflow.active_run().info.run_id
    print(f"\nModel saved as artifact in run: {run_id}")
    print(f"Artifact path: model/xgboost_model.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Feature Importance

# COMMAND ----------

for feat, imp in sorted(importance.items(), key=lambda x: -x[1]):
    bar = "#" * int(imp * 100)
    print(f"  {feat:25s} {imp:.4f} {bar}")
