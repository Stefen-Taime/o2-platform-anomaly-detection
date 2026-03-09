"""
O2 Platform — XGBoost Anomaly Detection Model Training

This script is designed to run on Databricks Community Edition.
It trains an XGBoost binary classifier to detect anomalous events
based on features from the DynamoDB feature store.

Features per PRD §5.1:
  - velocity_2min: count transactions par account_id (fenetre 2 min)
  - amount_zscore: (amount - avg_30d) / std_30d
  - device_risk_score: 0 si device connu, 1 si inconnu
  - location_anomaly: 1 si pays hors liste habituelle
  - claim_delay_days: date_sinistre - date_souscription
  - night_transaction: 1 si heure entre 00h et 05h

Target: is_anomaly (0/1)
"""

import os
import json
import numpy as np
import pandas as pd
from datetime import datetime

import mlflow
import mlflow.xgboost
from xgboost import XGBClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import (
    roc_auc_score,
    precision_score,
    recall_score,
    f1_score,
    classification_report,
    confusion_matrix,
)


# ============================================================
# CONFIG
# ============================================================
MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")
EXPERIMENT_NAME = os.getenv("EXPERIMENT_NAME", "/O2-Anomaly-Detection")
MODEL_NAME = "o2-anomaly-xgboost"
RANDOM_STATE = 42
N_SAMPLES = 50000  # synthetic dataset size for POC


def generate_synthetic_dataset(n_samples: int) -> pd.DataFrame:
    """
    Generate a synthetic labeled dataset that mimics the enriched events
    from the Kafka Streams pipeline + DynamoDB feature store.

    Feature names aligned with PRD §5.1.

    In production, this would be replaced by a query to Snowflake
    (MART.FCT_ANOMALIES joined with RAW.TRANSACTIONS).
    """
    np.random.seed(RANDOM_STATE)

    # 85% normal, 15% anomalous
    n_anomalies = int(n_samples * 0.15)
    n_normal = n_samples - n_anomalies

    # Normal events
    normal = pd.DataFrame({
        "velocity_2min": np.random.poisson(2, n_normal),
        "amount_zscore": np.random.normal(0, 1, n_normal).clip(-2, 2),
        "device_risk_score": np.random.binomial(1, 0.05, n_normal).astype(float),
        "location_anomaly": np.random.binomial(1, 0.03, n_normal),
        "claim_delay_days": np.random.randint(30, 365, n_normal),
        "night_transaction": np.random.binomial(1, 0.08, n_normal),
        "is_anomaly": 0,
    })

    # Anomalous events (different distributions)
    anomalous = pd.DataFrame({
        "velocity_2min": np.random.poisson(8, n_anomalies).clip(0, 50),
        "amount_zscore": np.random.normal(4, 1.5, n_anomalies).clip(2, 10),
        "device_risk_score": np.random.binomial(1, 0.6, n_anomalies).astype(float),
        "location_anomaly": np.random.binomial(1, 0.5, n_anomalies),
        "claim_delay_days": np.random.randint(0, 14, n_anomalies),
        "night_transaction": np.random.binomial(1, 0.4, n_anomalies),
        "is_anomaly": 1,
    })

    df = pd.concat([normal, anomalous], ignore_index=True)
    return df.sample(frac=1, random_state=RANDOM_STATE).reset_index(drop=True)


def train():
    """Train XGBoost model and log to MLflow."""

    # Setup MLflow
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    mlflow.set_experiment(EXPERIMENT_NAME)

    print(f"MLflow tracking URI: {MLFLOW_TRACKING_URI}")
    print(f"Experiment: {EXPERIMENT_NAME}")

    # Generate dataset
    print(f"Generating synthetic dataset ({N_SAMPLES} samples)...")
    df = generate_synthetic_dataset(N_SAMPLES)
    print(f"  Normal: {(df.is_anomaly == 0).sum()}, Anomalous: {(df.is_anomaly == 1).sum()}")

    # Features / target — PRD §5.1
    feature_cols = [
        "velocity_2min",
        "amount_zscore",
        "device_risk_score",
        "location_anomaly",
        "claim_delay_days",
        "night_transaction",
    ]
    X = df[feature_cols]
    y = df["is_anomaly"]

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=RANDOM_STATE, stratify=y
    )

    print(f"  Train: {len(X_train)}, Test: {len(X_test)}")

    # Train XGBoost
    with mlflow.start_run(run_name=f"xgboost-{datetime.now().strftime('%Y%m%d-%H%M%S')}"):

        params = {
            "n_estimators": 200,
            "max_depth": 6,
            "learning_rate": 0.1,
            "subsample": 0.8,
            "colsample_bytree": 0.8,
            "scale_pos_weight": (y_train == 0).sum() / (y_train == 1).sum(),
            "eval_metric": "auc",
            "random_state": RANDOM_STATE,
            "use_label_encoder": False,
        }

        mlflow.log_params(params)
        mlflow.log_param("n_samples", N_SAMPLES)
        mlflow.log_param("n_features", len(feature_cols))
        mlflow.log_param("features", json.dumps(feature_cols))

        print("Training XGBoost model...")
        model = XGBClassifier(**params)
        model.fit(
            X_train, y_train,
            eval_set=[(X_test, y_test)],
            verbose=False,
        )

        # Predictions
        y_pred = model.predict(X_test)
        y_proba = model.predict_proba(X_test)[:, 1]

        # Metrics — PRD §5.3 targets: AUC >= 0.82, Precision >= 0.85, Recall >= 0.80
        auc = roc_auc_score(y_test, y_proba)
        precision = precision_score(y_test, y_pred)
        recall = recall_score(y_test, y_pred)
        f1 = f1_score(y_test, y_pred)
        fpr = ((y_pred == 1) & (y_test == 0)).sum() / (y_test == 0).sum()

        mlflow.log_metric("auc", auc)
        mlflow.log_metric("precision", precision)
        mlflow.log_metric("recall", recall)
        mlflow.log_metric("f1", f1)
        mlflow.log_metric("false_positive_rate", fpr)

        print(f"\n  AUC:       {auc:.4f}  (target >= 0.82)")
        print(f"  Precision: {precision:.4f}  (target >= 0.85)")
        print(f"  Recall:    {recall:.4f}  (target >= 0.80)")
        print(f"  F1:        {f1:.4f}")
        print(f"  FPR:       {fpr:.4f}")

        # Classification report
        report = classification_report(y_test, y_pred)
        print(f"\n{report}")
        mlflow.log_text(report, "classification_report.txt")

        # Confusion matrix
        cm = confusion_matrix(y_test, y_pred)
        mlflow.log_text(str(cm), "confusion_matrix.txt")

        # Feature importance — PRD §5.4
        importance = dict(zip(feature_cols, model.feature_importances_.tolist()))
        mlflow.log_dict(importance, "feature_importance.json")
        print(f"\nFeature importance:")
        for feat, imp in sorted(importance.items(), key=lambda x: -x[1]):
            print(f"  {feat}: {imp:.4f}")

        # Log model — PRD §5.4: serialized model artifact
        mlflow.xgboost.log_model(
            model,
            artifact_path="model",
            registered_model_name=MODEL_NAME,
            input_example=X_test.iloc[:5],
        )

        print(f"\nModel logged to MLflow as '{MODEL_NAME}'")
        print(f"Run ID: {mlflow.active_run().info.run_id}")


if __name__ == "__main__":
    train()
