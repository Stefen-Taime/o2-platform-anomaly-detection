"""Setup file for building the model wheel (used by Jenkins pipeline)."""

from setuptools import setup, find_packages

setup(
    name="o2_anomaly_model",
    version="1.0.0",
    description="O2 Platform Anomaly Detection Model",
    packages=find_packages(),
    python_requires=">=3.9",
    install_requires=[
        "xgboost>=2.0.0",
        "mlflow>=2.14.0",
        "pandas>=2.0.0",
        "numpy>=1.24.0",
        "scikit-learn>=1.3.0",
        "boto3>=1.34.0",
    ],
    entry_points={
        "console_scripts": [
            "score=scoring.score:main",
        ],
    },
)
