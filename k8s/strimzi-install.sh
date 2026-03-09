#!/bin/bash
# =============================================================================
# Strimzi Operator Installation
# =============================================================================
set -euo pipefail

echo "=== Installing Strimzi Kafka Operator ==="

# Create namespace if not exists
kubectl create namespace kafka --dry-run=client -o yaml | kubectl apply -f -

# Install Strimzi 0.40.0 operator
kubectl create -f 'https://strimzi.io/install/latest?namespace=kafka' -n kafka || true

echo "Waiting for Strimzi operator to be ready..."
kubectl wait --for=condition=ready pod -l name=strimzi-cluster-operator -n kafka --timeout=300s

echo "=== Strimzi Operator Ready ==="
