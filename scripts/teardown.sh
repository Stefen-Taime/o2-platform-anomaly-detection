#!/bin/bash
# =============================================================================
# O2 Platform — Teardown Script
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
TERRAFORM_DIR="$PROJECT_DIR/terraform"
K8S_DIR="$PROJECT_DIR/k8s"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log() { echo -e "${GREEN}[O2]${NC} $1"; }
warn() { echo -e "${YELLOW}[O2]${NC} $1"; }

echo ""
echo -e "${RED}╔══════════════════════════════════════════╗${NC}"
echo -e "${RED}║  WARNING: This will destroy ALL          ║${NC}"
echo -e "${RED}║  O2 Platform AWS resources!              ║${NC}"
echo -e "${RED}╚══════════════════════════════════════════╝${NC}"
echo ""
read -p "Are you sure? (type 'yes' to confirm): " confirm

if [ "$confirm" != "yes" ]; then
    echo "Aborted."
    exit 0
fi

# =============================================================================
# Step 1: Delete Kubernetes resources
# =============================================================================
log "Deleting Kubernetes resources..."
kubectl delete -f "$K8S_DIR/kafka-streams-app.yaml" --ignore-not-found -n kafka || true
kubectl delete -f "$K8S_DIR/kafka-topics.yaml" --ignore-not-found -n kafka || true
kubectl delete -f "$K8S_DIR/kafka-cluster.yaml" --ignore-not-found -n kafka || true

log "Waiting for Kafka PVCs to be released..."
sleep 15
kubectl delete pvc --all -n kafka 2>/dev/null || true

# =============================================================================
# Step 2: Remove Strimzi operator
# =============================================================================
log "Removing Strimzi operator..."
kubectl delete -f 'https://strimzi.io/install/latest?namespace=kafka' -n kafka 2>/dev/null || true
kubectl delete namespace kafka --ignore-not-found || true

# =============================================================================
# Step 3: Terraform destroy
# =============================================================================
log "Destroying AWS infrastructure..."
cd "$TERRAFORM_DIR"
terraform destroy -auto-approve

log ""
log "============================================"
log "  O2 Platform Teardown Complete"
log "============================================"
log "  All AWS resources have been destroyed."
log "  Estimated cost for this session: ~\$0.40"
log ""
