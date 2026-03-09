#!/bin/bash
# =============================================================================
# O2 Platform — Full Deployment Script
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
TERRAFORM_DIR="$PROJECT_DIR/terraform"
K8S_DIR="$PROJECT_DIR/k8s"
APP_DIR="$PROJECT_DIR/kafka-streams-app"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log() { echo -e "${GREEN}[O2]${NC} $1"; }
warn() { echo -e "${YELLOW}[O2]${NC} $1"; }
err() { echo -e "${RED}[O2]${NC} $1"; exit 1; }

# =============================================================================
# Step 0: Pre-checks
# =============================================================================
log "Pre-flight checks..."

command -v terraform >/dev/null || err "terraform not found"
command -v aws >/dev/null || err "aws CLI not found"
command -v kubectl >/dev/null || err "kubectl not found"
command -v docker >/dev/null || err "docker not found"

aws sts get-caller-identity >/dev/null 2>&1 || err "AWS credentials not configured"
log "AWS identity: $(aws sts get-caller-identity --query 'Arn' --output text)"

# =============================================================================
# Step 1: Generate SSH key if not exists
# =============================================================================
if [ ! -f "$TERRAFORM_DIR/o2-key" ]; then
    log "Generating SSH key pair..."
    ssh-keygen -t ed25519 -f "$TERRAFORM_DIR/o2-key" -N "" -q
    log "SSH key generated: $TERRAFORM_DIR/o2-key"
else
    log "SSH key already exists"
fi

# =============================================================================
# Step 2: Terraform — provision AWS infrastructure
# =============================================================================
log "Provisioning AWS infrastructure with Terraform..."

cd "$TERRAFORM_DIR"
terraform init -upgrade
terraform plan -out=tfplan
terraform apply tfplan

# Capture outputs
EKS_CLUSTER=$(terraform output -raw eks_cluster_name)
ECR_URL=$(terraform output -raw ecr_repository_url)
MLFLOW_IP=$(terraform output -raw mlflow_public_ip)
JENKINS_IP=$(terraform output -raw jenkins_public_ip)
AUDIT_BUCKET=$(terraform output -raw s3_audit_trail_bucket)
SNS_ARN=$(terraform output -raw sns_anomaly_topic_arn)

log "Infrastructure provisioned!"
log "  EKS Cluster:  $EKS_CLUSTER"
log "  ECR URL:      $ECR_URL"
log "  MLflow:       http://$MLFLOW_IP:5000"
log "  Jenkins:      http://$JENKINS_IP:8080"

# =============================================================================
# Step 3: Configure kubectl
# =============================================================================
log "Configuring kubectl..."
aws eks update-kubeconfig --name "$EKS_CLUSTER" --region us-east-1
kubectl cluster-info

# =============================================================================
# Step 4: Install Strimzi Operator
# =============================================================================
log "Installing Strimzi Kafka operator..."
kubectl create namespace kafka --dry-run=client -o yaml | kubectl apply -f -
kubectl create -f 'https://strimzi.io/install/latest?namespace=kafka' -n kafka 2>/dev/null || true

log "Waiting for Strimzi operator..."
kubectl wait --for=condition=ready pod -l name=strimzi-cluster-operator -n kafka --timeout=300s

# =============================================================================
# Step 5: Deploy Kafka Cluster + Topics
# =============================================================================
log "Deploying Kafka cluster..."
kubectl apply -f "$K8S_DIR/kafka-cluster.yaml"

log "Waiting for Kafka to be ready (this takes ~3-5 minutes)..."
kubectl wait kafka/o2-kafka --for=condition=Ready --timeout=600s -n kafka

log "Creating Kafka topics..."
kubectl apply -f "$K8S_DIR/kafka-topics.yaml"
sleep 10

# =============================================================================
# Step 6: Build and push Kafka Streams app
# =============================================================================
log "Building Kafka Streams Docker image..."

# Login to ECR
aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin "$ECR_URL"

cd "$APP_DIR"
docker build --platform linux/amd64 -t o2-anomaly-detector:latest .
docker tag o2-anomaly-detector:latest "$ECR_URL:latest"
docker push "$ECR_URL:latest"

log "Image pushed to ECR: $ECR_URL:latest"

# =============================================================================
# Step 7: Deploy Kafka Streams app
# =============================================================================
log "Deploying Kafka Streams anomaly detector..."

# Replace placeholders with actual values
sed -e "s|REPLACE_WITH_ECR_URL|$ECR_URL|g" \
    -e "s|REPLACE_WITH_SNS_ARN|$SNS_ARN|g" \
    "$K8S_DIR/kafka-streams-app.yaml" | kubectl apply -f -

log "Waiting for anomaly detector to be ready..."
kubectl wait --for=condition=available deployment/o2-anomaly-detector -n kafka --timeout=120s

# =============================================================================
# Step 8: Deploy Kafka Connect S3 Sink (optional)
# =============================================================================
warn "Kafka Connect S3 Sink requires ECR build support in Strimzi."
warn "For the POC demo, S3 sink can be configured manually."
warn "Skipping Kafka Connect deployment — configure manually if needed."

# Replace bucket name in connector config
# sed "s|REPLACE_WITH_AUDIT_BUCKET|$AUDIT_BUCKET|g" "$K8S_DIR/kafka-connect-s3.yaml" | kubectl apply -f -

# =============================================================================
# Done!
# =============================================================================
echo ""
log "============================================"
log "  O2 Platform Deployment Complete!"
log "============================================"
log ""
log "  EKS Cluster : $EKS_CLUSTER"
log "  MLflow UI   : http://$MLFLOW_IP:5000"
log "  Jenkins UI  : http://$JENKINS_IP:8080"
log ""
log "  To run the Go generator locally:"
log "    cd $PROJECT_DIR/generator"
log "    go build -o o2-generator ."
log "    kubectl port-forward svc/o2-kafka-kafka-external-bootstrap 9094:9094 -n kafka &"
log "    ./o2-generator --bootstrap localhost:9094 --rate 100"
log ""
log "  To view anomalies:"
log "    kubectl exec -it o2-kafka-kafka-0 -n kafka -- bin/kafka-console-consumer.sh \\"
log "      --bootstrap-server localhost:9092 --topic anomalies --from-beginning"
log ""
