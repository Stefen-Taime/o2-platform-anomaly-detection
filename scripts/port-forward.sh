#!/bin/bash
# =============================================================================
# O2 Platform — Port Forward Kafka for Local Go Generator
# =============================================================================
set -euo pipefail

echo "Setting up port-forward to Kafka bootstrap..."
echo "This will allow the local Go generator to connect via localhost:9094"
echo ""
echo "Press Ctrl+C to stop."
echo ""

kubectl port-forward svc/o2-kafka-kafka-external-bootstrap 9094:9094 -n kafka
