#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"

echo "Building KafkaSortPipeline Docker image..."
docker build -t kafkasortpipeline:latest -f "$PROJECT_DIR/docker/Dockerfile" "$PROJECT_DIR"
echo "Build complete: kafkasortpipeline:latest"
