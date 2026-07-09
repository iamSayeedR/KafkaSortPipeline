#!/bin/bash
set -e

RECORD_COUNT=${1:-50000000}

echo "Running KafkaSortPipeline with RECORD_COUNT=$RECORD_COUNT (memory=2g, cpus=4)"
docker run --rm \
    --memory=2g \
    --cpus=4 \
    -e RECORD_COUNT="$RECORD_COUNT" \
    kafkasortpipeline:latest
