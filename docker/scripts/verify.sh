#!/bin/bash
set -e

echo "Running verification..."
java -Xmx128m -Xms64m -XX:+UseG1GC \
    -cp /app/kafka-sort-pipeline.jar \
    com.pipeline.verify.VerifyApp
