#!/bin/bash
set -e

# 1. Build the JAR
mvn clean package -DskipTests

# 2. Rebuild Docker image from scratch (no layer cache)
docker-compose build --no-cache

echo ""
echo "✔ Build complete. Run ./run.sh to start the app."
