#!/bin/bash
set -e

# 1. Build the JAR
mvn clean package -DskipTests

# 2. Rebuild Docker image from scratch (no layer cache)
docker-compose build --no-cache

# 3. Tear down running containers and recreate them so the new image is used
docker-compose down
docker-compose up -d --force-recreate

echo ""
echo "✔ Build complete. App is running."
echo "  → Open http://localhost:9096 in your browser."
echo "  → Hard-refresh (Ctrl+Shift+R / Cmd+Shift+R) to bypass any browser cache."
