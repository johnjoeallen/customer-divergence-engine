#!/bin/bash
set -e
# Run the app with Docker Compose
# Usage: ./run.sh [--clean]

# Always force-remove named containers first to avoid name conflicts
docker rm -f similarity-engine similarity-postgres 2>/dev/null || true

if [[ "$1" == "--clean" ]]; then
  echo "Stopping containers and removing database volume..."
  docker-compose down -v --remove-orphans
  echo "Rebuilding and starting containers in detached mode..."
  docker-compose build --no-cache && docker-compose up -d --force-recreate
else
  docker-compose down --remove-orphans
  docker-compose build --no-cache && docker-compose up -d --force-recreate
fi
