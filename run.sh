#!/bin/bash
# Run the app with Docker Compose
# Usage: ./run.sh [--clean]

if [[ "$1" == "--clean" ]]; then
  echo "Stopping containers and removing database volume..."
  docker-compose down -v
  echo "Rebuilding and starting containers in detached mode..."
  docker-compose build --no-cache && docker-compose up -d
else
  docker-compose build --no-cache && docker-compose up -d
fi
