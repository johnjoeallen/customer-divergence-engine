#!/bin/bash
# Run the app with Docker Compose
# Usage: ./run.sh [--clean]

if [[ "$1" == "--clean" ]]; then
  echo "Stopping containers and removing database volume..."
  docker-compose down -v
  echo "Rebuilding and starting containers in detached mode..."
  docker-compose up --build -d
else
  docker-compose up --build -d
fi
