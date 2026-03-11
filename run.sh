#!/bin/bash
set -e
# Run the app with Docker Compose
# Usage: ./run.sh [--clean]

# Force-remove named containers to avoid name conflicts
docker rm -f similarity-engine similarity-postgres 2>/dev/null || true

if [[ "$1" == "--clean" ]]; then
  echo "Removing database volume..."
  docker-compose down -v --remove-orphans
else
  docker-compose down --remove-orphans
fi

docker-compose up -d --force-recreate

echo ""
echo "✔ App is running at http://localhost:9096"
echo "  → Hard-refresh (Ctrl+Shift+R) to bypass any browser cache."
