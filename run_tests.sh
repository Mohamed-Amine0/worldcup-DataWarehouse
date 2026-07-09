#!/bin/bash
set -euo pipefail

echo "Execution des tests dans le conteneur ingestion..."
docker exec ingestion pytest /tests -v --tb=short
