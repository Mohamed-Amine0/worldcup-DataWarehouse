$ErrorActionPreference = "Stop"
Write-Host "Execution des tests dans le conteneur ingestion..."
docker compose -f docker-compose.1worker.yml up -d ingestion | Out-Null
docker exec ingestion pytest /tests -v --tb=short
