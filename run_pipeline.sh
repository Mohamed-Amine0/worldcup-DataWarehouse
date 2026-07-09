#!/bin/bash
set -euo pipefail

COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.1worker.yml}"
SPARK_JARS="/opt/spark/jars/postgresql-42.7.3.jar"

echo "[DEBUT] Lancement du pipeline ETL World Cup Data Warehouse"

echo "[ETAPE 1/3] Ingestion vers Bronze..."
docker exec ingestion python /scripts/bronze/csv_to_postgres.py
docker exec ingestion python /scripts/bronze/elo_to_bronze.py

echo "[ETAPE 2/3] Transformation Bronze -> Silver..."
docker exec spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --jars "${SPARK_JARS}" \
    --conf spark.executorEnv.PYTHONPATH=/scripts \
    --conf spark.driverEnv.PYTHONPATH=/scripts \
    /scripts/silver/bronze_to_silver.py

echo "[ETAPE 3/3] Transformation Silver -> Gold..."
docker exec spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --jars "${SPARK_JARS}" \
    --conf spark.executorEnv.PYTHONPATH=/scripts \
    --conf spark.driverEnv.PYTHONPATH=/scripts \
    /scripts/gold/silver_to_gold.py

echo "[FIN] Pipeline ETL termine avec succes !"
