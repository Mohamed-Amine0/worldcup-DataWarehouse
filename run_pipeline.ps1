$ErrorActionPreference = "Stop"

function Invoke-Step([string]$Label, [scriptblock]$Action) {
    Write-Host $Label
    & $Action
    if ($LASTEXITCODE -ne 0) {
        throw "Echec: $Label (code $LASTEXITCODE)"
    }
}

$SparkJars = "/opt/spark/jars/postgresql-42.7.3.jar"

Write-Host "[DEBUT] Lancement du pipeline ETL World Cup Data Warehouse"

Invoke-Step "[ETAPE 1/3] Ingestion vers Bronze..." {
    docker exec ingestion python /scripts/bronze/csv_to_postgres.py
    docker exec ingestion python /scripts/bronze/elo_to_bronze.py
}

Invoke-Step "[ETAPE 2/3] Transformation Bronze -> Silver..." {
    docker exec spark-master /opt/spark/bin/spark-submit `
        --master spark://spark-master:7077 `
        --jars $SparkJars `
        --conf spark.executorEnv.PYTHONPATH=/scripts `
        --conf spark.driverEnv.PYTHONPATH=/scripts `
        /scripts/silver/bronze_to_silver.py
}

Invoke-Step "[ETAPE 3/3] Transformation Silver -> Gold..." {
    docker exec spark-master /opt/spark/bin/spark-submit `
        --master spark://spark-master:7077 `
        --jars $SparkJars `
        --conf spark.executorEnv.PYTHONPATH=/scripts `
        --conf spark.driverEnv.PYTHONPATH=/scripts `
        /scripts/gold/silver_to_gold.py
}

Write-Host "[FIN] Pipeline ETL termine avec succes !"
