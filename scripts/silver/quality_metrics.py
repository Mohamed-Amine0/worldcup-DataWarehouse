from datetime import datetime

from pyspark.sql import SparkSession


def store_quality_metrics(
    spark: SparkSession,
    jdbc_url: str,
    db_properties: dict,
    table_name: str,
    total_records: int,
    null_count: int,
    duplicate_count: int,
    invalid_records: int,
    processing_time: float,
):
    """Enregistre ou met à jour les métriques de qualité Silver."""
    metrics_df = spark.createDataFrame(
        [
            {
                "table_name": table_name,
                "ingestion_timestamp": datetime.now(),
                "total_records": total_records,
                "null_count": null_count,
                "duplicate_count": duplicate_count,
                "invalid_records": invalid_records,
                "processing_time_seconds": float(processing_time),
            }
        ]
    )

    metrics_df.write.jdbc(
        url=jdbc_url,
        table="silver.quality_metrics_staging",
        mode="overwrite",
        properties=db_properties,
    )

    # Upsert via JDBC temp table + SQL côté driver
    from pyspark.sql import Row

    jvm = spark._jvm
    jvm.java.lang.Class.forName(db_properties["driver"])
    conn = jvm.java.sql.DriverManager.getConnection(
        jdbc_url, db_properties["user"], db_properties["password"]
    )
    stmt = conn.createStatement()
    stmt.execute(
        """
        INSERT INTO silver.quality_metrics
            (table_name, ingestion_timestamp, total_records, null_count,
             duplicate_count, invalid_records, processing_time_seconds)
        SELECT table_name, ingestion_timestamp, total_records, null_count,
               duplicate_count, invalid_records, processing_time_seconds
        FROM silver.quality_metrics_staging
        ON CONFLICT (table_name) DO UPDATE SET
            ingestion_timestamp = EXCLUDED.ingestion_timestamp,
            total_records = EXCLUDED.total_records,
            null_count = EXCLUDED.null_count,
            duplicate_count = EXCLUDED.duplicate_count,
            invalid_records = EXCLUDED.invalid_records,
            processing_time_seconds = EXCLUDED.processing_time_seconds
        """
    )
    stmt.execute("DROP TABLE IF EXISTS silver.quality_metrics_staging")
    stmt.close()
    conn.close()
