import os
import sys
from datetime import datetime

import pandas as pd
from sqlalchemy import create_engine, text

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.logging_config import setup_logging

DB_USER = os.getenv("DB_USER", "user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "password")
DB_HOST = os.getenv("DB_HOST", "postgres")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "world_cup_dw")
DATA_DIR = os.getenv("DATA_DIR", "/data/source")

logger = setup_logging("elo_bronze_ingestion")


def upsert_metadata(engine, table_name: str, source_file: str, row_count: int):
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO bronze.metadata (table_name, source_file, row_count)
                VALUES (:table_name, :source_file, :row_count)
                ON CONFLICT (table_name) DO UPDATE SET
                    source_file = EXCLUDED.source_file,
                    ingestion_timestamp = NOW(),
                    row_count = EXCLUDED.row_count
                """
            ),
            {"table_name": table_name, "source_file": source_file, "row_count": row_count},
        )


def main():
    engine = create_engine(
        f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )
    pipeline_start = datetime.now()
    filename = "elo_ratings_wc2026.csv"
    table_name = "elo_ratings"
    file_path = os.path.join(DATA_DIR, filename)

    if not os.path.exists(file_path):
        logger.error(
            f"Fichier {file_path} introuvable",
            extra={"context": {"filename": filename}},
        )
        sys.exit(1)

    df = pd.read_csv(file_path)
    row_count = len(df)
    file_size = os.path.getsize(file_path)

    # Try to truncate first to avoid dropping (which fails if views depend on it)
    truncated = False
    try:
        with engine.begin() as conn:
            conn.execute(text("TRUNCATE TABLE bronze.elo_ratings"))
        truncated = True
        logger.info("Table bronze.elo_ratings tronquée avec succès")
    except Exception as e:
        logger.info("Impossible de tronquer la table (elle n'existe probablement pas encore)")

    if truncated:
        df.to_sql(
            table_name,
            engine,
            schema="bronze",
            if_exists="append",
            index=False,
            method="multi",
            chunksize=1000,
        )
    else:
        df.to_sql(
            table_name,
            engine,
            schema="bronze",
            if_exists="replace",
            index=False,
            method="multi",
            chunksize=1000,
        )

    upsert_metadata(engine, table_name, filename, row_count)
    ingestion_time = (datetime.now() - pipeline_start).total_seconds()
    rows_per_sec = row_count / ingestion_time if ingestion_time > 0 else row_count

    logger.info(
        f"Fichier {filename} chargé dans bronze.{table_name}",
        extra={
            "context": {
                "table": f"bronze.{table_name}",
                "source_file": filename,
                "row_count": row_count,
                "file_size_bytes": file_size,
                "ingestion_time_seconds": ingestion_time,
                "rows_per_second": round(rows_per_sec, 2),
                "timestamp": datetime.now().isoformat(),
            }
        },
    )


if __name__ == "__main__":
    main()
