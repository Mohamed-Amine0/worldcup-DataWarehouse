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

logger = setup_logging("bronze_ingestion")

CSV_FILES = {
    "fifa_rankings_2022": "fifa_ranking_2022-10-06.csv",
    "fifa_rankings_2026": "fifa_ranking_2026-06-08.csv",
    "world_cup_history": "world_cup.csv",
    "schedule_2026": "schedule_2026.csv",
    "matches_1930_2022": "matches_1930_2022.csv",
}


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
    loaded = 0

    for table_name, filename in CSV_FILES.items():
        file_path = os.path.join(DATA_DIR, filename)
        file_start = datetime.now()

        if not os.path.exists(file_path):
            logger.warning(
                f"Fichier {file_path} introuvable",
                extra={"context": {"filename": filename}},
            )
            continue

        df = pd.read_csv(file_path)
        row_count = len(df)
        file_size = os.path.getsize(file_path)

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
        ingestion_time = (datetime.now() - file_start).total_seconds()
        rows_per_sec = row_count / ingestion_time if ingestion_time > 0 else row_count
        loaded += 1

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

    total_time = (datetime.now() - pipeline_start).total_seconds()
    logger.info(
        "Ingestion Bronze terminée",
        extra={
            "context": {
                "total_tables": loaded,
                "total_processing_time_seconds": total_time,
            }
        },
    )


if __name__ == "__main__":
    main()
