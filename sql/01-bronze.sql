-- Schéma Bronze pour les données brutes
CREATE SCHEMA IF NOT EXISTS bronze;

CREATE TABLE IF NOT EXISTS bronze.metadata (
    table_name VARCHAR(100) PRIMARY KEY,
    source_file VARCHAR(255),
    ingestion_timestamp TIMESTAMP DEFAULT NOW(),
    row_count INT,
    schema_version VARCHAR(50) DEFAULT '1.0'
);
