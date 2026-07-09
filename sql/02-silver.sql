-- Schéma Silver pour les données nettoyées
CREATE SCHEMA IF NOT EXISTS silver;

CREATE TABLE IF NOT EXISTS silver.quality_metrics (
    table_name VARCHAR(100) PRIMARY KEY,
    ingestion_timestamp TIMESTAMP DEFAULT NOW(),
    total_records INT,
    null_count INT,
    duplicate_count INT,
    invalid_records INT,
    processing_time_seconds FLOAT
);

CREATE TABLE IF NOT EXISTS silver.fifa_rankings (
    rank INT,
    country VARCHAR(100),
    points INT,
    year INT,
    PRIMARY KEY (country, year)
);

CREATE TABLE IF NOT EXISTS silver.world_cup_history (
    year INT PRIMARY KEY,
    host_country VARCHAR(100),
    winner VARCHAR(100),
    runner_up VARCHAR(100),
    third_place VARCHAR(100),
    fourth_place VARCHAR(100),
    goals_scored INT,
    matches_played INT
);

CREATE TABLE IF NOT EXISTS silver.schedule_2026 (
    match_id VARCHAR(100) PRIMARY KEY,
    match_date DATE,
    stage VARCHAR(50),
    group_name VARCHAR(10),
    home_team VARCHAR(100),
    away_team VARCHAR(100),
    stadium VARCHAR(100),
    city VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS silver.matches (
    match_id VARCHAR(100) PRIMARY KEY,
    year INT,
    match_date DATE,
    stage VARCHAR(50),
    group_name VARCHAR(10),
    home_team VARCHAR(100),
    away_team VARCHAR(100),
    home_score INT,
    away_score INT,
    stadium VARCHAR(100),
    city VARCHAR(100),
    attendance INT
);

CREATE INDEX IF NOT EXISTS idx_fifa_rankings_country ON silver.fifa_rankings(country);
CREATE INDEX IF NOT EXISTS idx_fifa_rankings_year ON silver.fifa_rankings(year);
CREATE INDEX IF NOT EXISTS idx_matches_year ON silver.matches(year);
CREATE INDEX IF NOT EXISTS idx_matches_home_team ON silver.matches(home_team);
CREATE INDEX IF NOT EXISTS idx_schedule_2026_match_date ON silver.schedule_2026(match_date);
