-- Schéma Gold pour les KPIs
CREATE SCHEMA IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS gold.team_performance (
    team VARCHAR(100),
    year INT,
    wins INT,
    losses INT,
    draws INT,
    matches_played INT,
    goals_scored INT,
    goals_conceded INT,
    goal_difference INT,
    points INT,
    avg_goals_scored FLOAT,
    avg_goals_conceded FLOAT,
    avg_rank FLOAT,
    PRIMARY KEY (team, year)
);

CREATE TABLE IF NOT EXISTS gold.tournament_stats (
    year INT PRIMARY KEY,
    total_matches INT,
    total_goals INT,
    avg_goals_per_match FLOAT,
    max_goals_in_match INT,
    home_wins INT,
    away_wins INT,
    draws INT
);

CREATE TABLE IF NOT EXISTS gold.predictions_2026 (
    match_id VARCHAR(100),
    home_team VARCHAR(100),
    away_team VARCHAR(100),
    match_date DATE,
    home_rank INT,
    away_rank INT,
    predicted_winner VARCHAR(100),
    PRIMARY KEY (match_id)
);

CREATE INDEX IF NOT EXISTS idx_team_performance_team ON gold.team_performance(team);
CREATE INDEX IF NOT EXISTS idx_team_performance_year ON gold.team_performance(year);
CREATE INDEX IF NOT EXISTS idx_predictions_match_date ON gold.predictions_2026(match_date);
