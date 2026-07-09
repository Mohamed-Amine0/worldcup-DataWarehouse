-- Vues Gold pour les dashboards
CREATE OR REPLACE VIEW gold.v_team_performance AS
SELECT
    team,
    year,
    wins,
    losses,
    draws,
    matches_played,
    goals_scored,
    goals_conceded,
    goal_difference,
    points,
    avg_goals_scored,
    avg_goals_conceded,
    avg_rank
FROM gold.team_performance;

CREATE OR REPLACE VIEW gold.v_tournament_stats AS
SELECT
    year,
    total_matches,
    total_goals,
    avg_goals_per_match,
    max_goals_in_match,
    home_wins,
    away_wins,
    draws
FROM gold.tournament_stats;
