-- Création du schéma Silver pour les données nettoyées et structurées
CREATE SCHEMA IF NOT EXISTS silver;

-------------------------------------------------------------------------------
-- 1. Table des classements FIFA unifiée
-------------------------------------------------------------------------------
DROP TABLE IF EXISTS silver.fifa_rankings;
CREATE TABLE silver.fifa_rankings AS
SELECT 
    2022 AS ranking_year,
    team,
    team_code,
    association,
    CAST(NULLIF(rank, '') AS INT) AS rank,
    CAST(NULLIF(previous_rank, '') AS INT) AS previous_rank,
    CAST(NULLIF(points, '') AS NUMERIC) AS points,
    CAST(NULLIF(previous_points, '') AS NUMERIC) AS previous_points,
    CAST(NULL AS INT) AS rated_matches
FROM public.fifa_ranking_2022
UNION ALL
SELECT 
    2026 AS ranking_year,
    team,
    team_code,
    association,
    CAST(NULLIF(rank, '') AS INT) AS rank,
    CAST(NULLIF(previous_rank, '') AS INT) AS previous_rank,
    CAST(NULLIF(points, '') AS NUMERIC) AS points,
    CAST(NULLIF(previous_points, '') AS NUMERIC) AS previous_points,
    CAST(NULLIF(rated_matches, '') AS INT) AS rated_matches
FROM public.fifa_ranking_2026;

-------------------------------------------------------------------------------
-- 2. Table de l'historique des Coupes du Monde typée
-------------------------------------------------------------------------------
DROP TABLE IF EXISTS silver.world_cup_history;
CREATE TABLE silver.world_cup_history AS
SELECT 
    CAST(NULLIF(year, '') AS INT) AS year,
    host,
    CAST(NULLIF(teams, '') AS INT) AS teams,
    champion,
    runner_up,
    top_scorer,
    CAST(NULLIF(attendance, '') AS INT) AS attendance,
    CAST(NULLIF(attendance_avg, '') AS INT) AS attendance_avg,
    CAST(NULLIF(matches, '') AS INT) AS matches
FROM public.world_cup_history;

-------------------------------------------------------------------------------
-- 3. Table du calendrier 2026 typée
-------------------------------------------------------------------------------
DROP TABLE IF EXISTS silver.schedule_2026;
CREATE TABLE silver.schedule_2026 AS
SELECT 
    round,
    day,
    CAST(NULLIF(date, '') AS DATE) AS date,
    time,
    score,
    referee,
    notes,
    CAST(NULLIF(year, '') AS INT) AS year,
    home_team,
    away_team
FROM public.schedule_2026;

-------------------------------------------------------------------------------
-- 4. Table des matchs historiques (1930 - 2022) nettoyée et typée
-------------------------------------------------------------------------------
DROP TABLE IF EXISTS silver.matches;
CREATE TABLE silver.matches AS
SELECT 
    home_team,
    away_team,
    CAST(NULLIF(home_score, '') AS INT) AS home_score,
    CAST(NULLIF(NULLIF(home_xg, ''), 'None') AS NUMERIC) AS home_xg,
    CAST(NULLIF(NULLIF(home_penalty, ''), 'None') AS INT) AS home_penalty,
    CAST(NULLIF(away_score, '') AS INT) AS away_score,
    CAST(NULLIF(NULLIF(away_xg, ''), 'None') AS NUMERIC) AS away_xg,
    CAST(NULLIF(NULLIF(away_penalty, ''), 'None') AS INT) AS away_penalty,
    home_manager,
    home_captain,
    away_manager,
    away_captain,
    CAST(NULLIF(NULLIF(attendance, ''), 'None') AS INT) AS attendance,
    venue,
    officials,
    round,
    CAST(NULLIF(date, '') AS DATE) AS date,
    score,
    referee,
    notes,
    host,
    CAST(NULLIF(year, '') AS INT) AS year,
    -- On garde les listes d'événements complexes sous forme de texte pour analyse future
    home_goal,
    away_goal,
    home_goal_long,
    away_goal_long,
    home_own_goal,
    away_own_goal,
    home_penalty_goal,
    away_penalty_goal,
    home_penalty_miss_long,
    away_penalty_miss_long,
    home_penalty_shootout_goal_long,
    away_penalty_shootout_goal_long,
    home_penalty_shootout_miss_long,
    away_penalty_shootout_miss_long,
    home_red_card,
    away_red_card,
    home_yellow_red_card,
    away_yellow_red_card,
    home_yellow_card_long,
    away_yellow_card_long,
    home_substitute_in_long,
    away_substitute_in_long
FROM public.matches_1930_2022;
