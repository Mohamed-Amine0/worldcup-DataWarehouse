-- Création des tables brutes (Staging / Bronze)

-- 1. Classement FIFA 2022
CREATE TABLE IF NOT EXISTS fifa_ranking_2022 (
    team TEXT,
    team_code TEXT,
    association TEXT,
    rank TEXT,
    previous_rank TEXT,
    points TEXT,
    previous_points TEXT
);

-- 2. Classement FIFA 2026
CREATE TABLE IF NOT EXISTS fifa_ranking_2026 (
    team TEXT,
    team_code TEXT,
    association TEXT,
    rank TEXT,
    previous_rank TEXT,
    points TEXT,
    previous_points TEXT,
    rated_matches TEXT
);

-- 3. Historique des Coupes du Monde
CREATE TABLE IF NOT EXISTS world_cup_history (
    year TEXT,
    host TEXT,
    teams TEXT,
    champion TEXT,
    runner_up TEXT,
    top_scorer TEXT,
    attendance TEXT,
    attendance_avg TEXT,
    matches TEXT
);

-- 4. Calendrier 2026
CREATE TABLE IF NOT EXISTS schedule_2026 (
    round TEXT,
    day TEXT,
    date TEXT,
    time TEXT,
    score TEXT,
    referee TEXT,
    notes TEXT,
    year TEXT,
    home_team TEXT,
    away_team TEXT
);

-- 5. Historique des Matchs (1930 - 2022)
CREATE TABLE IF NOT EXISTS matches_1930_2022 (
    home_team TEXT,
    away_team TEXT,
    home_score TEXT,
    home_xg TEXT,
    home_penalty TEXT,
    away_score TEXT,
    away_xg TEXT,
    away_penalty TEXT,
    home_manager TEXT,
    home_captain TEXT,
    away_manager TEXT,
    away_captain TEXT,
    attendance TEXT,
    venue TEXT,
    officials TEXT,
    round TEXT,
    date TEXT,
    score TEXT,
    referee TEXT,
    notes TEXT,
    host TEXT,
    year TEXT,
    home_goal TEXT,
    away_goal TEXT,
    home_goal_long TEXT,
    away_goal_long TEXT,
    home_own_goal TEXT,
    away_own_goal TEXT,
    home_penalty_goal TEXT,
    away_penalty_goal TEXT,
    home_penalty_miss_long TEXT,
    away_penalty_miss_long TEXT,
    home_penalty_shootout_goal_long TEXT,
    away_penalty_shootout_goal_long TEXT,
    home_penalty_shootout_miss_long TEXT,
    away_penalty_shootout_miss_long TEXT,
    home_red_card TEXT,
    away_red_card TEXT,
    home_yellow_red_card TEXT,
    away_yellow_red_card TEXT,
    home_yellow_card_long TEXT,
    away_yellow_card_long TEXT,
    home_substitute_in_long TEXT,
    away_substitute_in_long TEXT
);

-- Importation des données via la commande COPY (les chemins pointent vers le volume monté dans /tmp/source)
COPY fifa_ranking_2022 FROM '/tmp/source/WorldCupArchive/fifa_ranking_2022-10-06.csv' DELIMITER ',' CSV HEADER;
COPY fifa_ranking_2026 FROM '/tmp/source/WorldCupArchive/fifa_ranking_2026-06-08.csv' DELIMITER ',' CSV HEADER;
COPY world_cup_history FROM '/tmp/source/WorldCupArchive/world_cup.csv' DELIMITER ',' CSV HEADER;
COPY schedule_2026 FROM '/tmp/source/WorldCupArchive/schedule_2026.csv' DELIMITER ',' CSV HEADER;
COPY matches_1930_2022 FROM '/tmp/source/WorldCupArchive/matches_1930_2022.csv' DELIMITER ',' CSV HEADER;
