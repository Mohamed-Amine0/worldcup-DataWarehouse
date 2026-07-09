-- Table/Vue Silver pour stocker les prédictions Elo
CREATE OR REPLACE VIEW silver.match_predictions AS
WITH latest_elo AS (
    SELECT country, rating, goals_for, matches_total
    FROM bronze.elo_ratings
    WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM bronze.elo_ratings)
)
SELECT
    a.country AS team_a,
    b.country AS team_b,
    a.rating AS rating_a,
    b.rating AS rating_b,
    1 / (1 + POWER(10, (b.rating - a.rating) / 400.0)) AS prob_a_win,
    1 / (1 + POWER(10, (a.rating - b.rating) / 400.0)) AS prob_b_win,
    (a.goals_for / NULLIF(a.matches_total, 0)) * (a.rating / b.rating) AS expected_goals_a,
    (b.goals_for / NULLIF(b.matches_total, 0)) * (b.rating / a.rating) AS expected_goals_b
FROM latest_elo a
CROSS JOIN latest_elo b
WHERE a.country != b.country;
