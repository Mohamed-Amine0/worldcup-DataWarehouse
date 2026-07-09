import math
import os
import pandas as pd

# Path to the CSV source (can also be loaded from the DB if needed, but CSV is fast and guaranteed to be present)
DATA_DIR = os.getenv("DATA_DIR", "/data/source")
CSV_PATH = os.path.join(DATA_DIR, "elo_ratings_wc2026.csv")

# Load Elo data if file exists
if os.path.exists(CSV_PATH):
    df_elo = pd.read_csv(CSV_PATH)
else:
    # Fallback to local relative path if run outside docker
    local_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "data", "source", "elo_ratings_wc2026.csv")
    if os.path.exists(local_path):
        df_elo = pd.read_csv(local_path)
    else:
        df_elo = pd.DataFrame()

# If data loaded, compute latest snapshot per country
if not df_elo.empty:
    latest_elo = df_elo.groupby('country').apply(lambda x: x.loc[x['snapshot_date'].idxmax()]).reset_index(drop=True)
    elo_dict = latest_elo.set_index('country')['rating'].to_dict()
else:
    latest_elo = pd.DataFrame()
    elo_dict = {}

# Map to standardise team names between datasets (e.g. ELO ratings vs Schedule/FIFA ranks)
NAME_MAPPING = {
    "USA": "United States",
    "United States": "United States",
    "Korea Republic": "South Korea",
    "South Korea": "South Korea",
    "Iran": "IR Iran",
    "IR Iran": "IR Iran",
}

def get_clean_team_name(team_name):
    """Normalize team name for Elo lookup."""
    if not team_name:
        return team_name
    
    # Try direct mapping
    if team_name in NAME_MAPPING:
        return NAME_MAPPING[team_name]
    
    # Try reverse mapping or partial mapping
    for k, v in NAME_MAPPING.items():
        if team_name.lower() == k.lower() or team_name.lower() == v.lower():
            return v
            
    return team_name

def predict_match(team_a, team_b, home_team=None):
    """
    Predict a match between team_a and team_b using Elo ratings.
    Takes into account rating diff, home advantage (+100 Elo), historic goals, and appearances.
    """
    clean_a = get_clean_team_name(team_a)
    clean_b = get_clean_team_name(team_b)

    # Resolve ratings (default to 1500 if not found)
    rating_a = elo_dict.get(clean_a, 1500.0)
    rating_b = elo_dict.get(clean_b, 1500.0)
    
    # Check if either team has home advantage (+100 ELO points)
    home_advantage = 100 if home_team == team_a or home_team == clean_a else (-100 if home_team == team_b or home_team == clean_b else 0)

    # Basic Elo win probability formula
    prob_a = 1 / (1 + 10 ** ((rating_b - rating_a - home_advantage) / 400))
    
    # Simple draw probability estimate (around 22-26% in football matches, can adjust based on ratings similarity)
    # E.g. draw probability is higher if ratings are closer, but let's use a standard model:
    # P(draw) = 0.25 * exp(-((rating_a + home_advantage - rating_b) / 400)**2)
    rating_diff = (rating_a + home_advantage) - rating_b
    prob_draw = 0.25 * math.exp(- (rating_diff / 300) ** 2)
    
    # Distribute the rest between A and B
    remaining = 1.0 - prob_draw
    # Normalize probabilities
    prob_a_norm = prob_a * remaining
    prob_b_norm = (1.0 - prob_a) * remaining

    # Incorporate World Cup appearances / history to refine probabilities
    # Fetch historical data
    wc_app_a = 1
    wc_app_b = 1
    goals_for_a = 1.0
    matches_total_a = 1.0
    goals_for_b = 1.0
    matches_total_b = 1.0
    
    if not latest_elo.empty:
        stats_a = latest_elo[latest_elo['country'] == clean_a]
        stats_b = latest_elo[latest_elo['country'] == clean_b]
        
        if not stats_a.empty:
            wc_app_a = max(1, stats_a['wc_appearances'].values[0])
            goals_for_a = max(1.0, stats_a['goals_for'].values[0])
            matches_total_a = max(1.0, stats_a['matches_total'].values[0])
            
        if not stats_b.empty:
            wc_app_b = max(1, stats_b['wc_appearances'].values[0])
            goals_for_b = max(1.0, stats_b['goals_for'].values[0])
            matches_total_b = max(1.0, stats_b['matches_total'].values[0])

    # Expected goals using average historical goals/match modified by ELO ratio
    avg_goals_a = goals_for_a / matches_total_a
    avg_goals_b = goals_for_b / matches_total_b
    
    expected_goals_a = avg_goals_a * (rating_a / rating_b)
    expected_goals_b = avg_goals_b * (rating_b / rating_a)

    return {
        "team_a": team_a,
        "team_b": team_b,
        "rating_a": rating_a,
        "rating_b": rating_b,
        "winner_probabilities": {
            team_a: round(prob_a_norm, 4),
            team_b: round(prob_b_norm, 4),
            "draw": round(prob_draw, 4)
        },
        "expected_goals_a": round(expected_goals_a, 2),
        "expected_goals_b": round(expected_goals_b, 2),
        "expected_score": f"{expected_goals_a:.1f}-{expected_goals_b:.1f}"
    }
