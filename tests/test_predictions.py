import os
import sys
from sqlalchemy import text

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts.gold.prediction_utils import predict_match


def test_predict_match_logic():
    # Test basic prediction logic
    res = predict_match("France", "Germany")
    assert "winner_probabilities" in res
    assert "France" in res["winner_probabilities"]
    assert "Germany" in res["winner_probabilities"]
    assert "draw" in res["winner_probabilities"]
    assert "expected_score" in res
    
    # France should have a high ELO rating
    assert res["rating_a"] > 1800
    assert res["rating_b"] > 1800


def test_silver_predictions_view_exists_and_populated(db_engine):
    with db_engine.connect() as conn:
        row = conn.execute(text("SELECT COUNT(*) FROM silver.match_predictions")).scalar()
    assert row > 0


def test_gold_tournament_predictions_populated(db_engine):
    with db_engine.connect() as conn:
        count = conn.execute(text("SELECT COUNT(*) FROM gold.tournament_predictions")).scalar()
        # Should have 72 matches simulated
        assert count == 72
        
        # Check that we have the new ELO columns
        columns = conn.execute(text("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_schema = 'gold' AND table_name = 'tournament_predictions'
        """)).fetchall()
        column_names = [col[0] for col in columns]
        
        assert "rating_home" in column_names
        assert "rating_away" in column_names
        assert "prob_home_win" in column_names
        assert "prob_away_win" in column_names
        assert "prob_draw" in column_names
        assert "expected_goals_home" in column_names
        assert "expected_goals_away" in column_names
