import os

import pandas as pd
import pytest


DATA_DIR = os.path.join(
    os.path.dirname(__file__), "..", "data", "source"
)


@pytest.mark.parametrize(
    "filename,min_rows",
    [
        ("fifa_ranking_2022-10-06.csv", 200),
        ("fifa_ranking_2026-06-08.csv", 200),
        ("world_cup.csv", 20),
        ("schedule_2026.csv", 70),
        ("matches_1930_2022.csv", 900),
    ],
)
def test_source_csv_files_exist_and_not_empty(filename, min_rows):
    path = os.path.join(DATA_DIR, filename)
    assert os.path.exists(path), f"Fichier manquant: {filename}"
    df = pd.read_csv(path)
    assert len(df) >= min_rows


def test_fifa_rankings_columns():
    df = pd.read_csv(os.path.join(DATA_DIR, "fifa_ranking_2022-10-06.csv"))
    assert {"team", "rank", "points"}.issubset(df.columns)


def test_matches_required_columns():
    df = pd.read_csv(os.path.join(DATA_DIR, "matches_1930_2022.csv"))
    required = {"home_team", "away_team", "home_score", "away_score", "Date", "Year"}
    assert required.issubset(df.columns)


def test_schedule_required_columns():
    df = pd.read_csv(os.path.join(DATA_DIR, "schedule_2026.csv"))
    required = {"Date", "home_team", "away_team", "Round"}
    assert required.issubset(df.columns)
