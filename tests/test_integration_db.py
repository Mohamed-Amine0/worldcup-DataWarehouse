from sqlalchemy import text


def test_bronze_schemas_exist(db_engine):
    with db_engine.connect() as conn:
        schemas = conn.execute(
            text("SELECT schema_name FROM information_schema.schemata WHERE schema_name IN ('bronze','silver','gold')")
        ).fetchall()
    assert len(schemas) == 3


def test_bronze_metadata_populated(db_engine):
    with db_engine.connect() as conn:
        count = conn.execute(text("SELECT COUNT(*) FROM bronze.metadata")).scalar()
    assert count in (5, 6)


def test_bronze_tables_have_data(db_engine):
    with db_engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT table_name, row_count FROM bronze.metadata
                WHERE row_count > 0 ORDER BY table_name
                """
            )
        ).fetchall()
    assert len(rows) in (5, 6)


def test_silver_tables_populated(db_engine):
    tables = [
        "silver.fifa_rankings",
        "silver.world_cup_history",
        "silver.schedule_2026",
        "silver.matches",
    ]
    with db_engine.connect() as conn:
        for table in tables:
            count = conn.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar()
            assert count > 0, f"{table} est vide"


def test_silver_quality_metrics(db_engine):
    with db_engine.connect() as conn:
        count = conn.execute(text("SELECT COUNT(*) FROM silver.quality_metrics")).scalar()
    assert count == 4


def test_gold_tables_populated(db_engine):
    with db_engine.connect() as conn:
        team_count = conn.execute(text("SELECT COUNT(*) FROM gold.team_performance")).scalar()
        tournament_count = conn.execute(text("SELECT COUNT(*) FROM gold.tournament_stats")).scalar()
        predictions_count = conn.execute(text("SELECT COUNT(*) FROM gold.predictions_2026")).scalar()
    assert team_count > 100
    assert tournament_count >= 20
    assert predictions_count == 72


def test_gold_team_performance_kpis(db_engine):
    with db_engine.connect() as conn:
        row = conn.execute(
            text(
                """
                SELECT team, year, wins, losses, draws, points, goal_difference
                FROM gold.team_performance
                WHERE team = 'Brazil' AND year = 2002
                """
            )
        ).fetchone()
    assert row is not None
    assert row.wins >= 0


def test_gold_predictions_have_ranks(db_engine):
    with db_engine.connect() as conn:
        with_ranks = conn.execute(
            text(
                """
                SELECT COUNT(*) FROM gold.predictions_2026
                WHERE home_rank IS NOT NULL AND away_rank IS NOT NULL
                """
            )
        ).scalar()
        total = conn.execute(text("SELECT COUNT(*) FROM gold.predictions_2026")).scalar()
    assert with_ranks > total * 0.9
