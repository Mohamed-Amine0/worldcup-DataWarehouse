import os
import time
from typing import Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import Response
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest
from sqlalchemy import create_engine, text

DB_USER = os.getenv("DB_USER", "user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "password")
DB_HOST = os.getenv("DB_HOST", "postgres")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "world_cup_dw")

engine = create_engine(
    f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}",
    pool_pre_ping=True,
)

app = FastAPI(
    title="World Cup Data Warehouse API",
    description="API d'accès aux KPIs Gold (Coupe du Monde FIFA)",
    version="1.0.0",
)

REQUEST_COUNT = Counter(
    "fastapi_requests_total",
    "Total des requêtes API",
    ["method", "endpoint", "status"],
)
REQUEST_LATENCY = Histogram(
    "fastapi_request_duration_seconds",
    "Latence des requêtes API",
    ["endpoint"],
)


def fetch_all(query: str, params: Optional[dict] = None):
    with engine.connect() as conn:
        result = conn.execute(text(query), params or {})
        return [dict(row._mapping) for row in result]


@app.get("/health")
def health():
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    REQUEST_COUNT.labels("GET", "/health", "200").inc()
    return {"status": "ok", "database": "connected"}


@app.get("/metrics")
def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/api/v1/team-performance")
def team_performance(
    team: Optional[str] = Query(None),
    year: Optional[int] = Query(None),
    limit: int = Query(100, ge=1, le=1000),
):
    start = time.time()
    query = """
        SELECT team, year, wins, losses, draws, matches_played,
               goals_scored, goals_conceded, goal_difference, points,
               avg_goals_scored, avg_goals_conceded, avg_rank
        FROM gold.team_performance
        WHERE 1=1
    """
    params = {"limit": limit}
    if team:
        query += " AND team ILIKE :team"
        params["team"] = f"%{team}%"
    if year:
        query += " AND year = :year"
        params["year"] = year
    query += " ORDER BY year DESC, points DESC LIMIT :limit"

    data = fetch_all(query, params)
    REQUEST_LATENCY.labels("/api/v1/team-performance").observe(time.time() - start)
    REQUEST_COUNT.labels("GET", "/api/v1/team-performance", "200").inc()
    return {"count": len(data), "data": data}


@app.get("/api/v1/tournament-stats")
def tournament_stats():
    start = time.time()
    data = fetch_all(
        """
        SELECT year, total_matches, total_goals, avg_goals_per_match,
               max_goals_in_match, home_wins, away_wins, draws
        FROM gold.tournament_stats
        ORDER BY year DESC
        """
    )
    REQUEST_LATENCY.labels("/api/v1/tournament-stats").observe(time.time() - start)
    REQUEST_COUNT.labels("GET", "/api/v1/tournament-stats", "200").inc()
    return {"count": len(data), "data": data}


@app.get("/api/v1/predictions-2026")
def predictions_2026(limit: int = Query(100, ge=1, le=500)):
    start = time.time()
    data = fetch_all(
        """
        SELECT match_id, home_team, away_team, match_date,
               home_rank, away_rank, predicted_winner
        FROM gold.predictions_2026
        ORDER BY match_date
        LIMIT :limit
        """,
        {"limit": limit},
    )
    REQUEST_LATENCY.labels("/api/v1/predictions-2026").observe(time.time() - start)
    REQUEST_COUNT.labels("GET", "/api/v1/predictions-2026", "200").inc()
    return {"count": len(data), "data": data}


@app.get("/api/v1/top-teams")
def top_teams(limit: int = Query(10, ge=1, le=50)):
    start = time.time()
    data = fetch_all(
        """
        SELECT team, SUM(wins) AS total_wins, SUM(matches_played) AS total_matches,
               SUM(goals_scored) AS total_goals
        FROM gold.team_performance
        GROUP BY team
        ORDER BY total_wins DESC
        LIMIT :limit
        """,
        {"limit": limit},
    )
    REQUEST_LATENCY.labels("/api/v1/top-teams").observe(time.time() - start)
    REQUEST_COUNT.labels("GET", "/api/v1/top-teams", "200").inc()
    return {"count": len(data), "data": data}


@app.get("/api/v1/bronze/metadata")
def bronze_metadata():
    data = fetch_all(
        """
        SELECT table_name, source_file, ingestion_timestamp, row_count, schema_version
        FROM bronze.metadata
        ORDER BY table_name
        """
    )
    if not data:
        raise HTTPException(status_code=404, detail="Aucune métadonnée Bronze trouvée")
    REQUEST_COUNT.labels("GET", "/api/v1/bronze/metadata", "200").inc()
    return {"count": len(data), "data": data}


@app.get("/api/v1/silver/quality-metrics")
def silver_quality_metrics():
    data = fetch_all(
        """
        SELECT table_name, ingestion_timestamp, total_records, null_count,
               duplicate_count, invalid_records, processing_time_seconds
        FROM silver.quality_metrics
        ORDER BY table_name
        """
    )
    REQUEST_COUNT.labels("GET", "/api/v1/silver/quality-metrics", "200").inc()
    return {"count": len(data), "data": data}
