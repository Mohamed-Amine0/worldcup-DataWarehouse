import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_USER = os.getenv("DB_USER", "user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "password")
DB_NAME = os.getenv("DB_NAME", "world_cup_dw")


@pytest.fixture(scope="session")
def db_engine():
    try:
        from sqlalchemy import create_engine, text
    except ImportError as exc:
        pytest.skip(f"SQLAlchemy indisponible: {exc}")

    try:
        import psycopg2  # noqa: F401
    except ImportError:
        pytest.skip("psycopg2-binary requis pour les tests d'intégration DB")

    engine = create_engine(
        f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
    except Exception as exc:
        pytest.skip(f"PostgreSQL indisponible: {exc}")
    return engine
