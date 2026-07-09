import os

from pyspark.sql import SparkSession


def create_spark_session(app_name: str) -> SparkSession:
    """Crée une session Spark configurée pour PostgreSQL JDBC."""
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.driver.extraClassPath", "/opt/spark/jars/*")
        .config("spark.executor.extraClassPath", "/opt/spark/jars/*")
        .getOrCreate()
    )


def get_db_config():
    """Retourne la configuration JDBC PostgreSQL depuis les variables d'environnement."""
    user = os.getenv("DB_USER", "user")
    password = os.getenv("DB_PASSWORD", "password")
    host = os.getenv("DB_HOST", "postgres")
    port = os.getenv("DB_PORT", "5432")
    db_name = os.getenv("DB_NAME", "world_cup_dw")

    jdbc_url = f"jdbc:postgresql://{host}:{port}/{db_name}"
    db_properties = {
        "user": user,
        "password": password,
        "driver": "org.postgresql.Driver",
    }
    return jdbc_url, db_properties
