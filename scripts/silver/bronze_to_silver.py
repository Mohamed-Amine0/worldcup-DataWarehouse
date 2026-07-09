import os
import sys
from datetime import datetime

from pyspark.sql.functions import (
    coalesce,
    col,
    concat_ws,
    isnull,
    lit,
    regexp_extract,
    split,
    to_date,
    trim,
    when,
)

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from silver.quality_metrics import store_quality_metrics
from utils.logging_config import setup_logging
from utils.spark_utils import create_spark_session, get_db_config

logger = setup_logging("silver_transformation")


def count_nulls(df):
    return sum(df.filter(isnull(c)).count() for c in df.columns)


def write_table(df, jdbc_url, db_properties, table_name):
    properties = {**db_properties, "truncate": "true"}
    df.write.jdbc(
        url=jdbc_url,
        table=table_name,
        mode="overwrite",
        properties=properties,
    )


def transform_fifa_rankings(spark, jdbc_url, db_properties, start_time):
    logger.info("Traitement des classements FIFA...")
    rankings_2022 = spark.read.jdbc(
        jdbc_url, "bronze.fifa_rankings_2022", properties=db_properties
    )
    rankings_2026 = spark.read.jdbc(
        jdbc_url, "bronze.fifa_rankings_2026", properties=db_properties
    )

    def normalize(df, year):
        country_col = "team" if "team" in df.columns else "country"
        return (
            df.withColumn("rank", col("rank").cast("int"))
            .withColumn("points", col("points").cast("double").cast("int"))
            .withColumn("country", trim(col(country_col)))
            .withColumn("year", lit(year))
            .select("rank", "country", "points", "year")
        )

    fifa_rankings = normalize(rankings_2022, 2022).unionByName(
        normalize(rankings_2026, 2026)
    )
    before_dedup = fifa_rankings.count()
    fifa_rankings = fifa_rankings.dropDuplicates(["country", "year"])
    duplicate_count = before_dedup - fifa_rankings.count()
    null_count = count_nulls(fifa_rankings)

    write_table(fifa_rankings, jdbc_url, db_properties, "silver.fifa_rankings")
    elapsed = (datetime.now() - start_time).total_seconds()
    total = fifa_rankings.count()

    store_quality_metrics(
        spark,
        jdbc_url,
        db_properties,
        "silver.fifa_rankings",
        total,
        null_count,
        duplicate_count,
        0,
        elapsed,
    )

    logger.info(
        "Classements FIFA traités",
        extra={
            "context": {
                "table": "silver.fifa_rankings",
                "total_records": total,
                "null_count": null_count,
                "duplicate_count": duplicate_count,
                "processing_time_seconds": elapsed,
            }
        },
    )


def transform_world_cup_history(spark, jdbc_url, db_properties, start_time):
    logger.info("Traitement de l'historique des Coupes du Monde...")
    history = spark.read.jdbc(
        jdbc_url, "bronze.world_cup_history", properties=db_properties
    )

    history_clean = (
        history.withColumn("year", col("Year").cast("int"))
        .withColumn("host_country", coalesce(col("Host"), lit("Unknown")))
        .withColumn("winner", coalesce(col("Champion"), lit("Unknown")))
        .withColumn("runner_up", coalesce(col("Runner-Up"), lit("Unknown")))
        .withColumn("third_place", lit(None).cast("string"))
        .withColumn("fourth_place", lit(None).cast("string"))
        .withColumn("goals_scored", lit(None).cast("int"))
        .withColumn("matches_played", coalesce(col("Matches").cast("int"), lit(0)))
        .select(
            "year",
            "host_country",
            "winner",
            "runner_up",
            "third_place",
            "fourth_place",
            "goals_scored",
            "matches_played",
        )
        .dropDuplicates(["year"])
    )

    null_count = count_nulls(history_clean)
    write_table(history_clean, jdbc_url, db_properties, "silver.world_cup_history")
    elapsed = (datetime.now() - start_time).total_seconds()
    total = history_clean.count()

    store_quality_metrics(
        spark,
        jdbc_url,
        db_properties,
        "silver.world_cup_history",
        total,
        null_count,
        0,
        0,
        elapsed,
    )

    logger.info(
        "Historique des Coupes du Monde traité",
        extra={
            "context": {
                "table": "silver.world_cup_history",
                "total_records": total,
                "processing_time_seconds": elapsed,
            }
        },
    )


def transform_schedule_2026(spark, jdbc_url, db_properties, start_time):
    logger.info("Traitement du calendrier 2026...")
    schedule = spark.read.jdbc(
        jdbc_url, "bronze.schedule_2026", properties=db_properties
    )

    schedule_clean = (
        schedule.withColumn("match_date", to_date(col("Date"), "yyyy-MM-dd"))
        .withColumn("stage", coalesce(col("Round"), lit("Unknown")))
        .withColumn("group_name", lit("TBD"))
        .withColumn("stadium", lit("TBD"))
        .withColumn("city", lit("TBD"))
        .withColumn(
            "match_id",
            concat_ws(
                "_",
                col("Date"),
                col("home_team"),
                col("away_team"),
            ),
        )
        .select(
            "match_id",
            "match_date",
            "stage",
            "group_name",
            "home_team",
            "away_team",
            "stadium",
            "city",
        )
        .dropDuplicates(["match_id"])
    )

    write_table(schedule_clean, jdbc_url, db_properties, "silver.schedule_2026")
    elapsed = (datetime.now() - start_time).total_seconds()
    total = schedule_clean.count()

    store_quality_metrics(
        spark,
        jdbc_url,
        db_properties,
        "silver.schedule_2026",
        total,
        0,
        0,
        0,
        elapsed,
    )

    logger.info(
        "Calendrier 2026 traité",
        extra={
            "context": {
                "table": "silver.schedule_2026",
                "total_records": total,
                "processing_time_seconds": elapsed,
            }
        },
    )


def transform_matches(spark, jdbc_url, db_properties, start_time):
    logger.info("Traitement des matchs historiques...")
    matches = spark.read.jdbc(
        jdbc_url, "bronze.matches_1930_2022", properties=db_properties
    )

    date_col = "Date" if "Date" in matches.columns else "match_date"
    year_col = "Year" if "Year" in matches.columns else "year"
    stage_col = "Round" if "Round" in matches.columns else "stage"
    attendance_col = "Attendance" if "Attendance" in matches.columns else "attendance"
    venue_col = "Venue" if "Venue" in matches.columns else "stadium"

    matches_clean = (
        matches.withColumn("year", col(year_col).cast("int"))
        .withColumn("match_date", to_date(col(date_col), "yyyy-MM-dd"))
        .withColumn("stage", coalesce(col(stage_col), lit("Unknown")))
        .withColumn("group_name", lit(None).cast("string"))
        .withColumn("home_team", coalesce(col("home_team"), lit("Unknown")))
        .withColumn("away_team", coalesce(col("away_team"), lit("Unknown")))
        .withColumn("home_score", coalesce(col("home_score"), lit(0)).cast("int"))
        .withColumn("away_score", coalesce(col("away_score"), lit(0)).cast("int"))
        .withColumn("attendance", coalesce(col(attendance_col), lit(0)).cast("int"))
        .withColumn("stadium", coalesce(split(col(venue_col), ",").getItem(0), lit("Unknown")))
        .withColumn(
            "city",
            coalesce(
                trim(regexp_extract(col(venue_col), r",\s*(.+)$", 1)),
                lit("Unknown"),
            ),
        )
        .withColumn(
            "match_id",
            concat_ws("_", col(date_col), col("home_team"), col("away_team")),
        )
        .select(
            "match_id",
            "year",
            "match_date",
            "stage",
            "group_name",
            "home_team",
            "away_team",
            "home_score",
            "away_score",
            "stadium",
            "city",
            "attendance",
        )
    )

    before_dedup = matches_clean.count()
    matches_clean = matches_clean.dropDuplicates(["match_id"])
    duplicate_count = before_dedup - matches_clean.count()

    write_table(matches_clean, jdbc_url, db_properties, "silver.matches")
    elapsed = (datetime.now() - start_time).total_seconds()
    total = matches_clean.count()

    store_quality_metrics(
        spark,
        jdbc_url,
        db_properties,
        "silver.matches",
        total,
        0,
        duplicate_count,
        0,
        elapsed,
    )

    logger.info(
        "Matchs historiques traités",
        extra={
            "context": {
                "table": "silver.matches",
                "total_records": total,
                "duplicate_count": duplicate_count,
                "processing_time_seconds": elapsed,
            }
        },
    )


def main():
    start_time = datetime.now()
    spark = create_spark_session("BronzeToSilver")
    jdbc_url, db_properties = get_db_config()

    try:
        transform_fifa_rankings(spark, jdbc_url, db_properties, start_time)
        transform_world_cup_history(spark, jdbc_url, db_properties, start_time)
        transform_schedule_2026(spark, jdbc_url, db_properties, start_time)
        transform_matches(spark, jdbc_url, db_properties, start_time)

        logger.info(
            "Transformation Bronze → Silver terminée",
            extra={
                "context": {
                    "total_processing_time_seconds": (
                        datetime.now() - start_time
                    ).total_seconds(),
                    "tables_processed": 4,
                }
            },
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
