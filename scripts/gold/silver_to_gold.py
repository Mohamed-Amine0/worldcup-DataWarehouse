import os
import sys
from datetime import datetime

from pyspark.sql.functions import avg, col, count, create_map, lit, max, sum, when

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.logging_config import setup_logging
from utils.spark_utils import create_spark_session, get_db_config

logger = setup_logging("gold_transformation")

TEAM_ALIASES = {
    "United States": "USA",
    "Bosnia-Herzegovina": "Bosnia and Herzegovina",
    "Cape Verde": "Cabo Verde",
    "Türkiye": "Turkey",
    "Korea Republic": "Korea Republic",
    "IR Iran": "Iran",
}


def normalize_team(column):
    alias_map = create_map(*[lit(x) for pair in TEAM_ALIASES.items() for x in pair])
    return when(alias_map.getItem(column).isNotNull(), alias_map.getItem(column)).otherwise(
        column
    )


def write_table(df, jdbc_url, db_properties, table_name):
    properties = {**db_properties, "truncate": "true"}
    df.write.jdbc(
        url=jdbc_url,
        table=table_name,
        mode="overwrite",
        properties=properties,
    )


def build_team_performance(matches, fifa_rankings):
    home_stats = matches.select(
        col("home_team").alias("team"),
        col("year"),
        col("home_score").alias("goals_scored"),
        col("away_score").alias("goals_conceded"),
        when(col("home_score") > col("away_score"), 1).otherwise(0).alias("win"),
        when(col("home_score") < col("away_score"), 1).otherwise(0).alias("loss"),
        when(col("home_score") == col("away_score"), 1).otherwise(0).alias("draw"),
    )

    away_stats = matches.select(
        col("away_team").alias("team"),
        col("year"),
        col("away_score").alias("goals_scored"),
        col("home_score").alias("goals_conceded"),
        when(col("away_score") > col("home_score"), 1).otherwise(0).alias("win"),
        when(col("away_score") < col("home_score"), 1).otherwise(0).alias("loss"),
        when(col("away_score") == col("home_score"), 1).otherwise(0).alias("draw"),
    )

    team_performance = (
        home_stats.unionByName(away_stats)
        .groupBy("team", "year")
        .agg(
            sum("win").alias("wins"),
            sum("loss").alias("losses"),
            sum("draw").alias("draws"),
            sum("goals_scored").alias("goals_scored"),
            sum("goals_conceded").alias("goals_conceded"),
            count("*").alias("matches_played"),
            avg("goals_scored").alias("avg_goals_scored"),
            avg("goals_conceded").alias("avg_goals_conceded"),
        )
        .withColumn("goal_difference", col("goals_scored") - col("goals_conceded"))
        .withColumn("points", (col("wins") * 3) + col("draws"))
    )

    fifa_avg = fifa_rankings.groupBy("country", "year").agg(
        avg("rank").alias("avg_rank")
    )

    return (
        team_performance.alias("tp")
        .join(
            fifa_avg.alias("fa"),
            (col("tp.team") == col("fa.country"))
            & (col("tp.year") == col("fa.year")),
            "left",
        )
        .select(
            col("tp.team").alias("team"),
            col("tp.year").alias("year"),
            col("tp.wins").alias("wins"),
            col("tp.losses").alias("losses"),
            col("tp.draws").alias("draws"),
            col("tp.matches_played").alias("matches_played"),
            col("tp.goals_scored").alias("goals_scored"),
            col("tp.goals_conceded").alias("goals_conceded"),
            col("tp.goal_difference").alias("goal_difference"),
            col("tp.points").alias("points"),
            col("tp.avg_goals_scored").alias("avg_goals_scored"),
            col("tp.avg_goals_conceded").alias("avg_goals_conceded"),
            col("fa.avg_rank").alias("avg_rank"),
        )
    )


def build_tournament_stats(matches):
    return matches.groupBy("year").agg(
        count("*").alias("total_matches"),
        (sum("home_score") + sum("away_score")).alias("total_goals"),
        avg(col("home_score") + col("away_score")).alias("avg_goals_per_match"),
        max(col("home_score") + col("away_score")).alias("max_goals_in_match"),
        sum(when(col("home_score") > col("away_score"), 1).otherwise(0)).alias(
            "home_wins"
        ),
        sum(when(col("home_score") < col("away_score"), 1).otherwise(0)).alias(
            "away_wins"
        ),
        sum(when(col("home_score") == col("away_score"), 1).otherwise(0)).alias(
            "draws"
        ),
    )


def build_predictions_2026(schedule_2026, fifa_rankings):
    schedule_norm = schedule_2026.withColumn(
        "home_team_norm", normalize_team(col("home_team"))
    ).withColumn("away_team_norm", normalize_team(col("away_team")))

    fifa_2026 = fifa_rankings.filter(col("year") == 2026).select(
        col("country").alias("home_country"),
        col("rank").alias("home_rank"),
    )
    fifa_2026_away = fifa_rankings.filter(col("year") == 2026).select(
        col("country").alias("away_country"),
        col("rank").alias("away_rank"),
    )

    schedule_with_ranks = (
        schedule_norm.join(
            fifa_2026,
            schedule_norm.home_team_norm == fifa_2026.home_country,
            "left",
        )
        .drop("home_country")
        .join(
            fifa_2026_away,
            schedule_norm.away_team_norm == fifa_2026_away.away_country,
            "left",
        )
        .drop("away_country")
    )

    return schedule_with_ranks.select(
        col("match_id"),
        col("home_team"),
        col("away_team"),
        col("match_date"),
        col("home_rank").cast("int"),
        col("away_rank").cast("int"),
        when(col("home_rank") < col("away_rank"), col("home_team"))
        .when(col("away_rank") < col("home_rank"), col("away_team"))
        .otherwise(lit("Draw"))
        .alias("predicted_winner"),
    )


def main():
    start_time = datetime.now()
    spark = create_spark_session("SilverToGold")
    jdbc_url, db_properties = get_db_config()

    try:
        matches = spark.read.jdbc(
            jdbc_url, "silver.matches", properties=db_properties
        )
        fifa_rankings = spark.read.jdbc(
            jdbc_url, "silver.fifa_rankings", properties=db_properties
        )
        schedule_2026 = spark.read.jdbc(
            jdbc_url, "silver.schedule_2026", properties=db_properties
        )

        logger.info("Calcul des performances par équipe...")
        team_performance = build_team_performance(matches, fifa_rankings)
        write_table(team_performance, jdbc_url, db_properties, "gold.team_performance")
        logger.info(
            "Performances par équipe calculées",
            extra={
                "context": {
                    "table": "gold.team_performance",
                    "total_records": team_performance.count(),
                }
            },
        )

        logger.info("Calcul des statistiques par tournoi...")
        tournament_stats = build_tournament_stats(matches)
        write_table(tournament_stats, jdbc_url, db_properties, "gold.tournament_stats")
        logger.info(
            "Statistiques par tournoi calculées",
            extra={
                "context": {
                    "table": "gold.tournament_stats",
                    "total_records": tournament_stats.count(),
                }
            },
        )

        logger.info("Calcul des prédictions pour 2026...")
        predictions = build_predictions_2026(schedule_2026, fifa_rankings)
        write_table(predictions, jdbc_url, db_properties, "gold.predictions_2026")
        logger.info(
            "Prédictions 2026 calculées",
            extra={
                "context": {
                    "table": "gold.predictions_2026",
                    "total_records": predictions.count(),
                }
            },
        )

        logger.info(
            "Transformation Silver → Gold terminée",
            extra={
                "context": {
                    "total_processing_time_seconds": (
                        datetime.now() - start_time
                    ).total_seconds(),
                    "tables_processed": 3,
                }
            },
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
