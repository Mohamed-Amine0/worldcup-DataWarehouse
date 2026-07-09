import os
import sys
from datetime import datetime

from pyspark.sql.functions import avg, col, count, create_map, lit, max, sum, when, pow, exp, round

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
    "South Korea": "Korea Republic",
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


def build_predictions_2026(schedule_2026, fifa_rankings, elo_ratings):
    from pyspark.sql import Window
    from pyspark.sql.functions import row_number, desc, pow, exp, round

    # 1. Normalize schedule team names
    schedule_norm = schedule_2026.withColumn(
        "home_team_norm", normalize_team(col("home_team"))
    ).withColumn("away_team_norm", normalize_team(col("away_team")))

    # 2. Get latest Elo snapshot per country and normalize its country name
    windowSpec = Window.partitionBy("country").orderBy(desc("snapshot_date"))
    latest_elo = elo_ratings.withColumn("rn", row_number().over(windowSpec)).filter(col("rn") == 1).drop("rn")
    latest_elo_norm = latest_elo.withColumn("country_norm", normalize_team(col("country")))

    # 3. Join schedule with Elo for home team
    home_elo = latest_elo_norm.select(
        col("country_norm").alias("home_country_elo"),
        col("rating").alias("rating_home"),
        col("goals_for").alias("goals_for_home"),
        col("matches_total").alias("matches_total_home"),
        col("is_host").alias("home_is_host")
    )
    
    # Join schedule with Elo for away team
    away_elo = latest_elo_norm.select(
        col("country_norm").alias("away_country_elo"),
        col("rating").alias("rating_away"),
        col("goals_for").alias("goals_for_away"),
        col("matches_total").alias("matches_total_away"),
        col("is_host").alias("away_is_host")
    )

    # Join schedule with FIFA ranks for home team
    fifa_2026_home = fifa_rankings.filter(col("year") == 2026).select(
        col("country").alias("home_country_fifa"),
        col("rank").alias("home_rank")
    )

    # Join schedule with FIFA ranks for away team
    fifa_2026_away = fifa_rankings.filter(col("year") == 2026).select(
        col("country").alias("away_country_fifa"),
        col("rank").alias("away_rank")
    )

    # Perform joins
    joined = schedule_norm \
        .join(home_elo, schedule_norm.home_team_norm == home_elo.home_country_elo, "left") \
        .join(away_elo, schedule_norm.away_team_norm == away_elo.away_country_elo, "left") \
        .join(fifa_2026_home, schedule_norm.home_team_norm == fifa_2026_home.home_country_fifa, "left") \
        .join(fifa_2026_away, schedule_norm.away_team_norm == fifa_2026_away.away_country_fifa, "left")

    # Fill null ratings/stats with defaults
    joined_filled = joined \
        .na.fill({"rating_home": 1500.0, "rating_away": 1500.0, 
                  "goals_for_home": 1.0, "goals_for_away": 1.0, 
                  "matches_total_home": 1, "matches_total_away": 1,
                  "home_is_host": 0, "away_is_host": 0})

    # 4. Perform ELO predictions
    home_adv = when(col("home_is_host") == 1, 100).when(col("away_is_host") == 1, -100).otherwise(0)
    rating_diff = col("rating_home") + home_adv - col("rating_away")
    
    prob_home_base = 1.0 / (1.0 + pow(lit(10.0), -rating_diff / 400.0))
    prob_draw = lit(0.25) * exp(- pow(rating_diff / 300.0, 2.0))
    remaining = lit(1.0) - prob_draw
    prob_home_win = prob_home_base * remaining
    prob_away_win = (lit(1.0) - prob_home_base) * remaining

    # Expected goals
    expected_goals_home = (col("goals_for_home") / col("matches_total_home")) * (col("rating_home") / col("rating_away"))
    expected_goals_away = (col("goals_for_away") / col("matches_total_away")) * (col("rating_away") / col("rating_home"))

    # Predicted Winner
    predicted_winner = when((prob_home_win > prob_away_win) & (prob_home_win > prob_draw), col("home_team")) \
                       .when((prob_away_win > prob_home_win) & (prob_away_win > prob_draw), col("away_team")) \
                       .otherwise(lit("Draw"))

    # Add columns to dataframe
    predicted_df = joined_filled.withColumn("rating_home", col("rating_home")) \
        .withColumn("rating_away", col("rating_away")) \
        .withColumn("prob_home_win", round(prob_home_win, 4)) \
        .withColumn("prob_away_win", round(prob_away_win, 4)) \
        .withColumn("prob_draw", round(prob_draw, 4)) \
        .withColumn("expected_goals_home", round(expected_goals_home, 2)) \
        .withColumn("expected_goals_away", round(expected_goals_away, 2)) \
        .withColumn("predicted_winner", predicted_winner)

    return predicted_df


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
        elo_ratings = spark.read.jdbc(
            jdbc_url, "bronze.elo_ratings", properties=db_properties
        )
        predictions = build_predictions_2026(schedule_2026, fifa_rankings, elo_ratings)
        
        # Legacy table predictions_2026 (required by tests)
        predictions_legacy = predictions.select(
            "match_id", "home_team", "away_team", "match_date", "home_rank", "away_rank", "predicted_winner"
        )
        write_table(predictions_legacy, jdbc_url, db_properties, "gold.predictions_2026")
        
        # Rich table tournament_predictions
        predictions_rich = predictions.select(
            "match_id", "home_team", "away_team", "match_date", "home_rank", "away_rank",
            "rating_home", "rating_away", "prob_home_win", "prob_away_win", "prob_draw",
            "expected_goals_home", "expected_goals_away", "predicted_winner"
        )
        write_table(predictions_rich, jdbc_url, db_properties, "gold.tournament_predictions")

        logger.info(
            "Prédictions 2026 calculées (avec tables de prédiction standard et enrichie)",
            extra={
                "context": {
                    "table_legacy": "gold.predictions_2026",
                    "table_rich": "gold.tournament_predictions",
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
                    "tables_processed": 4,
                }
            },
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
