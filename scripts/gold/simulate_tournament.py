import os
import sys
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, udf
from pyspark.sql.types import StringType, DoubleType, StructType, StructField

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.logging_config import setup_logging
from utils.spark_utils import create_spark_session, get_db_config
from gold.prediction_utils import predict_match, get_clean_team_name, elo_dict

logger = setup_logging("tournament_simulation")


def write_table(df, jdbc_url, db_properties, table_name, truncate=True):
    properties = {**db_properties}
    if truncate:
        properties["truncate"] = "true"
    df.write.jdbc(
        url=jdbc_url,
        table=table_name,
        mode="overwrite",
        properties=properties,
    )


def main():
    start_time = datetime.now()
    spark = create_spark_session("TournamentSimulation")
    jdbc_url, db_properties = get_db_config()

    try:
        logger.info("Lecture du calendrier et des classements...")
        schedule_2026 = spark.read.jdbc(
            jdbc_url, "silver.schedule_2026", properties=db_properties
        )
        fifa_rankings = spark.read.jdbc(
            jdbc_url, "silver.fifa_rankings", properties=db_properties
        )
        
        # Load ELO ratings from bronze
        elo_ratings = spark.read.jdbc(
            jdbc_url, "bronze.elo_ratings", properties=db_properties
        )

        logger.info("Conversion en Pandas pour application de la logique Elo...")
        schedule_pd = schedule_2026.toPandas()
        fifa_pd = fifa_rankings.filter(col("year") == 2026).toPandas()
        
        # Create FIFA rank dicts
        fifa_rank_dict = fifa_pd.set_index('country')['rank'].to_dict()

        predictions_list = []
        for _, row in schedule_pd.iterrows():
            match_id = row['match_id']
            home_team = row['home_team']
            away_team = row['away_team']
            match_date = row['match_date']
            
            # Predict
            pred = predict_match(home_team, away_team)
            
            # Get FIFA ranks (using basic normalization if needed)
            home_rank = fifa_rank_dict.get(home_team, fifa_rank_dict.get(get_clean_team_name(home_team), None))
            away_rank = fifa_rank_dict.get(away_team, fifa_rank_dict.get(get_clean_team_name(away_team), None))
            
            # Determine predicted winner based on Elo probabilities
            probs = pred["winner_probabilities"]
            prob_home = probs[home_team]
            prob_away = probs[away_team]
            prob_draw = probs["draw"]
            
            if prob_home > prob_away and prob_home > prob_draw:
                predicted_winner = home_team
            elif prob_away > prob_home and prob_away > prob_draw:
                predicted_winner = away_team
            else:
                predicted_winner = "Draw"

            predictions_list.append({
                "match_id": match_id,
                "home_team": home_team,
                "away_team": away_team,
                "match_date": match_date,
                "home_rank": int(home_rank) if home_rank is not None else None,
                "away_rank": int(away_rank) if away_rank is not None else None,
                "rating_home": float(pred["rating_a"]),
                "rating_away": float(pred["rating_b"]),
                "prob_home_win": float(prob_home),
                "prob_away_win": float(prob_away),
                "prob_draw": float(prob_draw),
                "expected_goals_home": float(pred["expected_goals_a"]),
                "expected_goals_away": float(pred["expected_goals_b"]),
                "expected_score": pred["expected_score"],
                "predicted_winner": predicted_winner
            })

        # Create Spark Dataframe
        schema = StructType([
            StructField("match_id", StringType(), False),
            StructField("home_team", StringType(), True),
            StructField("away_team", StringType(), True),
            StructField("match_date", StringType(), True),
            StructField("home_rank", StructField("home_rank", StringType(), True).dataType, True),
            StructField("away_rank", StructField("away_rank", StringType(), True).dataType, True),
            StructField("rating_home", DoubleType(), True),
            StructField("rating_away", DoubleType(), True),
            StructField("prob_home_win", DoubleType(), True),
            StructField("prob_away_win", DoubleType(), True),
            StructField("prob_draw", DoubleType(), True),
            StructField("expected_goals_home", DoubleType(), True),
            StructField("expected_goals_away", DoubleType(), True),
            StructField("expected_score", StringType(), True),
            StructField("predicted_winner", StringType(), True),
        ])
        
        predictions_df = spark.createDataFrame(predictions_list, schema=schema)
        
        # Cast match_date to DATE type
        predictions_df = predictions_df.withColumn("match_date", col("match_date").cast("date"))
        predictions_df = predictions_df.withColumn("home_rank", col("home_rank").cast("int"))
        predictions_df = predictions_df.withColumn("away_rank", col("away_rank").cast("int"))

        # Write to gold.predictions_2026 (keeping the original schema/columns for test compatibility)
        legacy_predictions_df = predictions_df.select(
            "match_id", "home_team", "away_team", "match_date", "home_rank", "away_rank", "predicted_winner"
        )
        write_table(legacy_predictions_df, jdbc_url, db_properties, "gold.predictions_2026", truncate=True)
        logger.info("predictions_2026 table populated successfully.")

        # Write to new gold.tournament_predictions (rich table with all ELO columns)
        write_table(predictions_df, jdbc_url, db_properties, "gold.tournament_predictions", truncate=False)
        logger.info("tournament_predictions table populated successfully.")

        logger.info(
            "Simulation du tournoi terminée avec succès !",
            extra={
                "context": {
                    "total_processing_time_seconds": (
                        datetime.now() - start_time
                    ).total_seconds(),
                    "matches_simulated": len(predictions_list),
                }
            },
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
