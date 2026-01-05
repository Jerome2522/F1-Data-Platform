from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, avg, stddev, desc, when, lit
from pyspark.sql.window import Window
import os

# Constants
RAW_DIR = "/data/raw"
PROCESSED_DIR = "/data/processed"

def main():
    print("Starting Spark Processing...")

    # Initialize Spark Session
    spark = SparkSession.builder \
        .appName("F1DataProcessing") \
        .config("spark.master", "local[*]") \
        .getOrCreate()

    # Load Raw Data
    drivers_df = spark.read.csv(f"{RAW_DIR}/drivers.csv", header=True, inferSchema=True)
    constructors_df = spark.read.csv(f"{RAW_DIR}/constructors.csv", header=True, inferSchema=True)
    races_df = spark.read.csv(f"{RAW_DIR}/races.csv", header=True, inferSchema=True)
    results_df = spark.read.csv(f"{RAW_DIR}/results.csv", header=True, inferSchema=True)

    # 1. Driver Standings by Season
    # Group by driver and sum points
    driver_standings = results_df.groupBy("driver_id") \
        .agg(sum("points").alias("total_points"), count("race_id").alias("races_entered")) \
        .join(drivers_df, "driver_id") \
        .select("driver_id", "forename", "surname", "total_points", "races_entered") \
        .orderBy(desc("total_points"))
    
    driver_standings.write.mode("overwrite").parquet(f"{PROCESSED_DIR}/driver_standings.parquet")
    print("Computed Driver Standings.")

    # 2. Constructor Standings by Season
    constructor_standings = results_df.groupBy("constructor_id") \
        .agg(sum("points").alias("total_points")) \
        .join(constructors_df, "constructor_id") \
        .select("constructor_id", "name", "total_points") \
        .orderBy(desc("total_points"))

    constructor_standings.write.mode("overwrite").parquet(f"{PROCESSED_DIR}/constructor_standings.parquet")
    print("Computed Constructor Standings.")

    # 3. Top Drivers by Total Wins
    wins_df = results_df.filter(col("position") == 1) \
        .groupBy("driver_id") \
        .count().alias("wins") \
        .withColumnRenamed("count", "wins") \
        .join(drivers_df, "driver_id") \
        .select("driver_id", "forename", "surname", "wins") \
        .orderBy(desc("wins"))
    
    wins_df.write.mode("overwrite").parquet(f"{PROCESSED_DIR}/driver_wins.parquet")
    print("Computed Driver Wins.")

    # 4. Driver Points Progression
    results_with_race = results_df.join(races_df, results_df.race_id == races_df.round, "inner") \
        .select(results_df.driver_id, races_df.round, races_df.name.alias("race_name"), results_df.points)
    
    window_spec = Window.partitionBy("driver_id").orderBy("round").rowsBetween(Window.unboundedPreceding, Window.currentRow)
    
    points_progression = results_with_race.withColumn("cumulative_points", sum("points").over(window_spec)) \
        .join(drivers_df, "driver_id") \
        .select("driver_id", "forename", "surname", "round", "race_name", "cumulative_points")
    
    points_progression.write.mode("overwrite").parquet(f"{PROCESSED_DIR}/driver_points_progression.parquet")
    print("Computed Points Progression.")

    # 5. Driver Consistency Index
    consistency_df = results_df.withColumn("position_int", col("position").cast("int")) \
        .filter(col("position_int").isNotNull()) \
        .groupBy("driver_id") \
        .agg(
            avg("position_int").alias("avg_position"),
            stddev("position_int").alias("position_variance")
        ) \
        .join(drivers_df, "driver_id") \
        .select("driver_id", "forename", "surname", "avg_position", "position_variance") \
        .orderBy("position_variance")
        
    consistency_df.write.mode("overwrite").parquet(f"{PROCESSED_DIR}/driver_consistency.parquet")
    print("Computed Consistency Index.")

    # 6. Constructor Reliability Score
    reliability_df = results_df.withColumn("is_finished", when(col("position_text").rlike("^[0-9]+$"), 1).otherwise(0)) \
        .groupBy("constructor_id") \
        .agg(
            sum("is_finished").alias("finished_races"),
            count("is_finished").alias("total_entries")
        ) \
        .withColumn("reliability_score", (col("finished_races") / col("total_entries")) * 100) \
        .join(constructors_df, "constructor_id") \
        .select("constructor_id", "name", "reliability_score") \
        .orderBy(desc("reliability_score"))
        
    reliability_df.write.mode("overwrite").parquet(f"{PROCESSED_DIR}/constructor_reliability.parquet")
    print("Computed Reliability Score.")
    
    # 7. Points Efficiency
    efficiency_df = results_df.groupBy("driver_id") \
        .agg(sum("points").alias("total_points"), count("race_id").alias("races_entered")) \
        .withColumn("points_efficiency", col("total_points") / col("races_entered")) \
        .join(drivers_df, "driver_id") \
        .select("driver_id", "forename", "surname", "points_efficiency") \
        .orderBy(desc("points_efficiency"))
        
    efficiency_df.write.mode("overwrite").parquet(f"{PROCESSED_DIR}/points_efficiency.parquet")
    print("Computed Points Efficiency.")
    
    print("Spark Processing Complete!")
    spark.stop()

if __name__ == "__main__":
    main()
