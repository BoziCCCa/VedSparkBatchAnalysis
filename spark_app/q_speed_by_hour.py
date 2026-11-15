"""Speed (avg + percentiles) per engine_type and hour_of_day.

Reads the cleaned dynamic + static Parquet, attaches engine_type onto
each reading, groups by (engine_type, hour_of_day), and writes one small
Parquet file with the summary.
"""
from pyspark.sql import SparkSession, functions as F

# HDFS paths
CLEAN_DYNAMIC = "hdfs://namenode:9000/ved/cleaned/dynamic"
CLEAN_STATIC  = "hdfs://namenode:9000/ved/cleaned/static"
RESULT        = "hdfs://namenode:9000/ved/results/speed_by_hour"

PERCENTILES = [0.25, 0.5, 0.75, 0.90]


def run(spark: SparkSession) -> None:
    # Load the two cleaned tables
    dynamic = spark.read.parquet(CLEAN_DYNAMIC)
    static  = spark.read.parquet(CLEAN_STATIC)

    # Attach engine_type (static is tiny; broadcast avoids a shuffle)
    joined = dynamic.join(F.broadcast(static.select("veh_id", "engine_type")),
                          on="veh_id", how="left")

    # Keep rows where both the group key and the measure are usable
    usable = joined.filter(F.col("speed_kmh").isNotNull()
                           & F.col("engine_type").isNotNull())

    # Aggregate: count + avg + percentiles per group
    summary = (usable
               .groupBy("engine_type", "hour_of_day")
               .agg(F.count("*").alias("n_readings"),
                    F.round(F.avg("speed_kmh"), 2).alias("avg_speed_kmh"),
                    F.percentile_approx("speed_kmh", PERCENTILES).alias("pcts"))
               .withColumn("p25", F.round(F.col("pcts")[0], 2))
               .withColumn("p50", F.round(F.col("pcts")[1], 2))
               .withColumn("p75", F.round(F.col("pcts")[2], 2))
               .withColumn("p90", F.round(F.col("pcts")[3], 2))
               .drop("pcts")
               .orderBy("engine_type", "hour_of_day"))

    summary.show(100, truncate=False)

    # Write one small Parquet file
    (summary.coalesce(1)
     .write.mode("overwrite")
     .parquet(RESULT))

    print("Result written to " + RESULT)


def main() -> None:
    spark = (SparkSession.builder
             .appName("ved-q-speed-by-hour")
             .config("spark.sql.session.timeZone", "UTC")
             .getOrCreate())
    try:
        run(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
