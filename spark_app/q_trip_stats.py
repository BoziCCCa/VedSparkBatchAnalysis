"""Per-trip aggregates rolled up by engine_type.

Two-stage aggregation: first collapse readings to one row per
(veh_id, trip), then summarise those trips per engine_type. Writes the
fleet-level rollup to HDFS.
"""
from pyspark.sql import SparkSession, functions as F

# HDFS paths
CLEAN_DYNAMIC = "hdfs://namenode:9000/ved/cleaned/dynamic"
CLEAN_STATIC  = "hdfs://namenode:9000/ved/cleaned/static"
RESULT        = "hdfs://namenode:9000/ved/results/trip_stats"


def run(spark: SparkSession) -> None:
    dynamic = spark.read.parquet(CLEAN_DYNAMIC)
    static  = spark.read.parquet(CLEAN_STATIC)

    # Attach engine_type once; the two-stage aggregation reuses it
    joined = dynamic.join(F.broadcast(static.select("veh_id", "engine_type")),
                          on="veh_id", how="left")

    # Stage 1: one row per (veh_id, trip) with its vitals
    trip_level = (joined
                  .filter(F.col("engine_type").isNotNull())
                  .groupBy("veh_id", "trip", "engine_type")
                  .agg(
                      (F.max("event_time").cast("long")
                       - F.min("event_time").cast("long")).alias("duration_sec"),
                      F.max("speed_kmh").alias("max_speed_kmh"),
                      F.avg("speed_kmh").alias("avg_speed_kmh"),
                      F.count("*").alias("n_readings"),
                  ))

    # Stage 2: fleet rollup per engine_type
    summary = (trip_level
               .groupBy("engine_type")
               .agg(
                   F.count("*").alias("trip_count"),
                   F.round(F.avg("duration_sec") / 60, 2).alias("avg_duration_min"),
                   F.round(F.avg("max_speed_kmh"), 2).alias("avg_max_speed"),
                   F.round(F.avg("avg_speed_kmh"), 2).alias("avg_mean_speed"),
                   F.round(F.avg("n_readings"), 1).alias("avg_readings_per_trip"),
                   F.percentile_approx("duration_sec", [0.5, 0.9]).alias("dur_pcts"),
               )
               .withColumn("median_duration_min", F.round(F.col("dur_pcts")[0] / 60, 2))
               .withColumn("p90_duration_min",    F.round(F.col("dur_pcts")[1] / 60, 2))
               .drop("dur_pcts")
               .orderBy("engine_type"))

    print("=" * 72)
    summary.show(10, truncate=False)
    print("=" * 72)

    (summary.coalesce(1)
     .write.mode("overwrite")
     .parquet(RESULT))

    print("Result written to " + RESULT)


def main() -> None:
    spark = (SparkSession.builder
             .appName("ved-q-trip-stats")
             .config("spark.sql.session.timeZone", "UTC")
             .getOrCreate())
    try:
        run(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
