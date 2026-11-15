"""Monthly fleet size per engine_type.

How many distinct vehicles and trips are active each month, split by
engine type? Also reports the raw reading volume so it's clear whether
a busy month came from more vehicles or more driving per vehicle.
"""
from pyspark.sql import SparkSession, functions as F

# HDFS paths
CLEAN_DYNAMIC = "hdfs://namenode:9000/ved/cleaned/dynamic"
CLEAN_STATIC  = "hdfs://namenode:9000/ved/cleaned/static"
RESULT        = "hdfs://namenode:9000/ved/results/fleet_by_month"


def run(spark: SparkSession) -> None:
    dynamic = spark.read.parquet(CLEAN_DYNAMIC)
    static  = spark.read.parquet(CLEAN_STATIC)

    # Attach engine_type via broadcast (static is tiny)
    joined = dynamic.join(F.broadcast(static.select("veh_id", "engine_type")),
                          on="veh_id", how="left")

    # One row per (event_month, engine_type) with the fleet metrics
    summary = (joined
               .filter(F.col("engine_type").isNotNull())
               .groupBy("event_month", "engine_type")
               .agg(
                   F.countDistinct("veh_id").alias("active_vehicles"),
                   F.countDistinct("veh_id", "trip").alias("active_trips"),
                   F.count("*").alias("n_readings"),
               )
               .orderBy("event_month", "engine_type"))

    print("=" * 72)
    summary.show(100, truncate=False)
    print("=" * 72)

    (summary.coalesce(1)
     .write.mode("overwrite")
     .parquet(RESULT))

    print("Result written to " + RESULT)


def main() -> None:
    spark = (SparkSession.builder
             .appName("ved-q-fleet-by-month")
             .config("spark.sql.session.timeZone", "UTC")
             .getOrCreate())
    try:
        run(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
