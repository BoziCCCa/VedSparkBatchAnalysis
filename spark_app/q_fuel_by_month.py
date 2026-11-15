"""Average fuel burn per month, for ICE and HEV vehicles.

Reads the cleaned dynamic + static Parquet, filters to the engine types
that actually log fuel_rate, and writes avg fuel_rate per
(engine_type, event_month).
"""
from pyspark.sql import SparkSession, functions as F

# HDFS paths
CLEAN_DYNAMIC = "hdfs://namenode:9000/ved/cleaned/dynamic"
CLEAN_STATIC  = "hdfs://namenode:9000/ved/cleaned/static"
RESULT        = "hdfs://namenode:9000/ved/results/fuel_by_month"

# Fuel rate is only logged for combustion-powered vehicles.
ENGINE_TYPES = ("ICE", "HEV")


def run(spark: SparkSession) -> None:
    dynamic = spark.read.parquet(CLEAN_DYNAMIC)
    static  = spark.read.parquet(CLEAN_STATIC)

    # Attach engine_type (static is tiny; broadcast avoids a shuffle)
    joined = dynamic.join(F.broadcast(static.select("veh_id", "engine_type")),
                          on="veh_id", how="left")

    # Keep only the engine types that log fuel, and rows where it exists
    usable = joined.filter(F.col("engine_type").isin(*ENGINE_TYPES)
                           & F.col("fuel_rate_l_h").isNotNull())

    # Aggregate per (engine_type, event_month)
    summary = (usable
               .groupBy("engine_type", "event_month")
               .agg(F.count("*").alias("n_readings"),
                    F.round(F.avg("fuel_rate_l_h"), 3).alias("avg_fuel_rate_l_h"))
               .orderBy("engine_type", "event_month"))

    print("=" * 72)
    summary.show(100, truncate=False)
    print("=" * 72)

    (summary.coalesce(1)
     .write.mode("overwrite")
     .parquet(RESULT))

    print("Result written to " + RESULT)


def main() -> None:
    spark = (SparkSession.builder
             .appName("ved-q-fuel-by-month")
             .config("spark.sql.session.timeZone", "UTC")
             .getOrCreate())
    try:
        run(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
