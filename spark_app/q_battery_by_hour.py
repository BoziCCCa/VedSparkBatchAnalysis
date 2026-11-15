"""HV battery SOC distribution by engine_type and hour_of_day.

For the engine types that actually log battery (PHEV, EV), compute avg
and quartiles of hv_batt_soc_pct per hour. Makes the "structural null"
pattern of the dataset concrete.
"""
from pyspark.sql import SparkSession, functions as F

# HDFS paths
CLEAN_DYNAMIC = "hdfs://namenode:9000/ved/cleaned/dynamic"
CLEAN_STATIC  = "hdfs://namenode:9000/ved/cleaned/static"
RESULT        = "hdfs://namenode:9000/ved/results/battery_by_hour"

# HV battery telemetry is only present on these engine types.
ENGINE_TYPES = ("PHEV", "EV")
PERCENTILES  = [0.25, 0.5, 0.75]


def run(spark: SparkSession) -> None:
    dynamic = spark.read.parquet(CLEAN_DYNAMIC)
    static  = spark.read.parquet(CLEAN_STATIC)

    # Attach engine_type (tiny table -> broadcast)
    joined = dynamic.join(F.broadcast(static.select("veh_id", "engine_type")),
                          on="veh_id", how="left")

    # Keep only engine types with battery data and rows where SOC is present
    usable = joined.filter(F.col("engine_type").isin(*ENGINE_TYPES)
                           & F.col("hv_batt_soc_pct").isNotNull())

    # Aggregate per (engine_type, hour_of_day)
    summary = (usable
               .groupBy("engine_type", "hour_of_day")
               .agg(F.count("*").alias("n_readings"),
                    F.round(F.avg("hv_batt_soc_pct"), 2).alias("avg_soc"),
                    F.percentile_approx("hv_batt_soc_pct", PERCENTILES).alias("pcts"))
               .withColumn("p25", F.round(F.col("pcts")[0], 2))
               .withColumn("p50", F.round(F.col("pcts")[1], 2))
               .withColumn("p75", F.round(F.col("pcts")[2], 2))
               .drop("pcts")
               .orderBy("engine_type", "hour_of_day"))

    print("=" * 72)
    summary.show(100, truncate=False)
    print("=" * 72)

    (summary.coalesce(1)
     .write.mode("overwrite")
     .parquet(RESULT))

    print("Result written to " + RESULT)


def main() -> None:
    spark = (SparkSession.builder
             .appName("ved-q-battery-by-hour")
             .config("spark.sql.session.timeZone", "UTC")
             .getOrCreate())
    try:
        run(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
