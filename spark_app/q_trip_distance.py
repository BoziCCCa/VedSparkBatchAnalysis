"""Per-trip distance via haversine over consecutive GPS points.

Uses a Window ordered by ts_ms within each (veh_id, trip) to pull the
previous (lat, lon), computes the great-circle segment distance, sums
per trip, then rolls up by engine_type.
"""
from pyspark.sql import SparkSession, Window, functions as F

# HDFS paths
CLEAN_DYNAMIC = "hdfs://namenode:9000/ved/cleaned/dynamic"
CLEAN_STATIC  = "hdfs://namenode:9000/ved/cleaned/static"
RESULT        = "hdfs://namenode:9000/ved/results/trip_distance"

EARTH_RADIUS_KM = 6371.0


def haversine_km(lat1, lon1, lat2, lon2):
    """Great-circle distance in km between two decimal-degree points."""
    lat1r = F.radians(lat1)
    lat2r = F.radians(lat2)
    dlat = F.radians(lat2 - lat1)
    dlon = F.radians(lon2 - lon1)
    a = F.pow(F.sin(dlat / 2), 2) + F.cos(lat1r) * F.cos(lat2r) * F.pow(F.sin(dlon / 2), 2)
    return 2 * EARTH_RADIUS_KM * F.asin(F.sqrt(a))


def run(spark: SparkSession) -> None:
    dynamic = spark.read.parquet(CLEAN_DYNAMIC)
    static  = spark.read.parquet(CLEAN_STATIC)

    # Distance needs a valid pair of GPS points
    gps = dynamic.filter(F.col("lat").isNotNull() & F.col("lon").isNotNull())

    # Window: each trip, ordered by the in-trip millisecond counter
    trip_window = Window.partitionBy("veh_id", "trip").orderBy("ts_ms")
    with_prev = (gps
                 .withColumn("prev_lat", F.lag("lat").over(trip_window))
                 .withColumn("prev_lon", F.lag("lon").over(trip_window)))

    # Haversine between consecutive points; null for the first row of a trip
    with_segment = with_prev.withColumn(
        "segment_km",
        F.when(F.col("prev_lat").isNotNull(),
               haversine_km(F.col("prev_lat"), F.col("prev_lon"),
                            F.col("lat"),      F.col("lon"))))

    # Per-trip distance (sum of all segments)
    trip_dist = (with_segment
                 .groupBy("veh_id", "trip")
                 .agg(F.round(F.sum("segment_km"), 3).alias("trip_km")))

    # Attach engine_type and roll up fleet-level
    with_type = trip_dist.join(F.broadcast(static.select("veh_id", "engine_type")),
                               on="veh_id", how="left")

    summary = (with_type
               .filter(F.col("engine_type").isNotNull())
               .groupBy("engine_type")
               .agg(
                   F.count("*").alias("trip_count"),
                   F.round(F.avg("trip_km"), 2).alias("avg_trip_km"),
                   F.round(F.sum("trip_km"), 0).alias("total_km"),
                   F.percentile_approx("trip_km", [0.5, 0.9]).alias("pcts"),
               )
               .withColumn("median_trip_km", F.round(F.col("pcts")[0], 2))
               .withColumn("p90_trip_km",    F.round(F.col("pcts")[1], 2))
               .drop("pcts")
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
             .appName("ved-q-trip-distance")
             .config("spark.sql.session.timeZone", "UTC")
             .getOrCreate())
    try:
        run(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
