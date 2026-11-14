import re

from pyspark.sql import SparkSession, DataFrame, functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType, DoubleType,
)

# HDFS paths
RAW_DYNAMIC = "hdfs://namenode:9000/ved/raw/dynamic/*.csv"
RAW_STATIC = "hdfs://namenode:9000/ved/raw/static/ved_static.csv"
CLEAN_DYNAMIC = "hdfs://namenode:9000/ved/cleaned/dynamic"
CLEAN_STATIC = "hdfs://namenode:9000/ved/cleaned/static"

# Dynamic columns: (raw CSV header, snake_case name, spark type)
COLUMNS = [
    ("DayNum",                           "day_num",         DoubleType()),
    ("VehId",                            "veh_id",          IntegerType()),
    ("Trip",                             "trip",            IntegerType()),
    ("Timestamp(ms)",                    "ts_ms",           LongType()),
    ("Latitude[deg]",                    "lat",             DoubleType()),
    ("Longitude[deg]",                   "lon",             DoubleType()),
    ("Vehicle Speed[km/h]",              "speed_kmh",       DoubleType()),
    ("MAF[g/sec]",                       "maf_g_s",         DoubleType()),
    ("Engine RPM[RPM]",                  "engine_rpm",      DoubleType()),
    ("Absolute Load[%]",                 "abs_load_pct",    DoubleType()),
    ("OAT[DegC]",                        "oat_c",           DoubleType()),
    ("Fuel Rate[L/hr]",                  "fuel_rate_l_h",   DoubleType()),
    ("Air Conditioning Power[kW]",       "ac_power_kw_raw", DoubleType()),
    ("Air Conditioning Power[Watts]",    "ac_power_w_raw",  DoubleType()),
    ("Heater Power[Watts]",              "heater_power_w",  DoubleType()),
    ("HV Battery Current[A]",            "hv_batt_i_a",     DoubleType()),
    ("HV Battery SOC[%]",                "hv_batt_soc_pct", DoubleType()),
    ("HV Battery Voltage[V]",            "hv_batt_v",       DoubleType()),
    ("Short Term Fuel Trim Bank 1[%]",   "fuel_trim_st_b1", DoubleType()),
    ("Short Term Fuel Trim Bank 2[%]",   "fuel_trim_st_b2", DoubleType()),
    ("Long Term Fuel Trim Bank 1[%]",    "fuel_trim_lt_b1", DoubleType()),
    ("Long Term Fuel Trim Bank 2[%]",    "fuel_trim_lt_b2", DoubleType()),
]

# Unix seconds for 2017-11-01 00:00:00 UTC (DayNum = 1.0 anchors here).
ANCHOR_EPOCH = 1509494400

# Sensor scrub ranges: values outside become null, the row survives.
SCRUB_RANGES = {
    "abs_load_pct":    (0,    200),
    "hv_batt_soc_pct": (0,    100),
    "oat_c":           (-50,   60),
    "engine_rpm":      (0,  10000),
    "speed_kmh":       (0,    250),
}

# Static header renames (both ICE/HEV "Vehicle Type" and PHEV/EV "EngineType")
STATIC_RENAMES = {
    "VehId":                                "veh_id",
    "EngineType":                           "engine_type",
    "Vehicle Type":                         "engine_type",
    "Vehicle Class":                        "vehicle_class",
    "Engine Configuration & Displacement":  "engine_config",
    "Transmission":                         "transmission",
    "Drive Wheels":                         "drive_wheels",
    "Generalized_Weight":                   "weight_lb",
}
STATIC_SENTINELS = ("NO DATA", "")

# Cap one Parquet file's row count so a busy month cannot blow up a single file.
MAX_ROWS_PER_FILE = 2_000_000


def scrub(df: DataFrame, column: str, low: float, high: float) -> DataFrame:
    return df.withColumn(column, F.when(F.col(column).between(low, high), F.col(column)))

def to_snake_case(name: str) -> str:
    s = re.sub(r"[^A-Za-z0-9]+", "_", name.strip())
    return s.strip("_").lower()


def build_dynamic_schema() -> StructType:
    fields = [StructField(raw, typ) for raw, _, typ in COLUMNS]
    fields.append(StructField("_corrupt_record", StringType()))
    return StructType(fields)


def read_dynamic(spark: SparkSession) -> DataFrame:
    return (spark.read
            .option("header", "true")
            .option("enforceSchema", "false")
            .option("nullValue", "NaN")
            .option("mode", "PERMISSIVE")
            .option("columnNameOfCorruptRecord", "_corrupt_record")
            .schema(build_dynamic_schema())
            .csv(RAW_DYNAMIC))


def rename_dynamic(df: DataFrame) -> DataFrame:
    """Alias raw CSV columns to the snake_case names from COLUMNS."""
    return df.select([F.col(raw).alias(new) for raw, new, _ in COLUMNS])


def load_static(spark: SparkSession) -> DataFrame:
    """Read the static CSV, validate the header, cast numerics safely."""
    raw = spark.read.option("header", "true").csv(RAW_STATIC)
    print(f"static raw columns: {raw.columns}")

    # Apply known renames, snake_case anything else
    df = raw
    for src in list(df.columns):
        target = STATIC_RENAMES.get(src, to_snake_case(src))
        if src != target:
            df = df.withColumnRenamed(src, target)

    # Treat sentinel markers as null in every column
    for c in df.columns:
        df = df.withColumn(c, F.when(~F.col(c).isin(*STATIC_SENTINELS), F.col(c)))

    # ANSI-safe numeric casts (invalid text becomes null, not an exception)
    df = df.withColumn("veh_id",    F.expr("try_cast(veh_id as int)"))
    df = df.withColumn("weight_lb", F.expr("try_cast(weight_lb as int)"))

    return df.dropDuplicates(["veh_id"])


def run(spark: SparkSession) -> None:
    # 1. Read raw dynamic files
    raw = read_dynamic(spark)

    # 2. Rename columns to be snake_Case
    renamed = rename_dynamic(raw).filter(F.col("day_num").between(1, 399))
    n_raw = renamed.count()

    # 3. Derive datetime columns. DayNum is the trip start, ts_ms the offset.
    trip_start_sec = F.lit(ANCHOR_EPOCH) + (F.col("day_num") - 1) * 86400
    event_sec = trip_start_sec + F.col("ts_ms") / 1000
    with_time = (renamed
                 .withColumn("trip_start_time", F.timestamp_seconds(trip_start_sec))
                 .withColumn("event_time",      F.timestamp_seconds(event_sec))
                 .withColumn("event_date",      F.to_date("event_time"))
                 .withColumn("event_month",     F.date_format("event_time", "yyyy-MM"))
                 .withColumn("hour_of_day",     F.hour("event_time")))

    # 4. Collapse A/C power into one column in watts
    with_ac = (with_time
               .withColumn("ac_power_w",
                           F.coalesce(F.col("ac_power_w_raw"),
                                      F.col("ac_power_kw_raw") * 1000))
               .drop("ac_power_kw_raw", "ac_power_w_raw"))

    # 5. Drop rows with missing keys
    key_filtered = (with_ac
                    .filter(F.col("veh_id").isNotNull())
                    .filter(F.col("trip").isNotNull())
                    .filter(F.col("ts_ms").isNotNull())
                    .filter(F.col("day_num").isNotNull()))
    n_after_keys = key_filtered.count()

    # 6. Dedupe on the natural key
    deduped = key_filtered.dropDuplicates(["veh_id", "trip", "ts_ms", "day_num"])
    n_after_dedupe = deduped.count()

    # 7. Scrub sensor values outside their physical ranges
    scrubbed = deduped
    for column, (low, high) in SCRUB_RANGES.items():
        scrubbed = scrub(scrubbed, column, low, high)

    # 8. Null out invalid GPS; keep the row and its other sensors
    valid_gps = (F.col("lat").between(-90, 90)
                 & F.col("lon").between(-180, 180)
                 & ~((F.col("lat") == 0) & (F.col("lon") == 0)))
    gps_clean = (scrubbed
                 .withColumn("lat", F.when(valid_gps, F.col("lat")))
                 .withColumn("lon", F.when(valid_gps, F.col("lon"))))

    # 9. Load static and count dynamic rows with no matching veh_id
    static = load_static(spark)
    unmatched = (gps_clean
                 .join(F.broadcast(static.select("veh_id")), "veh_id", "left_anti")
                 .count())

    print(f"raw rows                     : {n_raw:>12,}")
    print(f"after key filters            : {n_after_keys:>12,}")
    print(f"after dedupe                 : {n_after_dedupe:>12,}")
    print(f"no match in static (veh_id)  : {unmatched:>12,}")

    # 10. Write cleaned dynamic data, sorted within each monthly partition
    (gps_clean
     .repartition("event_month")
     .sortWithinPartitions("veh_id", "day_num", "trip", "ts_ms")
     .write
     .mode("overwrite")
     .partitionBy("event_month")
     .option("maxRecordsPerFile", MAX_ROWS_PER_FILE)
     .parquet(CLEAN_DYNAMIC))

    # 11. Write the cleaned static table too (no join at cleaning time)
    static.write.mode("overwrite").parquet(CLEAN_STATIC)

    print("Cleaning done.")
    print("  dynamic -> " + CLEAN_DYNAMIC)
    print("  static  -> " + CLEAN_STATIC)


def main() -> None:
    spark = (SparkSession.builder
             .appName("ved-clean")
             .config("spark.sql.session.timeZone", "UTC")
             .getOrCreate())
    try:
        run(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
