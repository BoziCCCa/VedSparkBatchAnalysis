"""Checks that the app container, the Spark cluster and HDFS can talk to each other.

Writes a small Parquet file to HDFS, reads it back, and prints what it found.
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

HDFS_PATH = "hdfs://namenode:9000/tmp/smoke_test"

spark = SparkSession.builder.appName("smoke-test").getOrCreate()
sc = spark.sparkContext

df = spark.range(0, 1000000, numPartitions=8).withColumn("bucket", F.col("id") % 10)
df.write.mode("overwrite").parquet(HDFS_PATH)

back = spark.read.parquet(HDFS_PATH)
rows = back.count()
buckets = back.groupBy("bucket").count().count()

# Hosts that actually ran tasks (should be the workers, not the app container).
hosts = sorted(set(
    sc.parallelize(range(100), 8)
      .map(lambda _: __import__("socket").gethostname())
      .collect()
))

print("=" * 60)
print("Spark version : " + spark.version)
print("Master        : " + sc.master)
print("Rows read back: {} (expected 1000000)".format(rows))
print("Buckets       : {} (expected 10)".format(buckets))
print("Task hosts    : " + ", ".join(hosts))
print("SMOKE TEST " + ("PASSED" if rows == 1000000 and buckets == 10 else "FAILED"))
print("=" * 60)

spark.stop()
