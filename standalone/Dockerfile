# Same base image as the cluster app so Spark, Java and Python versions
# always match. The only difference from spark_app/Dockerfile is --master:
# this container runs Spark in local mode (all driver + executor threads
# inside this single JVM, no cluster involved). HDFS is still the shared
# storage layer, so --master is the only axis of comparison.
FROM bde2020/spark-base:3.3.0-hadoop3.3

ENV PYSPARK_PYTHON=python3 \
    PYSPARK_DRIVER_PYTHON=python3

WORKDIR /app

# Everything after the image name in "docker compose run spark-standalone ..."
# replaces CMD and is passed to spark-submit (script path + its arguments).
ENTRYPOINT ["/spark/bin/spark-submit", \
            "--master", "local[*]", \
            "--driver-memory", "3g", \
            "--conf", "spark.hadoop.fs.defaultFS=hdfs://namenode:9000", \
            "--conf", "spark.eventLog.enabled=true", \
            "--conf", "spark.eventLog.dir=file:///tmp/spark-events"]
