# Built on the same base image as the cluster, so the Spark, Java and Python
# versions in the application container always match the workers.
FROM bde2020/spark-base:3.3.0-hadoop3.3

ENV PYSPARK_PYTHON=python3 \
    PYSPARK_DRIVER_PYTHON=python3

WORKDIR /app
COPY . /app

# Everything after the image name in "docker compose run spark-app ..."
# replaces CMD and is passed to spark-submit (script path + its arguments).
ENTRYPOINT ["/spark/bin/spark-submit", \
            "--master", "spark://spark-master:7077", \
            "--deploy-mode", "client", \
            "--conf", "spark.hadoop.fs.defaultFS=hdfs://namenode:9000", \
            "--conf", "spark.eventLog.enabled=true", \
            "--conf", "spark.eventLog.dir=file:///tmp/spark-events"]
CMD ["/app/smoke_test.py"]
