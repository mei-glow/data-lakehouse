#!/bin/bash
set -e

MASTER_URL="spark://spark-master:7077"

case "$SPARK_MODE" in
  master)
    echo "===> Starting Spark Master"
    start-master.sh -p 7077
    start-history-server.sh
    tail -f /dev/null
    ;;
  worker)
    echo "===> Starting Spark Worker"
    start-worker.sh "$MASTER_URL" \
      --cores "${SPARK_WORKER_CORES:-2}" \
      --memory "${SPARK_WORKER_MEMORY:-2g}" \
      --webui-port 8081
    tail -f /dev/null
    ;;
  thrift)
    echo "===> Init Iceberg default namespace"
    spark-sql \
      --master "$MASTER_URL" \
      -e "CREATE NAMESPACE IF NOT EXISTS iceberg.default"

    echo "===> Starting Spark Thrift Server with RESOURCE LIMITS"

    # THÊM CÁC CONF ĐỂ GIỚI HẠN RESOURCES
    exec /opt/spark/bin/spark-submit \
      --master "$MASTER_URL" \
      --class org.apache.spark.sql.hive.thriftserver.HiveThriftServer2 \
      --name "Spark Thrift Server" \
      --driver-memory 1g \
      --executor-memory 1g \
      --executor-cores 1 \
      --total-executor-cores 1 \
      --conf spark.cores.max=1 \
      --conf spark.sql.hive.thriftServer.port=10000 \
      --conf spark.sql.hive.thriftServer.singleSession=true \
      --conf spark.sql.hive.metastore.jars=builtin \
      --conf spark.sql.hive.metastore.sharedPrefixes= \
      --conf spark.sql.catalogImplementation=in-memory \
      --conf spark.dynamicAllocation.enabled=false \
      --conf spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog \
      --conf spark.sql.catalog.iceberg.type=rest \
      --conf spark.sql.catalog.iceberg.uri=http://iceberg-rest:8181 \
      --conf spark.sql.catalog.iceberg.warehouse=s3a://warehouse/ \
      --conf spark.sql.catalog.iceberg.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
      --conf spark.sql.catalog.iceberg.s3.endpoint=http://minio:9000 \
      --conf spark.sql.catalog.iceberg.s3.path-style-access=true \
      --conf spark.sql.catalog.iceberg.s3.region=us-east-1 \
      --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
      --conf spark.sql.defaultCatalog=iceberg \
      spark-internal
    ;;

  *)
    exec "$@"
    ;;
esac