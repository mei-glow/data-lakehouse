import logging
from pyspark.sql import SparkSession

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def create_spark_session(app_name: str) -> SparkSession:
    """
    Create Spark session (Iceberg REST catalog + MinIO)
    chạy trong container spark-master (đã có Java)
    """
    logger.info("🔧 Creating Spark session: %s", app_name)

    spark = (
        SparkSession.builder
        .appName(app_name)
        .config(
            "spark.jars.packages",
            ",".join([
                "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0",
                "org.apache.hadoop:hadoop-aws:3.3.4",
                "software.amazon.awssdk:bundle:2.20.18",
                "software.amazon.awssdk:url-connection-client:2.20.18",
            ])
        )
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.iceberg.type", "rest")
        .config("spark.sql.catalog.iceberg.uri", "http://rest:8181")
        .config("spark.sql.catalog.iceberg.warehouse", "s3a://warehouse/")
        .config("spark.sql.catalog.iceberg.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")

        .config("spark.hadoop.fs.s3a.access.key", "admin")
        .config("spark.hadoop.fs.s3a.secret.key", "password")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")

        # Optionally make Iceberg the default catalog
        .config("spark.sql.defaultCatalog", "iceberg")
        .getOrCreate()
    )

    logger.info("✅ Spark created | version=%s", spark.version)
    return spark
