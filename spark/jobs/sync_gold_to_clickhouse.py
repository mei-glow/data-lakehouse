#!/usr/bin/env python3
"""
Sync Gold tables from Iceberg to ClickHouse
"""

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
import logging
import subprocess

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ClickHouse JDBC config
CLICKHOUSE_HOST = "clickhouse"
CLICKHOUSE_PORT = "8123"
CLICKHOUSE_DB = "lakehouse"
CLICKHOUSE_USER = "default"
CLICKHOUSE_PASSWORD = "clickhouse123"

CLICKHOUSE_URL = f"jdbc:clickhouse://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/{CLICKHOUSE_DB}"

# Table mapping: Iceberg → ClickHouse
TABLES_TO_SYNC = [
    "gold_daily_sales_summary",
    "gold_product_performance",
    "gold_category_performance",
    "gold_user_rfm_segments",
    "gold_conversion_funnel_daily",
    "gold_user_journey_funnel",
    "gold_hourly_traffic",
]

def create_spark_session():
    """Create Spark session"""
    logger.info("Creating Spark session...")
    
    spark = SparkSession.builder \
        .appName("Gold to ClickHouse Sync") \
        .config("spark.jars.packages",
                "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,"
                "org.apache.hadoop:hadoop-aws:3.3.4,"
                "software.amazon.awssdk:bundle:2.20.18,"
                "com.clickhouse:clickhouse-jdbc:0.4.6") \
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.iceberg.type", "rest") \
        .config("spark.sql.catalog.iceberg.uri", "http://iceberg-rest:8181") \
        .config("spark.sql.catalog.iceberg.warehouse", "s3a://warehouse/") \
        .config("spark.sql.catalog.iceberg.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
        .config("spark.hadoop.fs.s3a.access.key", "admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "password") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.defaultCatalog", "iceberg") \
        .getOrCreate()
    
    logger.info(" Spark session created")
    return spark


def sync_table(spark, table_name):
    """Sync one table from Iceberg to ClickHouse"""
    
    iceberg_table = f"iceberg.silver_gold.{table_name}"
    clickhouse_table = table_name.replace("gold_", "")

    
    logger.info(f"\n{'='*60}")
    logger.info(f"Syncing: {iceberg_table} → {clickhouse_table}")
    logger.info(f"{'='*60}")
    
    try:
        # Read from Iceberg
        logger.info(f"Reading from {iceberg_table}...")
        df = spark.table(iceberg_table)
        
        row_count = df.count()
        logger.info(f" Read {row_count:,} rows")
        
        if row_count == 0:
            logger.warning(f"  No data in {iceberg_table}, skipping...")
            return
        
        # Write to ClickHouse (overwrite mode)
        logger.info(f"Writing to ClickHouse table '{clickhouse_table}'...")
        
        df.write \
            .format("jdbc") \
            .option("url", CLICKHOUSE_URL) \
            .option("dbtable", table_name.replace('gold_', '')) \
            .option("user", CLICKHOUSE_USER) \
            .option("password", CLICKHOUSE_PASSWORD) \
            .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
            .option("createTableOptions", "ENGINE = MergeTree() ORDER BY tuple()") \
            .mode("append") \
            .save()
        
        logger.info(f" Synced {row_count:,} rows to ClickHouse")
        
    except Exception as e:
        logger.error(f" Failed to sync {table_name}: {e}")
        raise

def main():
    """Main sync process"""
    
    logger.info("\n" + "="*80)
    logger.info(" GOLD TO CLICKHOUSE SYNC JOB")
    logger.info("="*80)
    
    spark = None
    
    try:
        spark = create_spark_session()
        
        total_synced = 0
        
        for table in TABLES_TO_SYNC:
            sync_table(spark, table)
            total_synced += 1
        
        logger.info("\n" + "="*80)
        logger.info(f" SYNC COMPLETE: {total_synced}/{len(TABLES_TO_SYNC)} tables")
        logger.info("="*80)
        
        return 0
        
    except Exception as e:
        logger.error(f"\n SYNC FAILED: {e}", exc_info=True)
        return 1
        
    finally:
        if spark:
            spark.stop()
            logger.info(" Spark session stopped")

if __name__ == "__main__":
    sys.exit(main())