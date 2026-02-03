#!/usr/bin/env python3
"""
Bronze Layer Ingestion Job
- Tạo Bronze database & table
- Ingest CSV từ MinIO → Iceberg
- Verify data quality
"""

import argparse
import json
import logging
import sys
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit, to_date, col

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# =========================
# CREATE SPARK SESSION
# =========================
def create_spark_session():
    """Tạo Spark session với Iceberg + MinIO configs"""
    
    logger.info("🔧 Creating Spark session...")
    
    spark = SparkSession.builder \
        .appName("Bronze Ingestion - eCommerce") \
        .config("spark.jars.packages",
                "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,"
                "org.apache.hadoop:hadoop-aws:3.3.4,"
                "software.amazon.awssdk:bundle:2.20.18,"
                "software.amazon.awssdk:url-connection-client:2.20.18") \
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.iceberg", 
                "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.iceberg.type", "rest") \
        .config("spark.sql.catalog.iceberg.uri", "http://rest:8181") \
        .config("spark.sql.catalog.iceberg.warehouse", "s3a://warehouse/") \
        .config("spark.sql.catalog.iceberg.io-impl", 
                "org.apache.iceberg.aws.s3.S3FileIO") \
        .config("spark.sql.catalog.iceberg.s3.endpoint", "http://minio:9000") \
        .config("spark.sql.catalog.iceberg.s3.path-style-access", "true") \
        .config("spark.hadoop.fs.s3a.access.key", "admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "password") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", 
                "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .config("spark.executorEnv.AWS_REGION", "us-east-1") \
        .config("spark.executorEnv.AWS_ACCESS_KEY_ID", "admin") \
        .config("spark.executorEnv.AWS_SECRET_ACCESS_KEY", "password") \
        .getOrCreate()
    
    # Set Hadoop configs for all tasks
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", "admin")
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "password")
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "http://minio:9000")
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false")
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider", 
                                                       "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    
    logger.info("✅ Spark session created")
    logger.info(f"   Spark version: {spark.version}")
    logger.info(f"   App Name: {spark.sparkContext.appName}")
    
    return spark

# =========================
# CREATE DATABASE & TABLE
# =========================
def create_bronze_table(spark):
    """Tạo Bronze database và table nếu chưa có"""
    
    logger.info("🏗️  Creating Bronze database and table...")
    
    # Create namespace
    spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.bronze")
    logger.info("✅ Database 'bronze' created/verified")
    
    # Create table
    create_table_ddl = """
    CREATE TABLE IF NOT EXISTS iceberg.bronze.ecommerce_events (
        -- Original columns
        event_time STRING COMMENT 'Event timestamp in UTC',
        event_type STRING COMMENT 'view, cart, purchase',
        product_id BIGINT,
        category_id BIGINT,
        category_code STRING,
        brand STRING,
        price DOUBLE,
        user_id BIGINT,
        user_session STRING,
        
        -- Metadata columns
        _ingestion_time TIMESTAMP COMMENT 'Ingestion timestamp',
        _source_file STRING COMMENT 'Source CSV filename',
        _processing_date DATE COMMENT 'Partition key'
    )
    USING iceberg
    PARTITIONED BY (days(_processing_date))
    TBLPROPERTIES (
        'write.format.default' = 'parquet',
        'write.parquet.compression-codec' = 'snappy',
        'format-version' = '2'
    )
    """
    
    spark.sql(create_table_ddl)
    logger.info("✅ Table 'ecommerce_events' created/verified")
    
    # Show schema
    logger.info("📋 Table Schema:")
    spark.sql("DESCRIBE iceberg.bronze.ecommerce_events").show(truncate=False)


# =========================
# INGEST CSV FILES
# =========================
def ingest_csv_files(spark, file_list):
    """
    Ingest CSV files vào Iceberg Bronze table
    
    Args:
        spark: SparkSession
        file_list: List of dicts with 'file_name' and 's3a_path'
    """
    
    logger.info(f"📦 Processing {len(file_list)} CSV files...")
    
    total_records = 0
    
    for idx, file_info in enumerate(file_list, 1):
        file_name = file_info['file_name']
        s3a_path = file_info['s3a_path']
        
        logger.info(f"\n{'='*60}")
        logger.info(f"📄 File {idx}/{len(file_list)}: {file_name}")
        logger.info(f"{'='*60}")
        
        try:
            # Read CSV
            logger.info(f"📖 Reading CSV from: {s3a_path}")
            df = spark.read \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .csv(s3a_path)
            
            record_count = df.count()
            logger.info(f"✅ Read {record_count:,} records")
            
            # Add metadata columns
            logger.info("➕ Adding metadata columns...")
            df_enriched = df \
                .withColumn("_ingestion_time", current_timestamp()) \
                .withColumn("_source_file", lit(file_name)) \
                .withColumn("_processing_date", to_date(current_timestamp()))
            
            # Sample preview
            logger.info("🔍 Sample data (first 3 rows):")
            df_enriched.select(
                "event_time", "event_type", "product_id", "price",
                "_source_file", "_processing_date"
            ).show(3, truncate=False)
            
            # Write to Iceberg
            logger.info("💾 Writing to Iceberg...")
            df_enriched.writeTo("iceberg.bronze.ecommerce_events") \
                .option("write-format", "parquet") \
                .append()
            
            logger.info(f"✅ Ingested {record_count:,} records from {file_name}")
            total_records += record_count
            
        except Exception as e:
            logger.error(f"❌ Failed to process {file_name}: {e}")
            raise
    
    logger.info(f"\n🎉 Total records ingested: {total_records:,}")
    return total_records


# =========================
# VERIFY INGESTION
# =========================
def verify_ingestion(spark):
    """Verify dữ liệu sau khi ingest"""
    
    logger.info("\n" + "="*60)
    logger.info("🔍 VERIFICATION CHECKS")
    logger.info("="*60)
    
    # Check 1: Total records
    logger.info("\n📊 CHECK 1: Total records")
    total = spark.sql("SELECT COUNT(*) as cnt FROM iceberg.bronze.ecommerce_events").collect()[0].cnt
    logger.info(f"   Total: {total:,} records")
    
    # Check 2: Partitions
    logger.info("\n📂 CHECK 2: Partitions")
    spark.sql("""
        SELECT _processing_date, COUNT(*) as count
        FROM iceberg.bronze.ecommerce_events
        GROUP BY _processing_date
        ORDER BY _processing_date
    """).show()
    
    # Check 3: Source files
    logger.info("\n📄 CHECK 3: Source files")
    spark.sql("""
        SELECT _source_file, COUNT(*) as count
        FROM iceberg.bronze.ecommerce_events
        GROUP BY _source_file
        ORDER BY _source_file
    """).show()
    
    # Check 4: Event types
    logger.info("\n📈 CHECK 4: Event type distribution")
    spark.sql("""
        SELECT 
            event_type,
            COUNT(*) as count,
            ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as pct
        FROM iceberg.bronze.ecommerce_events
        GROUP BY event_type
        ORDER BY count DESC
    """).show()
    
    # Check 5: Sample data
    logger.info("\n🔬 CHECK 5: Sample records")
    spark.sql("""
        SELECT 
            event_time, event_type, brand, price,
            _source_file, _processing_date
        FROM iceberg.bronze.ecommerce_events
        LIMIT 5
    """).show(truncate=False)
    
    logger.info("\n✅ Verification complete!")
    
    return {
        "total_records": total,
        "status": "success"
    }


# =========================
# MAIN FUNCTION
# =========================
def main():
    """Main entry point"""
    
    # Parse arguments
    parser = argparse.ArgumentParser(description='Bronze Layer Ingestion Job')
    parser.add_argument('--file-list', type=str, required=True,
                       help='Path to JSON file containing list of files to ingest')
    parser.add_argument('--mode', type=str, default='full',
                       choices=['create', 'ingest', 'verify', 'full'],
                       help='Execution mode')
    
    args = parser.parse_args()
    
    # Read file list from JSON file
    try:
        logger.info(f"📄 Reading file list from: {args.file_list}")
        with open(args.file_list, 'r') as f:
            file_list = json.load(f)
        logger.info(f"✅ Loaded {len(file_list)} files")
    except Exception as e:
        logger.error(f"❌ Failed to read file list: {e}")
        return 1
    
    logger.info("\n" + "="*80)
    logger.info("🚀 BRONZE LAYER INGESTION JOB STARTED")
    logger.info("="*80)
    logger.info(f"Mode: {args.mode}")
    logger.info(f"Files to process: {len(file_list)}")
    
    spark = None
    
    try:
        # Create Spark session
        spark = create_spark_session()
        
        # Execute based on mode
        if args.mode in ['create', 'full']:
            create_bronze_table(spark)
        
        if args.mode in ['ingest', 'full']:
            total_records = ingest_csv_files(spark, file_list)
        
        if args.mode in ['verify', 'full']:
            result = verify_ingestion(spark)
        
        logger.info("\n" + "="*80)
        logger.info("✅ JOB COMPLETED SUCCESSFULLY")
        logger.info("="*80)
        
        return 0
        
    except Exception as e:
        logger.error(f"\n❌ JOB FAILED: {e}", exc_info=True)
        return 1
        
    finally:
        if spark:
            spark.stop()
            logger.info("🛑 Spark session stopped")


if __name__ == "__main__":
    sys.exit(main())