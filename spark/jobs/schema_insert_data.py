import logging
from pyspark.sql.functions import current_timestamp, lit, to_date
from schema_common import create_spark_session

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

TABLE = "iceberg.bronze.ecommerce_events"
SOURCE_FILE = "schema_evolution_demo.csv"

def main():
    spark = None
    try:
        spark = create_spark_session("Schema Evolution - Insert New Data")

        logger.info("=" * 80)
        logger.info(" INSERTING NEW DATA WITH payment_method")
        logger.info("=" * 80)

        sample_data = [
            ("2024-12-01 10:00:00", "purchase", 1234567, 5678, "electronics", "SAMSUNG", 599.99, 9001, "session_new_001", "credit_card"),
            ("2024-12-01 10:05:00", "purchase", 2345678, 6789, "fashion.shoes", "NIKE", 129.99, 9002, "session_new_002", "paypal"),
            ("2024-12-01 10:10:00", "purchase", 3456789, 7890, "home.furniture", "IKEA", 299.50, 9003, "session_new_003", "debit_card"),
            ("2024-12-01 10:15:00", "view",     4567890, 8901, "electronics.smartphone", "APPLE", 999.99, 9004, "session_new_004", None),
            ("2024-12-01 10:20:00", "cart",     5678901, 9012, "fashion.clothing", "ZARA",   79.99, 9005, "session_new_005", None),
        ]

        df = spark.createDataFrame(sample_data, [
            "event_time", "event_type", "product_id", "category_id",
            "category_code", "brand", "price", "user_id", "user_session", "payment_method"
        ])

        df_enriched = (
            df.withColumn("_ingestion_time", current_timestamp())
              .withColumn("_source_file", lit(SOURCE_FILE))
              .withColumn("_processing_date", to_date(current_timestamp()))
        )

        logger.info(" Sample data to insert:")
        df_enriched.select(
            "event_time", "event_type", "product_id", "price",
            "payment_method", "_processing_date", "_source_file"
        ).show(truncate=False)

        logger.info(" Writing to Iceberg...")
        df_enriched.writeTo(TABLE).append()

        logger.info(" Insert completed!")

        logger.info(" Verify inserted rows:")
        spark.sql(f"""
            SELECT event_time, event_type, brand, price, payment_method, _source_file
            FROM {TABLE}
            WHERE _source_file = '{SOURCE_FILE}'
            ORDER BY event_time
        """).show(truncate=False)

    finally:
        if spark:
            spark.stop()

if __name__ == "__main__":
    main()
