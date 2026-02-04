import logging
from schema_common import create_spark_session

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

TABLE = "iceberg.bronze.ecommerce_events"

def main():
    spark = None
    try:
        spark = create_spark_session("Schema Evolution - Check Current")

        logger.info("=" * 80)
        logger.info(" CHECKING CURRENT BRONZE TABLE SCHEMA")
        logger.info("=" * 80)

        logger.info(" Current schema:")
        spark.sql(f"DESCRIBE {TABLE}").show(truncate=False)

        # Count columns (DESCRIBE trả cả header/info; mình vẫn giữ như code cũ)
        col_count = spark.sql(f"DESCRIBE {TABLE}").count()
        logger.info(" Total DESCRIBE rows (approx columns): %s", col_count)

        logger.info(" Sample data (before schema change):")
        spark.sql(f"""
            SELECT event_time, event_type, product_id, price, user_id
            FROM {TABLE}
            LIMIT 3
        """).show(truncate=False)

        logger.info(" Current Iceberg snapshots:")
        spark.sql(f"SELECT * FROM {TABLE}.snapshots").show(truncate=False)

        logger.info(" CHECK DONE")
    finally:
        if spark:
            spark.stop()

if __name__ == "__main__":
    main()
