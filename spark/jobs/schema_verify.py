import logging
from schema_common import create_spark_session

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

TABLE = "iceberg.bronze.ecommerce_events"

def main():
    spark = None
    try:
        spark = create_spark_session("Schema Evolution - Lightweight Verify")

        logger.info("=" * 80)
        logger.info(" LIGHTWEIGHT SCHEMA EVOLUTION VERIFICATION")
        logger.info("=" * 80)

        # 1️⃣ Check schema (metadata only, rất nhẹ)
        logger.info(" Checking table schema...")
        spark.sql(f"DESCRIBE TABLE {TABLE}").show(truncate=False)

        # 2️⃣ Sample vài dòng có payment_method (dữ liệu mới)
        logger.info(" Sample rows WITH payment_method (new schema data)")
        spark.sql(f"""
            SELECT event_time, event_type, price, payment_method, _source_file
            FROM {TABLE}
            WHERE payment_method IS NOT NULL
            LIMIT 10
        """).show(truncate=False)

        # 3️⃣ Sample vài dòng không có payment_method (dữ liệu cũ)
        logger.info(" Sample rows WITHOUT payment_method (old schema data)")
        spark.sql(f"""
            SELECT event_time, event_type, price, payment_method, _source_file
            FROM {TABLE}
            WHERE payment_method IS NULL
            LIMIT 10
        """).show(truncate=False)

        # 4️⃣ Check snapshot history (metadata, rất nhẹ)
        logger.info(" Iceberg Snapshot History (latest 5)")
        spark.sql(f"""
            SELECT committed_at, snapshot_id, operation
            FROM {TABLE}.snapshots
            ORDER BY committed_at DESC
            LIMIT 5
        """).show(truncate=False)

        logger.info(" LIGHTWEIGHT VERIFICATION COMPLETE!")

    finally:
        if spark:
            spark.stop()

if __name__ == "__main__":
    main()
