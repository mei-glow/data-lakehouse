import logging
from schema_common import create_spark_session

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

TABLE = "iceberg.bronze.ecommerce_events"

def main():
    spark = None
    try:
        spark = create_spark_session("Schema Evolution - Verify Compatibility")

        logger.info("=" * 80)
        logger.info("🔄 VERIFYING BACKWARD COMPATIBILITY")
        logger.info("=" * 80)

        logger.info("📊 Query 1: Count records by payment_method (NULL -> LEGACY_DATA)")
        spark.sql(f"""
            SELECT
                COALESCE(payment_method, 'LEGACY_DATA') AS payment_method,
                COUNT(*) AS count,
                MIN(event_time) AS earliest_event,
                MAX(event_time) AS latest_event
            FROM {TABLE}
            GROUP BY payment_method
            ORDER BY count DESC
        """).show(truncate=False)

        logger.info("📊 Query 2: Old vs new data by _source_file")
        spark.sql(f"""
            SELECT
                _source_file,
                COUNT(*) AS total_records,
                COUNT(payment_method) AS records_with_payment,
                COUNT(*) - COUNT(payment_method) AS records_without_payment
            FROM {TABLE}
            GROUP BY _source_file
            ORDER BY total_records DESC
            LIMIT 10
        """).show(truncate=False)

        logger.info("📊 Query 3: Sample mixed data (latest 10)")
        spark.sql(f"""
            SELECT
                event_time,
                event_type,
                price,
                payment_method,
                CASE WHEN payment_method IS NULL THEN 'OLD_SCHEMA' ELSE 'NEW_SCHEMA' END AS data_version,
                _source_file
            FROM {TABLE}
            ORDER BY event_time DESC
            LIMIT 10
        """).show(truncate=False)

        logger.info("📸 Iceberg Snapshot History (latest 5)")
        spark.sql(f"""
            SELECT committed_at, snapshot_id, operation, summary
            FROM {TABLE}.snapshots
            ORDER BY committed_at DESC
            LIMIT 5
        """).show(truncate=False)

        logger.info("✅ SCHEMA EVOLUTION VERIFICATION COMPLETE!")

    finally:
        if spark:
            spark.stop()

if __name__ == "__main__":
    main()
