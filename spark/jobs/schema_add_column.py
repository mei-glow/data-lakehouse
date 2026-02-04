import logging
from schema_common import create_spark_session

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

TABLE = "iceberg.bronze.ecommerce_events"

def main():
    spark = None
    try:
        spark = create_spark_session("Schema Evolution - Add Column")

        logger.info("=" * 80)
        logger.info("➕ ADDING NEW COLUMN: payment_method")
        logger.info("=" * 80)

        # Idempotent: nếu cột đã tồn tại thì skip
        cols = [r.col_name for r in spark.sql(f"DESCRIBE {TABLE}").collect() if r.col_name and not r.col_name.startswith("#")]
        if "payment_method" in cols:
            logger.info("⏭️ Column payment_method already exists -> skip ALTER")
        else:
            logger.info("🔧 Executing ALTER TABLE ADD COLUMN payment_method ...")
            spark.sql(f"""
                ALTER TABLE {TABLE}
                ADD COLUMN payment_method STRING
                COMMENT 'Payment method: credit_card, debit_card, paypal, cash_on_delivery'
            """)
            logger.info("✅ Column added successfully!")

        logger.info("🔍 New schema:")
        spark.sql(f"DESCRIBE {TABLE}").show(truncate=False)

        logger.info("📸 Snapshot History (latest 5):")
        spark.sql(f"""
            SELECT committed_at, snapshot_id, operation, summary
            FROM {TABLE}.snapshots
            ORDER BY committed_at DESC
            LIMIT 5
        """).show(truncate=False)

    finally:
        if spark:
            spark.stop()

if __name__ == "__main__":
    main()
