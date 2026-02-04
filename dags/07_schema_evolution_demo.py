from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from datetime import timedelta
import pendulum

default_args = {
    "owner": "lakehouse",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Common Spark submit args (giống style các DAG cũ)
SPARK_SUBMIT_BASE = r"""
set -e

echo " Submitting Spark job..."
echo " Start: $(date '+%Y-%m-%d %H:%M:%S')"
echo ""

docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --name "{job_name}" \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,org.apache.hadoop:hadoop-aws:3.3.4,software.amazon.awssdk:bundle:2.20.18,software.amazon.awssdk:url-connection-client:2.20.18 \
  --conf spark.driver.memory=2g \
  --conf spark.executor.memory=2g \
  --conf spark.executor.cores=2 \
  {job_file}

EXIT_CODE=$?

echo ""
echo " End: $(date '+%Y-%m-%d %H:%M:%S')"

if [ $EXIT_CODE -eq 0 ]; then
  echo " Spark job completed successfully"
else
  echo " Spark job failed with exit code: $EXIT_CODE"
  exit $EXIT_CODE
fi
"""

with DAG(
    dag_id="07_schema_evolution_demo",
    description="Demo Apache Iceberg Schema Evolution - Add payment_method column",
    default_args=default_args,
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["lakehouse", "schema-evolution", "iceberg"],
) as dag:

    start = EmptyOperator(task_id="start_schema_evolution")

    # Task 1: Check current schema
    check_schema = BashOperator(
        task_id="check_current_schema",
        bash_command=SPARK_SUBMIT_BASE.format(
            job_name="Iceberg Schema - Check Current",
            job_file="/opt/spark/jobs/schema_check.py",
        ),
    )

    # Task 2: Add new column
    add_column = BashOperator(
        task_id="add_payment_method_column",
        bash_command=SPARK_SUBMIT_BASE.format(
            job_name="Iceberg Schema - Add Column payment_method",
            job_file="/opt/spark/jobs/schema_add_column.py",
        ),
    )

    # Task 3: Insert new data
    insert_data = BashOperator(
        task_id="insert_new_data_with_payment",
        bash_command=SPARK_SUBMIT_BASE.format(
            job_name="Iceberg Schema - Insert New Data",
            job_file="/opt/spark/jobs/schema_insert_data.py",
        ),
    )

    # Task 4: Verify compatibility
    verify = BashOperator(
        task_id="verify_backward_compatibility",
        bash_command=SPARK_SUBMIT_BASE.format(
            job_name="Iceberg Schema - Verify Compatibility",
            job_file="/opt/spark/jobs/schema_verify.py",
        ),
    )

    # Task 5: Show success summary (giữ nguyên tinh thần code cũ)
    summary = BashOperator(
        task_id="show_summary",
        bash_command=r"""
        echo ""
        echo "╔════════════════════════════════════════════════════════════════╗"
        echo "║      SCHEMA EVOLUTION DEMO COMPLETED SUCCESSFULLY!          ║"
        echo "╚════════════════════════════════════════════════════════════════╝"
        echo ""
        echo "What happened:"
        echo "   1. ✓ Checked original Bronze table schema"
        echo "   2. ✓ Added 'payment_method' column via ALTER TABLE"
        echo "   3. ✓ Inserted new data with payment_method values"
        echo "   4. ✓ Verified old data still works (payment_method = NULL)"
        echo ""
        echo "Apache Iceberg Features Demonstrated:"
        echo "   ✨ Schema Evolution without data rewrite"
        echo "   ✨ Backward compatibility (old data readable)"
        echo "   ✨ Forward compatibility (new queries work)"
        echo "   ✨ Snapshot-based time travel capability"
        echo ""
        echo "This proves Iceberg can handle production schema changes!"
        echo ""
        """,
    )

    end = EmptyOperator(task_id="end_schema_evolution")

    start >> check_schema >> add_column >> insert_data >> verify >> summary >> end
