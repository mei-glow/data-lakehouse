from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

from datetime import timedelta
import pendulum
import json
import logging
import os

default_args = {
    "owner": "lakehouse",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def discover_and_save_file_list(**context):
    """Discover files và save to JSON file"""
    from minio import Minio
    
    logging.info("🔍 Discovering CSV files...")
    
    client = Minio("minio:9000", access_key="admin", secret_key="password", secure=False)
    objects = client.list_objects("warehouse", prefix="raw/ecommerce/", recursive=True)
    
    file_list = []
    for obj in objects:
        if obj.object_name.endswith('.csv'):
            file_list.append({
                "file_name": obj.object_name.split('/')[-1],
                "s3a_path": f"s3a://warehouse/{obj.object_name}"
            })
    
    file_list = sorted(file_list, key=lambda x: x['file_name'])
    
    if not file_list:
        raise ValueError("No CSV files found in MinIO")
    
    # Save to shared volume
    output_file = "/opt/airflow/data/bronze_file_list.json"
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    with open(output_file, 'w') as f:
        json.dump(file_list, f, indent=2)
    
    logging.info(f" Saved {len(file_list)} files to {output_file}")
    logging.info(f" File list:")
    for f in file_list:
        logging.info(f"   - {f['file_name']}")
    
    return len(file_list)


with DAG(
    dag_id="02_bronze_ingestion_to_iceberg",
    default_args=default_args,
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["lakehouse", "bronze"],
) as dag:

    discover_task = PythonOperator(
        task_id="discover_files",
        python_callable=discover_and_save_file_list,
    )

    spark_job_task = BashOperator(
        task_id="run_spark_job",
        bash_command="""
        set -e
        
        echo " File list location: /opt/airflow/data/bronze_file_list.json"
        cat /opt/airflow/data/bronze_file_list.json
        
        echo ""
        echo " Submitting Spark job..."
        
        # Submit Spark job with --file-list pointing to JSON file
        docker exec spark-master /opt/spark/bin/spark-submit \
          --master spark://spark-master:7077 \
          --deploy-mode client \
          --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,org.apache.hadoop:hadoop-aws:3.3.4,software.amazon.awssdk:bundle:2.20.18,software.amazon.awssdk:url-connection-client:2.20.18 \
          --conf spark.executor.memory=2g \
          --conf spark.driver.memory=2g \
          /opt/spark/jobs/bronze_ingestion.py \
          --file-list /opt/airflow/data/bronze_file_list.json \
          --mode full
        
        EXIT_CODE=$?
        
        if [ $EXIT_CODE -eq 0 ]; then
            echo " Spark job completed successfully"
        else
            echo " Spark job failed with exit code: $EXIT_CODE"
            exit $EXIT_CODE
        fi
        """,
    )


    bronze_ingestion_done = EmptyOperator(
        task_id="bronze_ingestion_done"
    )

    discover_task >> spark_job_task >> bronze_ingestion_done
