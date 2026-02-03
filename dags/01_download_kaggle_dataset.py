from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import timedelta, datetime
import pendulum
import os
import subprocess
import logging
import glob
from minio import Minio
from minio.error import S3Error

# =========================
# Default arguments
# =========================
default_args = {
    "owner": "lakehouse",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# =========================
# Task 1: Download dataset
# =========================
def download_kaggle_dataset(**context):
    """Download eCommerce dataset from Kaggle"""

    kaggle_username = os.getenv("KAGGLE_USERNAME")
    kaggle_key = os.getenv("KAGGLE_KEY")

    if not kaggle_username or not kaggle_key:
        raise ValueError(
            "Kaggle credentials not found! "
            "Set KAGGLE_USERNAME and KAGGLE_KEY"
        )

    dataset_name = "mkechinov/ecommerce-events-history-in-cosmetics-shop"
    output_path = "/opt/airflow/data/raw/ecommerce"

    os.makedirs(output_path, exist_ok=True)

    logging.info(f"Downloading dataset: {dataset_name}")
    logging.info(f"Output path: {output_path}")

    try:
        result = subprocess.run(
            [
                "kaggle", "datasets", "download",
                "-d", dataset_name,
                "-p", output_path,
                "--unzip",
                "--force",
            ],
            capture_output=True,
            text=True,
            check=True,
        )

        logging.info("✅ Download & unzip successful")
        logging.info(result.stdout)

        # Remove leftover zip files
        zip_files = glob.glob(os.path.join(output_path, "*.zip"))
        for z in zip_files:
            logging.warning(f"Removing leftover zip: {z}")
            os.remove(z)

        # Find all CSV files
        csv_files = glob.glob(os.path.join(output_path, "*.csv"))
        if not csv_files:
            raise FileNotFoundError("No CSV files found after download")

        # Sort by size (largest first)
        csv_files_sorted = sorted(
            csv_files,
            key=lambda x: os.path.getsize(x),
            reverse=True,
        )

        logging.info(f"✅ Found {len(csv_files_sorted)} CSV files:")
        for f in csv_files_sorted:
            size_mb = os.path.getsize(f) / (1024 * 1024)
            logging.info(f"   - {os.path.basename(f)} ({size_mb:.2f} MB)")

        # Push to XCom for next tasks
        context["ti"].xcom_push(key="csv_files", value=csv_files_sorted)

        return csv_files_sorted

    except subprocess.CalledProcessError as e:
        logging.error("❌ Kaggle CLI failed")
        logging.error(e.stderr)
        raise


# =========================
# Task 2: Verify ALL datasets
# =========================
def verify_dataset(**context):
    """Verify all CSV files"""
    import pandas as pd

    ti = context["ti"]
    csv_files = ti.xcom_pull(task_ids="download_dataset", key="csv_files")

    if not csv_files:
        raise ValueError("No CSV files to verify")

    logging.info(f"Verifying {len(csv_files)} CSV files...")

    required_columns = [
        "event_time",
        "event_type",
        "product_id",
        "user_id",
        "price",
    ]

    verified_files = []

    for file_path in csv_files:
        logging.info(f"\n📄 Verifying: {os.path.basename(file_path)}")
        
        # Read sample
        df = pd.read_csv(file_path, nrows=100)
        
        # Check columns
        missing = [c for c in required_columns if c not in df.columns]
        if missing:
            raise ValueError(f"{file_path} missing columns: {missing}")
        
        # Get stats
        file_size_mb = os.path.getsize(file_path) / (1024 * 1024)
        with open(file_path, 'r') as f:
            total_rows = sum(1 for _ in f) - 1  # exclude header
        
        logging.info(f"   ✅ Size: {file_size_mb:.2f} MB")
        logging.info(f"   ✅ Rows: {total_rows:,}")
        logging.info(f"   ✅ Columns: {df.columns.tolist()}")
        
        verified_files.append({
            "path": file_path,
            "size_mb": file_size_mb,
            "rows": total_rows,
        })

    logging.info(f"\n✅ All {len(csv_files)} CSV files verified!")
    
    # Push verified files to XCom
    ti.xcom_push(key="verified_files", value=verified_files)
    
    return verified_files


# =========================
# Task 3: Upload ALL files to MinIO
# =========================
def upload_all_to_minio(**context):
    """Upload ALL CSV files to MinIO"""
    
    ti = context["ti"]
    csv_files = ti.xcom_pull(task_ids="download_dataset", key="csv_files")
    
    if not csv_files:
        raise ValueError("No CSV files to upload")
    
    minio_endpoint = "minio:9000"
    minio_access_key = os.getenv("MINIO_ROOT_USER", "admin")
    minio_secret_key = os.getenv("MINIO_ROOT_PASSWORD", "password")
    bucket_name = "warehouse"
    
    logging.info(f"Connecting to MinIO at {minio_endpoint}")
    
    try:
        # Initialize MinIO client
        client = Minio(
            minio_endpoint,
            access_key=minio_access_key,
            secret_key=minio_secret_key,
            secure=False
        )
        
        # Ensure bucket exists
        if not client.bucket_exists(bucket_name):
            logging.warning(f"Creating bucket '{bucket_name}'...")
            client.make_bucket(bucket_name)
            logging.info(f"✅ Bucket created")
        else:
            logging.info(f"✅ Bucket '{bucket_name}' exists")
        
        upload_date = datetime.now().strftime("%Y-%m-%d")
        uploaded_objects = []
        
        # Upload each CSV file
        for file_path in csv_files:
            file_name = os.path.basename(file_path)
            object_name = f"raw/ecommerce/{upload_date}/{file_name}"
            
            logging.info(f"\n📤 Uploading: {file_name}")
            logging.info(f"   → s3://{bucket_name}/{object_name}")
            
            file_size_mb = os.path.getsize(file_path) / (1024 * 1024)
            logging.info(f"   Size: {file_size_mb:.2f} MB")
            
            # Upload
            client.fput_object(
                bucket_name=bucket_name,
                object_name=object_name,
                file_path=file_path,
            )
            
            # Verify upload
            stat = client.stat_object(bucket_name, object_name)
            uploaded_size_mb = stat.size / (1024 * 1024)
            
            logging.info(f"   ✅ Uploaded: {uploaded_size_mb:.2f} MB")
            
            uploaded_objects.append({
                "file_name": file_name,
                "object_name": object_name,
                "s3a_path": f"s3a://{bucket_name}/{object_name}",
                "size_mb": uploaded_size_mb,
            })
        
        logging.info(f"\n🎉 Successfully uploaded {len(uploaded_objects)} files to MinIO!")
        
        # Push to XCom
        ti.xcom_push(key="bucket_name", value=bucket_name)
        ti.xcom_push(key="uploaded_objects", value=uploaded_objects)
        ti.xcom_push(key="upload_date", value=upload_date)
        
        return uploaded_objects
        
    except S3Error as e:
        logging.error(f"❌ MinIO S3 Error: {e}")
        raise
    except Exception as e:
        logging.error(f"❌ Upload failed: {e}")
        raise


# =========================
# Task 4: Verify MinIO Upload
# =========================
def verify_minio_upload(**context):
    """Verify ALL files exist in MinIO"""
    
    ti = context["ti"]
    
    bucket_name = ti.xcom_pull(task_ids="upload_all_to_minio", key="bucket_name")
    uploaded_objects = ti.xcom_pull(task_ids="upload_all_to_minio", key="uploaded_objects")
    
    if not bucket_name or not uploaded_objects:
        raise ValueError("Missing bucket or object information")
    
    minio_endpoint = "minio:9000"
    minio_access_key = os.getenv("MINIO_ROOT_USER", "admin")
    minio_secret_key = os.getenv("MINIO_ROOT_PASSWORD", "password")

    try:
        client = Minio(
            minio_endpoint,
            access_key=minio_access_key,
            secret_key=minio_secret_key,
            secure=False
        )
        
        logging.info(f"📂 Verifying {len(uploaded_objects)} files in MinIO:")
        
        total_size = 0
        
        for obj in uploaded_objects:
            object_name = obj["object_name"]
            
            # Verify each object
            stat = client.stat_object(bucket_name, object_name)
            size_mb = stat.size / (1024 * 1024)
            total_size += size_mb
            
            logging.info(f"\n✅ {obj['file_name']}")
            logging.info(f"   Path: s3://{bucket_name}/{object_name}")
            logging.info(f"   Size: {size_mb:.2f} MB")
            logging.info(f"   Modified: {stat.last_modified}")
        
        logging.info(f"\n📊 Summary:")
        logging.info(f"   Total files: {len(uploaded_objects)}")
        logging.info(f"   Total size: {total_size:.2f} MB")
        logging.info(f"   Bucket: {bucket_name}")
        
        # List all objects in prefix
        logging.info(f"\n📂 All files in s3://{bucket_name}/raw/ecommerce/:")
        objects = client.list_objects(bucket_name, prefix="raw/ecommerce/", recursive=True)
        
        for obj in objects:
            obj_size_mb = obj.size / (1024 * 1024)
            logging.info(f"   - {obj.object_name} ({obj_size_mb:.2f} MB)")
        
        logging.info("\n✅ MinIO upload verification passed!")
        
        return {
            "bucket": bucket_name,
            "total_files": len(uploaded_objects),
            "total_size_mb": total_size,
        }
        
    except S3Error as e:
        logging.error(f"❌ MinIO verification failed: {e}")
        raise


# =========================
# DAG definition
# =========================
with DAG(
    dag_id="01_download_kaggle_dataset",
    description="Download eCommerce dataset from Kaggle and upload ALL files to MinIO",
    default_args=default_args,
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["lakehouse", "bronze", "ingestion"],
) as dag:

    download_task = PythonOperator(
        task_id="download_dataset",
        python_callable=download_kaggle_dataset,
    )

    verify_task = PythonOperator(
        task_id="verify_dataset",
        python_callable=verify_dataset,
    )
    
    upload_task = PythonOperator(
        task_id="upload_all_to_minio",
        python_callable=upload_all_to_minio,
    )
    
    verify_minio_task = PythonOperator(
        task_id="verify_minio_upload",
        python_callable=verify_minio_upload,
    )

    download_task >> verify_task >> upload_task >> verify_minio_task