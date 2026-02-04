# Project title: Sailor Moon - Data Lakehouse Project

# Data Lakehouse Project

This repository implements a modern data lakehouse architecture using open-source tools. The stack includes Airflow for orchestration, Spark for processing, dbt for transformations, ClickHouse for analytics, and Superset for BI dashboards.

## Project Structure

- `dags/` — Airflow DAGs for orchestrating data pipelines
- `dbt/` — dbt project for SQL-based data transformations
- `clickhouse/` — ClickHouse configuration and setup scripts
- `spark/` — Spark jobs and configuration
- `superset/` — Superset configuration for BI dashboards
- `data/` — Raw and processed data storage
- `models/` — Data models (gold, silver, staging)
- `logs/` — Pipeline and application logs
- `config/` — Configuration files (e.g., Airflow)
- `requirements.txt` — Python dependencies
- `docker-compose.yaml` — Multi-service orchestration
- `Dockerfile` — Base image for custom services

## Quick Start

1. **Clone the repository**
   ```powershell
   git clone <your-repo-url>
   cd data-lakehouse
   ```

2. **Set up environment variables**
   - Copy the sample environment file and rename it:
     ```powershell
     copy .env.example .env
     ```
   - Mở file `.env` vừa tạo và thêm thông tin tài khoản Kaggle của bạn:
     ```env
     KAGGLE_USERNAME=your_kaggle_username
     KAGGLE_KEY=your_kaggle_api_key
     ```
   - (Tùy chọn) Thêm các biến môi trường hoặc thông tin bí mật khác nếu cần.

3. **Start the stack**
   ```powershell
   docker-compose up --build
   ```

4. **Access services:**
   - Airflow: http://localhost:8080
   - Superset: http://localhost:8088
   - ClickHouse: http://localhost:8123

5. **Run the data pipeline:**
   - Open the Airflow UI at http://localhost:8080
   - Enable and trigger each DAG in the following order:
     1. `01_download_kaggle_dataset`
     2. `02_bronze_ingestion_to_iceberg`
     3. `03_silver_dbt_transformation`
     4. `04_gold_aggregation`
     5. `05_clickhouse_complete_setup`
     6. `06_superset_dashboard`
     7. `07_schema_evolution_demo` (new DAG for schema evolution demo)
   - Alternatively, you can trigger DAGs via the Airflow CLI:
     ```powershell
     docker-compose exec airflow-webserver airflow dags trigger <dag_id>
     ```

## Main Components

- **Airflow**: Orchestrates ETL pipelines via DAGs in `dags/`.
- **Spark**: Handles large-scale data processing (see `spark/`).
- **dbt**: Manages SQL transformations and modeling (see `dbt/`).
- **ClickHouse**: High-performance OLAP database for analytics.
- **Superset**: Data visualization and dashboarding.

## Data Pipeline Overview

1. Download datasets (Kaggle or other sources)
2. Ingest raw data (bronze) to Iceberg
3. Transform data (silver) using dbt
4. Aggregate and model data (gold)
5. Load into ClickHouse for analytics
6. Visualize with Superset

## Customization
- Add new DAGs to `dags/`
- Add/modify dbt models in `dbt/models/`
- Update Spark jobs in `spark/jobs/`
- Configure Superset dashboards in `superset/`

## Requirements
- Windows, macOS, or Linux (Windows recommended for this guide)
- At least 16 GB RAM
- At least 100 GB free disk space
- Docker & Docker Compose (latest versions recommended)
- (Optional) Python 3.12+ for local development
- Stable internet connection (for downloading datasets and Docker images)

## Prequisite (Yêu cầu hệ thống)
- Windows, macOS, hoặc Linux (khuyến nghị Windows cho hướng dẫn này)
- Tối thiểu 16 GB RAM
- Tối thiểu 100 GB dung lượng ổ đĩa trống
- Docker & Docker Compose (khuyến nghị bản mới nhất)
- (Tùy chọn) Python 3.12+ cho phát triển local
- Kết nối internet ổn định (để tải dataset và Docker images)
