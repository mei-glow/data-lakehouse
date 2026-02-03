from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from datetime import timedelta
import pendulum

default_args = {
    "owner": "lakehouse",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="04_gold_aggregation",
    description="Run dbt to aggregate Silver → Gold",
    default_args=default_args,
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["lakehouse", "gold", "dbt"],
) as dag:

    start = EmptyOperator(task_id="start_gold_aggregation")

    # Run Core Business tables
    dbt_run_core = BashOperator(
        task_id="dbt_run_core_business",
        bash_command="""
        docker exec dbt bash -c '
        cd /usr/app/dbt &&
        dbt run --select gold.core
        '
        """,
        execution_timeout=timedelta(minutes=20),
    )

    # Run Funnel Analysis tables
    dbt_run_funnel = BashOperator(
        task_id="dbt_run_funnel_analysis",
        bash_command="""
        docker exec dbt bash -c '
        cd /usr/app/dbt &&
        dbt run --select gold.funnel
        '
        """,
        execution_timeout=timedelta(minutes=15),
    )

    # Run Traffic table
    dbt_run_traffic = BashOperator(
        task_id="dbt_run_traffic",
        bash_command="""
        docker exec dbt bash -c '
        cd /usr/app/dbt &&
        dbt run --select gold.traffic
        '
        """,
        execution_timeout=timedelta(minutes=10),
    )

    # Test all Gold tables
    dbt_test_gold = BashOperator(
        task_id="dbt_test_gold",
        bash_command="""
        docker exec dbt bash -c '
        cd /usr/app/dbt &&
        dbt test --select gold
        '
        """,
    )

    # Generate docs
    dbt_docs = BashOperator(
        task_id="dbt_docs_generate",
        bash_command="""
        docker exec dbt bash -c '
        cd /usr/app/dbt &&
        dbt docs generate
        '
        """,
    )

    end = EmptyOperator(task_id="end_gold_aggregation")

    # Dependencies
    start >> [dbt_run_core, dbt_run_funnel, dbt_run_traffic] >> dbt_test_gold >> dbt_docs >> end