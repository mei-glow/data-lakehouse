# dags/03_silver_dbt_transformation.py

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
    dag_id="03_silver_dbt_transformation",
    description="Run dbt to transform Bronze → Silver",
    default_args=default_args,
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["lakehouse", "silver", "dbt"],
) as dag:

    start = EmptyOperator(
        task_id="start_dbt_transformation"
    )

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command="""
        docker exec dbt bash -c '
        cd /usr/app/dbt &&
        dbt deps
        '
        """,
    )

    #  dbt run với real-time Thrift monitoring
    dbt_run_silver = BashOperator(
        task_id="dbt_run_silver",
        bash_command="""
        set -e
        
        echo " Starting dbt transformation..."
        echo " Start: $(date '+%Y-%m-%d %H:%M:%S')"
        echo ""
        
        # Start monitoring Thrift logs in background
        (
            echo " Monitoring Thrift Server logs..."
            while true; do
                docker logs spark-thrift --tail 5 2>&1 | grep -E "Starting task|Finished task|stage.*finished" || true
                sleep 3
            done
        ) &
        MONITOR_PID=$!
        
        # Run dbt
        docker exec dbt bash -c '
        cd /usr/app/dbt &&
        dbt run --select silver
        '
        DBT_EXIT=$?
        
        # Stop monitoring
        kill $MONITOR_PID 2>/dev/null || true
        
        echo ""
        echo " End: $(date '+%Y-%m-%d %H:%M:%S')"
        
        if [ $DBT_EXIT -eq 0 ]; then
            echo " dbt run completed successfully"
        fi
        """,
        execution_timeout=timedelta(minutes=30),
    )

    dbt_test_silver = BashOperator(
        task_id="dbt_test_silver",
        bash_command="""
        docker exec dbt bash -c '
        cd /usr/app/dbt &&
        dbt test --select silver
        '
        """,
    )

    dbt_docs = BashOperator(
        task_id="dbt_docs_generate",
        bash_command="""
        docker exec dbt bash -c '
        cd /usr/app/dbt &&
        dbt docs generate
        '
        """,
    )

    #  DEPENDENCY
    start >> dbt_deps >> dbt_run_silver >> dbt_test_silver >> dbt_docs