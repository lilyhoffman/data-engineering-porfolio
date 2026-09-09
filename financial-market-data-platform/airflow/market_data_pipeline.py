from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from audit_utils import (
    start_pipeline_run,
    finish_pipeline_run,
    fail_pipeline_run,
)


def log_task_failure(context):
    task_instance = context["task_instance"]
    exception = context.get("exception")
    dag_run_id = task_instance.run_id

    print("=" * 60)
    print("PIPELINE TASK FAILURE")
    print(f"DAG: {task_instance.dag_id}")
    print(f"Task: {task_instance.task_id}")
    print(f"Run ID: {dag_run_id}")
    print(f"Try Number: {task_instance.try_number}")
    print(f"Exception: {exception}")
    print("=" * 60)

    fail_pipeline_run(
        dag_run_id=dag_run_id,
        error_message=exception,
    )


default_args = {
    "owner": "lily",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "on_failure_callback": log_task_failure,
}


with DAG(
    dag_id="financial_market_data_pipeline",
    description="End-to-end financial market data engineering pipeline",
    default_args=default_args,
    start_date=datetime(2026, 9, 1),
    schedule="0 18 * * 1-5",
    catchup=False,
    tags=["data-engineering", "financial-markets"],
) as dag:

    start_audit = PythonOperator(
        task_id="start_audit",
        python_callable=start_pipeline_run,
        op_kwargs={
            "dag_run_id": "{{ run_id }}"
        },
    )

    fetch_market_data = BashOperator(
        task_id="fetch_market_data",
        bash_command="cd /app && python ingestion/FAKE_FILE.py",
    )

    bronze_to_silver = BashOperator(
        task_id="bronze_to_silver",
        bash_command=(
            "docker exec market_spark "
            "spark-submit spark/transform_stock_prices.py"
        ),
    )

    load_silver_to_postgres = BashOperator(
        task_id="load_silver_to_postgres",
        bash_command=(
            "cd /app && "
            "python ingestion/load_silver_to_postgres.py"
        ),
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            "dbt run "
            "--project-dir /app/dbt/market_analytics "
            "--profiles-dir /app/dbt/market_analytics"
        ),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            "dbt test "
            "--project-dir /app/dbt/market_analytics "
            "--profiles-dir /app/dbt/market_analytics"
        ),
    )

    finish_audit = PythonOperator(
        task_id="finish_audit",
        python_callable=finish_pipeline_run,
        op_kwargs={
            "dag_run_id": "{{ run_id }}"
        },
    )

    (
        start_audit
        >> fetch_market_data
        >> bronze_to_silver
        >> load_silver_to_postgres
        >> dbt_run
        >> dbt_test
        >> finish_audit
    )