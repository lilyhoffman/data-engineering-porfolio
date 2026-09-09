import os

import psycopg2


DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "database": os.getenv("POSTGRES_DB", "market_data"),
    "user": os.getenv("POSTGRES_USER", "market_user"),
    "password": os.getenv("POSTGRES_PASSWORD"),
    "port": int(os.getenv("POSTGRES_PORT", "5432")),
}


def start_pipeline_run(dag_run_id):
    connection = psycopg2.connect(**DB_CONFIG)
    cursor = connection.cursor()

    cursor.execute(
        """
        INSERT INTO pipeline_runs (
            pipeline_name,
            dag_run_id,
            start_time,
            status
        )
        VALUES (%s, %s, NOW(), %s)
        RETURNING run_id;
        """,
        (
            "financial_market_data_pipeline",
            dag_run_id,
            "RUNNING",
        ),
    )

    run_id = cursor.fetchone()[0]

    connection.commit()
    cursor.close()
    connection.close()

    print(f"Started pipeline audit record for {dag_run_id}.")
    return run_id


def finish_pipeline_run(dag_run_id):
    connection = psycopg2.connect(**DB_CONFIG)
    cursor = connection.cursor()

    cursor.execute(
        """
        UPDATE pipeline_runs
        SET
            end_time = NOW(),
            status = 'SUCCESS'
        WHERE dag_run_id = %s;
        """,
        (dag_run_id,),
    )

    connection.commit()
    cursor.close()
    connection.close()

    print(f"Pipeline audit record marked SUCCESS for {dag_run_id}.")


def fail_pipeline_run(dag_run_id, error_message):
    connection = psycopg2.connect(**DB_CONFIG)
    cursor = connection.cursor()

    cursor.execute(
        """
        UPDATE pipeline_runs
        SET
            end_time = NOW(),
            status = 'FAILED',
            error_message = %s
        WHERE dag_run_id = %s;
        """,
        (
            str(error_message),
            dag_run_id,
        ),
    )

    connection.commit()
    cursor.close()
    connection.close()

    print(f"Pipeline audit record marked FAILED for {dag_run_id}.")