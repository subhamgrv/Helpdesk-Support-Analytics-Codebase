
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="supportops_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
) as dag:

    generate = BashOperator(
        task_id="generate_data",
        bash_command="python scripts/generate_data.py",
    )

    pipeline = BashOperator(
        task_id="run_pipeline",
        bash_command="python scripts/run_pipeline.py",
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd dbt_project && dbt run --profiles-dir .",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd dbt_project && dbt test --profiles-dir .",
    )

    generate >> pipeline >> dbt_run >> dbt_test