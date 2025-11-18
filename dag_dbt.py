from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="dbt_run_test_snapshot",
    default_args=default_args,
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
) as dag:

   
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="""
        cd /home/airflow/gcs/dags/dbt_project
        dbt run --profiles-dir .
        """
    )

    
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="""
        cd /home/airflow/gcs/dags/dbt_project
        dbt test --profiles-dir .
        """
    )

    
    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command="""
        cd /home/airflow/gcs/dags/dbt_project
        dbt snapshot --profiles-dir .
        """
    )

    dbt_run >> dbt_test >> dbt_snapshot
