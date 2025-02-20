import os
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

from data_ingestion.main import main # type: ignore


default_args = {"owner": "airflow", "start_date": datetime(2024, 1, 1)}


dag = DAG(
    "dag_reatail_etl",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False
)

def print_working_directory():
    print("Current working directory:", os.getcwd())


task_print_cwd = PythonOperator(
    task_id="print_cwd",
    python_callable=print_working_directory,
    dag=dag,
)

ingest_data = PythonOperator(
    task_id="ingest_data",
    python_callable=main,
    dag=dag
)

dbt_clean = BashOperator(
    task_id="dbt_clean",
    bash_command="cd /home/sachi/airflow/dbt/data_pipeline && dbt clean",
    dag=dag
)

dbt_snapshot = BashOperator(
    task_id="dbt_snapshot",
    bash_command="cd /home/sachi/airflow/dbt/data_pipeline && dbt source freshness && dbt snapshot",
    dag=dag
)

dbt_run = BashOperator(
    task_id="dbt_run",
    bash_command="cd /home/sachi/airflow/dbt/data_pipeline && dbt deps && dbt run",
    dag=dag
)

dbt_test = BashOperator(
    task_id="dbt_test",
    bash_command="cd /home/sachi/airflow/dbt/data_pipeline && dbt test",
    dag=dag
)

task_print_cwd >> ingest_data >> dbt_snapshot >> dbt_clean >> dbt_run >> dbt_test
