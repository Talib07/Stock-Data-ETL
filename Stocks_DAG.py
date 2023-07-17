from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from datetime import timedelta
from Extraction_csv import run_extraction_csv
from Extraction_json import run_extraction_json

default_args = {
    'owner' : 'airflow',
    'depends_on_past' : False,
    'start_date' : datetime(2023,6,15),
    'email' : ['airflow@example.com'],
    'email_on_failure' : False,
    'email_on_retry' : False,
    'retries' : 1,
    'retry_delay' : timedelta(minutes=1)
}

dag = DAG(
    'Stocks_DAG',
    default_args = default_args,
    description = 'Stocks ETL Code'
)

task1 = PythonOperator(
    task_id='extraction_json',
    python_callable=run_extraction_json,
    dag=dag
)

task2 = PythonOperator(
    task_id='extraction_csv',
    python_callable=run_extraction_csv,
    dag=dag
)

task1 >> task2