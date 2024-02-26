from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
from spotify_etl import *

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 11, 8),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'spotify_dag',
    default_args=default_args,
    description='Our first DAG with ETL process!',
    schedule_interval=timedelta(days=1),
)

extract_spotify_data = PythonOperator(
    task_id='extract_spotify_data',
    python_callable=spotify_data_extract,
    dag=dag, 
)

transform_spotify_data = PythonOperator(
    task_id='transform_spotify_data',
    python_callable=spotify_data_transform,
    provide_context=True,
    dag=dag, 
)

load_spotify_data = PythonOperator(
    task_id='load_spotify_data',
    python_callable=spotify_data_load,
    provide_context=True,
    dag=dag, 
)


extract_spotify_data >> transform_spotify_data >> load_spotify_data