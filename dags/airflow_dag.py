import os
import sys
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

parent_folder = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parent_folder)

from write_csv_to_postgres import write_to_database
from write_df_to_postgres import write_df_to_postgres_main



default_args = {
    'owner': 'dogukanulu',
    'start_date': days_ago(0),
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}

with DAG('csv_extract_airflow_docker', default_args=default_args, schedule_interval='@daily', catchup=False) as dag:

    write_csv_to_postgres = PythonOperator(
        task_id='write_csv_to_postgres',
        python_callable=write_to_database,
        retries=1,
        retry_delay=timedelta(seconds=15))

    write_df_to_postgres = PythonOperator(
        task_id='write_df_to_postgres',
        python_callable=write_df_to_postgres_main,
        retries=1,
        retry_delay=timedelta(seconds=15))
    
    write_csv_to_postgres >> write_df_to_postgres 