import os
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

# Define the directory where your scripts are located
script_dir = '../scripts/'

# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 29),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# Define the DAG
dag = DAG(
    'etl_pipeline',
    default_args=default_args,
    description='A simple ETL pipeline',
    schedule_interval='@daily',
)

# Define the tasks for data extraction
extract_csv_task = PythonOperator(
    task_id='extract_csv',
    python_callable=lambda: os.system(f'python {script_dir}/data_extraction/csv_extractor.py'),
    dag=dag,
)

extract_mysql_task = PythonOperator(
    task_id='extract_mysql',
    python_callable=lambda: os.system(f'python {script_dir}/data_extraction/mysql_extractor.py'),
    dag=dag,
)

# Define the tasks for data transformation
transform_csv_task = PythonOperator(
    task_id='transform_csv',
    python_callable=lambda: os.system(f'python {script_dir}/data_transformation/csv_transformer.py'),
    dag=dag,
)

transform_mysql_task = PythonOperator(
    task_id='transform_mysql',
    python_callable=lambda: os.system(f'python {script_dir}/data_transformation/mysql_transformer.py'),
    dag=dag,
)

# Define the task for data loading
load_data_task = PythonOperator(
    task_id='load_data',
    python_callable=lambda: os.system(f'python {script_dir}/data_loading/data_loader.py'),
    dag=dag,
)

# Define the task dependencies
extract_csv_task >> transform_csv_task >> load_data_task
extract_mysql_task >> transform_mysql_task >> load_data_task
