from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import pandas as pd
import os

def read_files_from_folder_A():
    filepath = r'/opt/airflow/files/A/test.csv'
    df = pd.read_csv(filepath, delimiter=';')
    grouped_data = df.groupby(['Prod']).agg({'Value': 'sum'}).reset_index()
    grouped_data = grouped_data.to_dict()
    return grouped_data

def export_to_folder_B(ti):
    grouped_data = ti.xcom_pull(task_ids='read_files_from_folder_A')
    grouped_data = pd.DataFrame(grouped_data)
    grouped_data.to_csv(r'/opt/airflow/files/B/group.csv', index=False)

    print('finish')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False
}

with DAG(
    'first_file_processing_dag',
    default_args=default_args,
    description='DAG for file processing',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False
) as dag:

    read_files_task = PythonOperator(
        task_id='read_files_from_folder_A',
        python_callable=read_files_from_folder_A
    )

    export_to_folder_B_task = PythonOperator(
        task_id='export_to_folder_B',
        python_callable=export_to_folder_B
    )

    read_files_task >> export_to_folder_B_task
