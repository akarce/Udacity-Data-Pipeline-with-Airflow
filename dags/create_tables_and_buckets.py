import datetime
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from minio import Minio

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
}

dag = DAG(
    'create_tables_and_buckets_dag',
    default_args=default_args,
    start_date=datetime.datetime.now(),
    description='Create tables in PostgreSQL with Airflow'
)

def create_minio_buckets():
    # Access MinIO connection details from Airflow Connection
    aws_hook = AwsBaseHook("minio_conn")  # Create an AWS hook
    credentials = aws_hook.get_credentials()  # Get the credentials from the hook
    client = Minio("172.18.0.1:9000", access_key=credentials.access_key, secret_key=credentials.secret_key, secure=False)

    # Create the 'processed' bucket if it does not exist
    found_processed = client.bucket_exists("processed")
    if not found_processed:
        client.make_bucket("processed")
        print("MinIO bucket 'processed' created successfully.")

    # Create the 'udacity_dend' bucket if it does not exist
    found_udacity_dend = client.bucket_exists("udacity-dend")
    if not found_udacity_dend:
        client.make_bucket("udacity-dend")
        print("MinIO bucket 'udacity-dend' created successfully.")


create_minio_buckets_task = PythonOperator(
    task_id='create_minio_buckets',
    python_callable=create_minio_buckets,
    dag=dag,
)

create_tables_task = PostgresOperator(
    task_id='create_tables',
    postgres_conn_id='postgres_conn',
    sql='create_tables.sql',
    dag=dag
)

create_minio_buckets_task
create_tables_task
