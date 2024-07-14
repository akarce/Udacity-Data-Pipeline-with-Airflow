from datetime import datetime
from airflow import DAG
from helpers.sql_queries import SqlQueries
from operators.stage_postgresql_operator import StageToPostgresOperator
from operators.load_fact_operator import LoadFactOperator
from airflow.utils.task_group import TaskGroup
from operators.load_dim_operator import LoadDimensionOperator
from operators.data_quality_operator import DataQualityOperator


default_args = {
    'owner': 'cey',
    'start_date': datetime(2018, 11, 1),
    'end_date': datetime(2018, 11, 30)
}

dag = DAG('pipeline',
          default_args=default_args,
          schedule_interval='@daily'
)

stage_songs_to_postgres = StageToPostgresOperator(
    task_id='stage_songs_task',
    dag=dag,
    aws_credentials_id="minio_conn",
    postgres_conn_id="postgres_conn",  # Connection ID for PostgreSQL
    table="staging_songs",  # Table name in PostgreSQL
    s3_bucket="udacity-dend",
    s3_prefix="song_data",
    destination_bucket="processed",
    depends_on_past=True  # Wait for the previous DAG run to complete
)

stage_logs_to_postgres = StageToPostgresOperator(
    task_id='stage_logs_task',
    dag=dag,
    aws_credentials_id="minio_conn",
    postgres_conn_id="postgres_conn",  # Connection ID for PostgreSQL
    table="staging_events",  # Table name for logs in PostgreSQL
    s3_bucket="udacity-dend",
    s3_key="log_data/2018/11/{}-events.json",
    destination_bucket="processed",
    depends_on_past=True  # Wait for the previous DAG run to complete
)


load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    postgres_conn_id="postgres_conn",
    table='songplays',
    sql_stmt=SqlQueries.songplays_table_insert
)


def load_dimension_table(dag, table, sql_stmt, postgres_conn_id):
    load_dimension_task = LoadDimensionOperator(
        task_id=f'load_{table}_dim_table',
        dag=dag,
        postgres_conn_id=postgres_conn_id,
        table=table,
        sql_stmt=sql_stmt
    )
    return load_dimension_task

tables_to_load = ['users', 'times', 'songs', 'artists']

with TaskGroup(group_id='load_dimension_tables', dag=dag) as load_dimension_tables_group:
    tasks = []
    for table in tables_to_load:
        task = load_dimension_table(
            dag,
            table,
            getattr(SqlQueries, f'{table}_table_insert'),
            "postgres_conn"  # Replace with your actual connection ID
        )
        tasks.append(task)


run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    postgres_conn_id="postgres_conn",
    tables=['songplays', 'users', 'songs', 'artists', 'times']
)

stage_logs_to_postgres >> load_songplays_table
stage_songs_to_postgres>> load_songplays_table

load_songplays_table >> load_dimension_tables_group

load_dimension_tables_group >> run_quality_checks



