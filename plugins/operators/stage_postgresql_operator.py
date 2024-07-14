from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from minio import Minio
from minio.commonconfig import CopySource
import json

class StageToPostgresOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ("s3_key",)

    @apply_defaults
    def __init__(self, table="", s3_bucket="", s3_key="", s3_prefix="", destination_bucket="", aws_credentials_id="", postgres_conn_id="", *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.s3_prefix = s3_prefix
        self.destination_bucket = destination_bucket
        self.aws_credentials_id = aws_credentials_id
        self.postgres_conn_id = postgres_conn_id

    def execute(self, context):
        execution_date = context['ds']
        formatted_s3_key = self.s3_key.format(execution_date) if self.s3_key else None

        aws_hook = AwsBaseHook(self.aws_credentials_id) # Create an AWS hook
        credentials = aws_hook.get_credentials() # Get the credentials from the hook

        client = Minio("172.18.0.1:9000", access_key=credentials.access_key, secret_key=credentials.secret_key, secure=False)

        objects = client.list_objects(self.s3_bucket, prefix=self.s3_prefix, recursive=True) if self.s3_prefix else [client.stat_object(self.s3_bucket, formatted_s3_key)]

        postgres_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        conn = postgres_hook.get_conn()     
        cur = conn.cursor()     

        for obj in objects:
            object_name = obj.object_name if hasattr(obj, 'object_name') else formatted_s3_key
            # Read data from S3
            data = client.get_object(self.s3_bucket, object_name)
            data_string = data.data.decode('utf-8')  # Assuming data is bytes and needs decoding

            # Split the data string by newline and load each line as a JSON object
            data_json_list = [json.loads(line) for line in data_string.split('\n') if line.strip()]

            self.log.info(f"Adding {len(data_json_list)} entries to postgres table '{self.table}'")

            ### Insert data into PostgreSQL
            for entry in data_json_list:
                columns = entry.keys()
                values = [entry[column] if entry[column] not in [None, ""] else None for column in columns]

                # Create a query string to insert the data
                query = f"INSERT INTO {self.table} ({', '.join(columns)}) VALUES ({', '.join(['%s' for _ in columns])})"

                cur.execute(query, values)

            conn.commit()

            # Move the processed file to the destination bucket
            source_object = obj.object_name
            dest_object = obj.object_name

            # Use CopySource to specify the source object
            copy_source = CopySource(self.s3_bucket, source_object)

            client.copy_object(self.destination_bucket, dest_object, copy_source)
            client.remove_object(self.s3_bucket, obj.object_name)
