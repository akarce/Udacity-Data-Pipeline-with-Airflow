from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 postgres_conn_id="",
                 table="",
                 sql_stmt="",
                 append=False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)

        self.postgres_conn_id = postgres_conn_id
        self.table = table
        self.sql_stmt = sql_stmt
        self.append = append

    def execute(self, context):
        postgres = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        self.log.info(f"Loading dimension table {self.table}")
        
        if self.append:
            sql = f"""
                BEGIN;
                {self.sql_stmt};
                COMMIT;
            """
        else:
            sql = f"""
                BEGIN;
                TRUNCATE TABLE {self.table};
                {self.sql_stmt};
                COMMIT;
            """

        postgres.run(sql)
