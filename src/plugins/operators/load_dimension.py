from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 postgres_conn_id,
                 table,
                 sql,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.postgres_conn_id=postgres_conn_id
        self.table=table
        self.sql=sql

    def execute(self, context):
        self.log.inf("Connecting to Redshift")
        redshift = PostgresHook(self.redshift_conn_id)
        
        self.log.info(f"Inserting data into {self.table} table on Redshift")
        redshift.run(f"INSERT INTO {self.table} {self.sql}")
