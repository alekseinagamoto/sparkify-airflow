from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 postgres_conn_id,
                 table,
                 sql,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.postgres_conn_id=postgres_conn_id
        self.table=table
        self.sql=sql

    def execute(self, context):
        self.log.inf("Connecting to Redshift")
        redshift_hook = PostgresHook(self.redshift_conn_id)
        
        self.log.info(f"Inserting data into {self.table} table on Redshift")
        redshift.run(f"INSERT INTO {self.table} {self.sql}")
