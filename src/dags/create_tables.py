from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class CreateTablesOperator(BaseOperator):
    ui_color = '#358140'
    
    template_fields = ("s3_key",)
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 *args, **kwargs):


        super(CreateTablesOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info(f"Creating SQL tables in Redshift")
        redshift.run()