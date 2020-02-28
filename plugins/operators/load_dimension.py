from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

# The Operarator Create and Load Dimension Table in Redshift

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 table_name="",
                 redshift_conn_id="",
                 aws_credentials_id="",
                 creation_query="",
                 data_insertion_query="",
                 append_only = False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.table = table_name
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.creation_query = creation_query
        self.data_insertion_query = data_insertion_query
        self.append_only = append_only


    def execute(self, context):
        self.log.info("Data insertion in Dimension table - {}".format(self.table))
        
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if not self.append_only:
            self.log.info("Delete existing {} Dimension table".format(self.table))
            redshift.run("DELETE FROM {}".format(self.table))
        
        self.log.info("Creating New table")
        redshift.run(self.creation_query)
        
        self.log.info("Executing data insert query")        
        redshift.run(self.data_insertion_query)
