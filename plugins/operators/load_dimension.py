from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 table="",
                 truncat="",
                 sql_query="",
                 redshift_conn_id="",
                 aws_credentials_id="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.sql_query = sql_query
        self.table = table
        self.truncat = truncat
        self.aws_credentials_id = aws_credentials_id

    def execute(self, context):
        # Connect To readShift To Run Query
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        # Truncate Table 
        if self.truncat:
            redshift.run(f"TRUNCATE TABLE {self.table}") #Truncate Table Query
        # SQL Query        
        formatted_sql = self.sql_query.format(self.table)
        redshift.run(formatted_sql) #Run Sql
        self.log.info(f"Success: {self.task_id}")
        # self.log.info('LoadDimensionOperator not implemented yet')
