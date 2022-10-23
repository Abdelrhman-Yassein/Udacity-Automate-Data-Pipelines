from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):
    """_summary_
       Load Fact Table Information  
    Args:
        BaseOperator (Object): To Create New Operator 
    """
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 sql_query="",
                 redshift_conn_id="",
                 aws_credentials_id="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.sql_query = sql_query
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        # Connect To Postgre On RedShift
        redshift_hook =PostgresHook(self.redshift_conn_id)
        # Run Query
        redshift_hook.run(str(self.sql_query))
        # self.log.info('LoadFactOperator not implemented yet')
