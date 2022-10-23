from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):
    """_summary_
        Create New Operator To Check Data Quality From Table After Insert
    Args:
        BaseOperator (Object): To Create To operator

    Raises:
        ValueError: Error Value
    """
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 aws_credentials_id="",  # connection we created in airflow ui admin connection
                 redshift_conn_id="",  # connection id come from connection in airflow ui
                 tables={},  # DataBase tables
                 # Example:
                 # conn_id = your-connection-name
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables
        self.redshift_conn_id = redshift_conn_id


    def execute(self, context):
        """_summary_
            Data quality check for all tables 
            by count rows in every table
        """
        # Connect to Postgres by postgres hook
        redshift_hook = PostgresHook(self.redshift_conn_id)

        for table in self.tables:
            # Run Query To Check Tables Have Rows
            records = (redshift_hook
                       .get_records(f"SELECT COUNT(*) FROM {table}"))
            # Check If Records Have Values
            if len(records) < 1 or len(records[0]) < 1 :
                # If Table Not Have Any Value
                (self
                 .log
                 .error(f"Data quality check failed. {table} returned no results"))

                raise ValueError(
                    f"Data quality check failed. {table} returned no results")

            # If Table Have Data And Present Rows Number
            (self
             .log
             .info(f"Data quality on table {table} check passed with {records[0][0]} records"))

            # self.log.info('DataQualityOperator not implemented yet')
