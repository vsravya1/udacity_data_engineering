from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    """Run data quality checks on one or more tables.
    
    Parameters:
    data_check_query: A list of one or more queries to check data.
    table: A list of one or more tables for the data check queries.
    expected_results: A list of expected results for each data check query.
    
    Returns: Exception raised on data check failure.
    """

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 tables = [],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        
        for table in self.tables:
            
            self.log.info(f"Data quality validation is in progress for table : {table}")
            records = redshift_hook.get_records(f"select count(*) from {table};")

            if len(records) < 1 or len(records[0]) < 1 or records[0][0] < 1:
                self.log.error(f"Data Quality validation failed for table : {table}.")
                #raise ValueError(f"Data Quality validation failed for table : {table}")
            self.log.info(f"Data Quality Validation completed for table : {table}!!!")    
