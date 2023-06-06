from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    """
    This operator performs quality checks on data in Amazon redshift RDB after ETL
    
    Parameters:
    
    redshift_conn_id : Amazon Redshift RDB credentials
    
    data_quality_check : a list of dictionaries including SQL query to be executed for the check with key query and  it's expected returned value with key expected result if other than 0.
    """

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 tables=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables

    def execute(self, context):
        redshift = PostgreHook(postgres_conn_id=self.redshift_conn_id)
        for table in self.tables:
            records = redshift.get_records(f"SELECT COUNT(*) FROM {table}")
            if len(records) < 1 or len(records[0]) < 1:
                self.log.info(f"{table} table returned no results")
                raise ValueError(f"Data quality check failed. {table} table returned no results")
            num_records = records[0][0]
            if num_records < 1:
                self.log.info(f"{table} table has 0 records")
                raise ValueError(f"Data quality check failed. {table} table has 0 records")
        
        self.log.info('DataQualityOperator not implemented yet')