from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    """
    This operator loading data for s3 bucket in thier JSON and transform the into columns form, and load it to Amazon Redshift RDB
    
    Parameters:
    
    redshift_conn_id : Amazon Redshift RDB credentials
    
    aws_credentials_id : IAM role credentials
    
    table : table name which data insert into
    
    s3_bucket : name of the S3 bucket
    
    s3_key : name of the subdiractory S3 bucket where is the target data
    
    region : name of the region in Amazon redshift server
    
    json_path : path where the JSON file from S3 is loaded
    """
    
    ui_color = '#358140'
    
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        REGION "{}'
        TIMEFORMAT as 'epochmillisecs'
        TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
        FORMAT AS JSON '{}';
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 region="",
                 json_path="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.region = region
        self.json_path = json_path
       

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info(f"Deleting data from {self.table} Redshift table")
        redshift.run("DELETE FROM {}".format(self.table))
        
        self.log.info(f"Copying data from S3 to {self.table} Redshift table")
        s3_path = f"s3://{self.s3_bucket}/{self.s3_key}"
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.region,
            self.json_path
        )
        redshift.run(formatted_sql)
        self.log.info(f"StageToRedshiftOperator not implemented yet")





