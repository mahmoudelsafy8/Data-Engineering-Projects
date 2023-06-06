from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    
    """
    This operator loading fact tables into Amazod redshift RDB
    Parameters:
    redshift_conn_id : str
                       Amazon Redshift RDB credentials
    table : str
            table name which data insert into
    sql : str
          the sql query that will be used to insert the fact table
    """

    ui_color = '#F98866'
    insert_sql = """
          INSERT INTO {table}
          {sql_query};
    """
    

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql_query="",
                 turncate= False,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_query = sql_query
        self.turncate = turncate
        
        
    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if self.delete_first:
            self.log.info(f"Deleting data from {self.table} fact table")
            redshift.run("DELETE FROM {}".format(self.table))
            
        self.log.info(f"Inserting data from staging tables into {self.table} fact table")
        redshift.run(LoadFactOperator.insert_sql.format(
            table=self.table,
            sql_query=self.sql_query
        ))
        self.log.info('LoadFactOperator not implemented yet')
        
        
