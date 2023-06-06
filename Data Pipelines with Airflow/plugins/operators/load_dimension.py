from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    """
    This operator loads dimension tables into amazon redshift RDB
    
    Parameters
    
    redshift_conn_id : Amazon redshift RDB credentials
    
    table : table name which data insert into
    
    sql : the sql query that will be used to insert the fact table
    
    turncate : flag indicats turncating inserted dimensions
    """

    ui_color = '#80BD9E'
    insert_sql = """
        INSERT INTO {table}
        {sql_query};
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql_query="",
                 turncate = False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_query = sql_query
        self.turncate = turncate

    def execute(self, context):
        redshift = PostgreHook(postgres_conn_id=self.redshift_conn_id)
        
        if self.delete_first:
            self.log.info(f"Deleting data from {self.table} dimension table")
            redshift.run("DELETE FROM {}".format(self.table))
        
        self.log.info(f"Inserting data from fact table into {self.table} dimension table")
        redshifr.run(LoadDimensionOperator.insert_sql.format(
            table=self.table,
            sql_query=self.sql_query
        ))
        self.log.info('LoadDimensionOperator not implemented yet')
