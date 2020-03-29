from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    """Inserts data into dimension tables in the OLAP schema"""

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 table = "",
                 redshift_conn_id = "",
                 insert_statement = "",
                 truncate_insert = True,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.insert_statement = insert_statement
        self.truncate_insert = truncate_insert

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)

        if truncate_insert:

            self.log.info(f'Emplying {self.table} dimension table')
            redshift.run(f'TRUNCATE TABLE {self.table}')

        self.log.info(f'Inserting data into the {self.table} dimension table from staging')
        redshift.run(f'INSERT INTO {self.table} {self.insert_statement}')
