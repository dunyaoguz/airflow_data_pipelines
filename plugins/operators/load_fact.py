from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    """Inserts data into the fact table in the OLAP schema"""

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 table = "",
                 redshift_conn_id = "",
                 insert_statement = "",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.insert_statement = insert_statement

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)

        self.log.info(f'Inserting data into the {self.table} fact table from staging')
        redshift.run(f'INSERT INTO {self.table} {self.insert_statement}')
