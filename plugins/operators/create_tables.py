from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class CreateTablesOperator(BaseOperator):
    """Creates fact and dimension tables in the OLAP schema"""

    ui_color = '#F9CE66'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 tables = [],
                 create_statements = [],
                 *args, **kwargs):

        super(CreateTablesOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables
        self.create_statements = create_statements

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)

        for table, create_statement in zip(self.tables, self.create_statements):
            self.log.info('Clearing data at destination')
            redshift.run(f'DROP TABLE IF EXISTS {table}')

            self.log.info(f'Creating the {table} fact table on Redshift')
            redshift.run(create_statement)
