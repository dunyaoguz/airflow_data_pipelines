from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 tables_list = [],
                 redshift_conn_id = "",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.tables_list = tables_list
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)

        self.log.info('Running a check greater than zero test')

        results = []
        for table in self.tables_list:
            result = redshift.get_records(f'SELECT COUNT(*) FROM {table}')
            print(result)
            results.append(result)

        self.log.info('Running a null value check')
