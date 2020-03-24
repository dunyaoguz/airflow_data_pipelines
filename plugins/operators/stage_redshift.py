from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    copy_sql = """COPY {}
    FROM '{}'
    ACCESS_KEY_ID '{}'
    SECRET_ACCESS_KEY '{}'
    REGION 'us-west-1'
    FORMAT AS JSON '{}'
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 aws_credentials = "",
                 table = "",
                 s3_bucket = "",
                 copy_json_option= "",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials = aws_credentials
        self.table = table
        self.s3_bucket = s3_bucket
        self.copy_json_option = copy_json_option

    def execute(self, context):
        # define aws creds and redshift conn
        aws_hook = AwsHook(self.aws_credentials)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)

        self.log.info('Clearing data at Redshift source')
        redshift.run('DELETE FROM {}'.format(self.table))

        self.log.info('Copying data from s3 to Redshift')
        redshift.run(copy_sql.format(self.table,
                                     self.s3_bucket,
                                     credentials.access_key,
                                     credentials.secret_key,
                                     self.copy_json_option))
