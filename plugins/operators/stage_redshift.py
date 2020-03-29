from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    """Extracts JSON data from S3 and loads it onto staging tables on Redshift"""

    ui_color = '#358140'

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

        self.log.info('Copying data from s3 to Redshift')
        redshift.run("""COPY {}
                        FROM '{}'
                        ACCESS_KEY_ID '{}'
                        SECRET_ACCESS_KEY '{}'
                        REGION 'us-west-2'
                        FORMAT AS JSON '{}'""".format(self.table,
                                                      self.s3_bucket,
                                                      credentials.access_key,
                                                      credentials.secret_key,
                                                      self.copy_json_option))
