from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    copy_template = """
        COPY {table}
        FROM '{s3_path}'
        ACCESS_KEY_ID '{access_key_id}'
        SECRET_ACCESS_KEY '{secret_access_key}'
        JSON 'auto'
        """

    @apply_defaults
    def __init__(self,
                 s3_bucket='',
                 s3_prefix='',
                 table='',
                 redshift_conn_id='',
                 aws_conn_id='',
                 s3_key='',
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.aws_conn_id = aws_conn_id
        self.s3_key = s3_key

    def execute(self, context):
        aws_hook = AwsHook(self.aws_conn_id)
        credentials = aws_hook.get_credentials()
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info(f'Preparing to stage data from {self.s3_bucket}/{self.s3_prefix} to {self.table} table.')
        redshift_hook.run("DELETE FROM {}".format(self.table))

        self.log.info('Copying data from S3.')
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)

        copy = StageToRedshiftOperator.copy_template.format(
            table=self.table,
            s3_path=s3_path,
            access_key_id=credentials.access_key,
            secret_access_key=credentials.secret_key)

        redshift_hook.run(copy)
        self.log.info('Complete!')





