from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 table_names=[],
                 column='',
                 quality_checks=[],
                 redshift_conn_id='',
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.table_names = table_names
        self.column = column
        self.redshift_conn_id = redshift_conn_id
        self.quality_checks: list = quality_checks

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        for table in self.table_names:
            nonformatted = self.quality_checks[0]['check_sql'].format(table)
            formatted_sql = nonformatted.format(table)
            records = redshift_hook.get_records(formatted_sql) 

            if (len(records) < self.quality_checks[0]['fail_result'] or len(records[0]) < self.quality_checks[0]['fail_result']):
                self.log.info("Data quality check failed. {} returned no results".format(table))

        self.log.info("Data Quality check is completed!")