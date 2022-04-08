from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    template = """
    CREATE TABLE IF NOT EXISTS {destination_table} AS
    {fact_table}
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 fact_table='',
                 destination_table='',
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.fact_table=fact_table
        self.destination_table=destination_table

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info(f'Loading data into {self.table} fact table')

        formatted = LoadFactOperator.template.format(
            destination_table=self.destination_table,
            fact_table=self.fact_table)


        redshift_hook.run(formatted)
        self.log.info("Loading complete.")
