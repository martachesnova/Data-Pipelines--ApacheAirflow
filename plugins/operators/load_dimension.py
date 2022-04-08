from re import template
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 dim_table='',
                 destination_table='',
                 append_records=True,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.destination_table=destination_table
        self.dim_table_query=dim_table
        self.append_records=append_records

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if append_records:
            template = """
            INSERT INTO TABLE {destination_table} AS
            {dim_table}
            """
        else:
            template = """
            TRUNCATE TABLE {destination_table};
            INSERT INTO TABLE {destination_table} AS
            {dim_table}
            """
            self.log.info("Deleting records from existing Redshift table")

        # Format the SQL template
        formatted = LoadDimensionOperator.template.format(
            destination_table=self.destination_table,
            dim_table=self.dim_table
        )

        # run the query against redshift
        redshift_hook.run(formatted)
        self.log.info("Loading dimension table...")
