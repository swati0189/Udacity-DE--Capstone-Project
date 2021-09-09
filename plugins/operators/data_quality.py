from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):
    """
    DAG operator used for data quality checks.
   
    """

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 checks,
                 *args,
                 **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.checks = checks

    def execute(self, context):
        self.log.info('DataQualityOperator begin execute')
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info(f"Connected with {self.redshift_conn_id}")

        failed_tests = []
        for check in self.checks:
            sql = check.get('check_sql')
            if sql:
                exp_result = check.get('expected_result')
                descr = check.get('descr')
                self.log.info(f"...[{exp_result}/{descr}] {sql}")

                result = redshift_hook.get_records(sql)[0]

                if exp_result != result[0]:
                    failed_tests.append(
                        f"{descr}, expected {exp_result} got {result[0]}\n  "
                        "{sql}")
 
        if len(failed_tests) > 0:
            self.log.info('Tests failed')
            self.log.info(failed_tests)
            raise ValueError('Data quality check failed')

        self.log.info("DataQualityOperator complete")
