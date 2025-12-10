from airflow import DAG
import os
from datetime import datetime

from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.bash_operator import BashOperator

import logging
LOG = logging.getLogger(__name__)

class MyPostgresOperator(PostgresOperator):
    def execute(self, context):
        self.hook = PostgresHook(postgres_conn_id=self.postgres_conn_id,
                                 schema=self.database)
        self.hook.run(self.sql, parameters=self.parameters)


with DAG( os.path.splitext(os.path.basename(__file__))[0],
        description="Purge old entries in Airflow Database",
        schedule_interval="03 18 * * *",
        catchup=False,
        start_date=datetime(2018,1,1),
        max_active_runs=1,
        concurrency=1
    ) as dag:

    item = []
    tables = ( 'airflow_stats_to_influx', 'tem1_daq', 'tem2_daq', 'tem3_daq', 'tem4_daq', 'temalpha_daq', 'tembeta_daq', 'temgamma_daq', 'fib1_daq', 'daq_cleanup', 'experiment_cleanup', )
    for table in tables:

        MyPostgresOperator(task_id='purge_%s' % table, 
            queue='dtn',
            database="airflow",
            sql="""
            delete from xcom where dag_id = '{table}' and execution_date < NOW() - interval '1 day';
            delete from task_instance where dag_id = '{table}' and execution_date < NOW() - interval '1 day';
            delete from sla_miss where dag_id = '{table}' and execution_date < NOW() - interval '1 day';
            delete from log where dag_id = '{table}' and execution_date < NOW() - interval '1 day';
            delete from job where dag_id = '{table}' and end_date < NOW() - interval '1 day';
            delete from dag_run where dag_id = '{table}' and execution_date < NOW() - interval '1 day';
            """.format(table=table),
        )

    BashOperator( task_id='purge_log_files',
        queue='dtn',
        bash_command="""
find /usr/local/airflow/logs -mtime +7 -print -delete
""")
       
