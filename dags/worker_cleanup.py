from airflow import DAG
import os
from airflow.operators.bash_operator import BashOperator
from datetime import datetime

with DAG( os.path.splitext(os.path.basename(__file__))[0],
        description="Seppuku workers",
        schedule_interval="6 0,6,12,18 * * *",
        catchup=False,
        start_date=datetime(2018,1,1),
        max_active_runs=1,
        concurrency=1
    ) as dag:
    
    seppuku = BashOperator( task_id='seppuku',
        bash_command="kill -TERM 1"
    )
