'''
This DAG listens for files that should be dropped onto a mountpoint that is accessible to the airflow workers. It will then stage the file to another location for permanent storage. After this, it will trigger a few other DAGs in order to complete processing.
'''

from airflow import DAG

from airflow import settings
import contextlib
from airflow.utils.decorators import apply_defaults
from airflow.models import Variable

from airflow.operators.dummy_operator import DummyOperator

from airflow.models import BaseOperator
from airflow.operators.python_operator import PythonOperator, ShortCircuitOperator
from airflow.operators.bash_operator import BashOperator

from airflow.operators.dagrun_operator import TriggerDagRunOperator, DagRunOrder

from airflow.operators.cryoem_plugin import LogbookConfigurationSensor, LogbookCreateRunOperator
from airflow.operators.file_plugin import RsyncOperator, ExtendedAclOperator, HasFilesOperator

from airflow.operators.trigger_plugin import TriggerMultipleDagRunOperator
from airflow.operators.slack_plugin import SlackAPIEnsureChannelOperator, SlackAPIInviteToChannelOperator

from airflow.exceptions import AirflowException, AirflowSkipException

from airflow.hooks.http_hook import HttpHook

from airflow.models import DagRun, DagBag
from airflow.utils.state import State

from pathlib import Path
from datetime import datetime
from time import sleep
import re
import os

import logging
LOG = logging.getLogger(__name__)

###
# default args
###
args = {
    'owner': 'cryo-daq',
    'provide_context': True,
    'start_date': datetime(2020,1,1), 
    'tem': 'TEM2',
    'source_directory': '/srv/cryoem/tem2',
    'source_excludes':  [ '*.bin', 'cifs48*' ],
    'destination_directory': '/sdf/group/cryoem/exp', #gpfs/slac/cryo/fs1/exp/',
    #'destination_directory': '/gpfs/slac/cryo/fs1/exp/',
    'logbook_connection_id': 'cryoem_logbook',
    'remove_files_after': '6 hours',
    'remove_files_larger_than': '+100M',
    'data_transfer_queue': 'dtn-tem2', # airflow worker queue for data transfers
    'dry_run': False,
}



with DAG( os.path.splitext(os.path.basename(__file__))[0],
        description="Stream data off the TEMs based on the CryoEM logbook",
        schedule_interval="* * * * *",
        default_args=args,
        catchup=False,
        concurrency=4,
        max_active_runs=1
    ) as dag:
   
   
   
    logbook_hook = HttpHook( http_conn_id=args['logbook_connection_id'], method='GET' )
   
    ###
    # read and pase the configuration file for the experiment
    ###
    config = LogbookConfigurationSensor(task_id='config',
        queue=str(args['data_transfer_queue']),
        http_hook=logbook_hook,
        microscope=args['tem'],
        base_directory=args['destination_directory'],
        ignore_older_than=60,
    )

    ###
    # make sure we have a directory to write to; also create the symlink
    ###
    sample_directory = BashOperator( task_id='sample_directory',
        queue=str(args['data_transfer_queue']),
        bash_command = "mkdir -p %s/{{ ti.xcom_pull(task_ids='config',key='directory_suffix') }}/{{ ti.xcom_pull(task_ids='config',key='sample')['guid'] }}/raw/ && chmod o-rwx %s/{{ ti.xcom_pull(task_ids='config',key='directory_suffix') }} && echo %s/{{ ti.xcom_pull(task_ids='config',key='directory_suffix') }}/{{ ti.xcom_pull(task_ids='config',key='sample')['guid'] }}/raw/" % (args['destination_directory'],args['destination_directory'],args['destination_directory']),
        xcom_push=True
    )
    
    sample_symlink = BashOperator( task_id='sample_symlink',
        queue=str(args['data_transfer_queue']),
        bash_command = """
        cd %s/{{ ti.xcom_pull(task_ids='config',key='directory_suffix') }}/
        if [ ! -L {{ ti.xcom_pull(task_ids='config',key='sample')['name'] }}  ]; then
            ln -s {{ ti.xcom_pull(task_ids='config',key='sample')['guid'] }} {{ ti.xcom_pull(task_ids='config',key='sample')['name'] }} 
        fi
        """ % args['destination_directory'],
    )

    setfacl = ExtendedAclOperator( task_id='setfacl',
        queue=str(args['data_transfer_queue']),
        directory="%s/{{ ti.xcom_pull(task_ids='config',key='directory_suffix') }}" % args['destination_directory'],
        users="{{ ti.xcom_pull(task_ids='config',key='collaborators') }}",
    )



    last_rsync = BashOperator( task_id='last_rsync',
        queue=str(args['data_transfer_queue']),
        bash_command="""
        if [ ! -d {{ params.directory }} ]; then
          mkdir {{ params.directory }}
        fi
        cd {{ params.directory }}
        ls -1 -t -A {{ params.prefix }}*  2>/dev/null | tail -n1 | xargs -n1 --no-run-if-empty readlink -e 
        """,
        params={
            'directory': args['destination_directory'] + '/.daq/',
            'prefix': args['tem'] + '_sync_'
        },
        xcom_push=True,
    )

    ###
    # use this file as an anchor for which files to care about
    ###
    touch = BashOperator( task_id='touch',
        queue=str(args['data_transfer_queue']),
        bash_command="""
        TS=$(date '+%Y%m%d_%H%M%S')
        [ -f "{{ params.source }}/.online" ] && [ -d "{{ params.directory }}/../" ] && mkdir -p {{ params.directory }} && cd {{ params.directory }} && touch {{ params.prefix }}${TS}
        """,
        params={
            'directory': args['destination_directory'] + '/.daq/',
            'prefix': args['tem'] + '_sync_',
            'source': args['source_directory']
        }
    )

    ###
    # rsync the globbed files over and store on target without hierachy
    ###
    rsync = RsyncOperator( task_id='rsync',
        queue=str(args['data_transfer_queue']),
        dry_run=str(args['dry_run']),
        source=args['source_directory']+'/',
        target="{{ ti.xcom_pull(task_ids='sample_directory') }}",
        excludes=args['source_excludes'],
        prune_empty_dirs=True,
        chmod='ug+x,u+rw,g+r,g-w,o-rwx',
        flatten=False,
        priority_weight=50,
        newer="{{ ti.xcom_pull(task_ids='last_rsync') }}"
    )

    has_gain_refs = HasFilesOperator( task_id='has_gain_refs',
        queue=str(args['data_transfer_queue']),
        target="{{ ti.xcom_pull(task_ids='sample_directory') }}/GainRefs/",
    )

    gain_refs = RsyncOperator( task_id='gain_refs',
        queue=str(args['data_transfer_queue']),
        dry_run=str(args['dry_run']),
        source='%s-gainrefs/' % args['source_directory'],
        target="{{ ti.xcom_pull(task_ids='sample_directory') }}/GainRefs/",
        includes="*.dm4",
        chmod='ug+x,u+rw,g+r,g-w,o-rwx',
        flatten=False,
        priority_weight=50,
    )

    untouch = BashOperator( task_id='untouch',
        queue=str(args['data_transfer_queue']),
        bash_command="""
        cd {{ params.directory }}
        if [ `ls -1 -t -A {{ params.prefix }}* | wc -l` -gt 1 ]; then
          ls -1 -t -A {{ params.prefix }}* | tail -n +2 | xargs -t rm -f
        fi
        """,
        params={
            'directory': args['destination_directory'] + '/.daq/',
            'prefix': args['tem'] + '_sync_'
        }
    )

    ###
    # delete files large file over a certain amount of time
    ###
    # newer = 'date -d "$(date -r ' + self.newer + ') - ' + self.newer_offset + '" +"%Y-%m-%d %H:%M:%S"'
    delete = BashOperator( task_id='delete',
        queue=str(args['data_transfer_queue']),
        bash_command="""
            find {{ params.source_directory }} {{ params.file_glob }} -type f ! -newermt "`date -d "$(date -r {{ ti.xcom_pull(task_ids='last_rsync') }}) -  {{ params.age }}" +"%Y-%m-%d %H:%M:%S"`" -size {{ params.size }} -print {% if not params.dry_run %}-delete{% endif %} | grep -v "Permission denied" | true
        """,
        params={
            'source_directory': args['source_directory'],
            'file_glob': "\( -name 'FoilHole_*_Data_*.mrc' -o -name 'FoilHole_*_Data_*.dm4' -o -name '*.tif' -o -name '*.tiff' \)",
            'age': args['remove_files_after'],
            'size': args['remove_files_larger_than'],
            'dry_run': False
        }
    )


    pipeline = BashOperator( task_id='pipeline',
        queue=str(args['data_transfer_queue']),
        bash_command="""
        export DAG=\"{{ ti.xcom_pull(task_ids='config',key='experiment') }}_{{ ti.xcom_pull(task_ids='config',key='sample')['guid'] }}\"
        if [ `airflow list_dags | grep ${DAG} | wc -l` -eq 0 ]; then
            echo copying ${DAG}
            # {{ ti.xcom_pull(task_ids='config',key='sample')['params']['imaging_method'] }}
            cp /usr/local/airflow/dags/pipeline_{{ ti.xcom_pull(task_ids='config',key='sample')['params']['imaging_method'] | lower }}_pre-processing.py /usr/local/airflow/dags/${DAG}.py
            sed -i \"s/__imaging_software__/{{ ti.xcom_pull(task_ids='config',key='sample')['params']['imaging_software'] }}/g\" /usr/local/airflow/dags/${DAG}.py

            # wait until dag is registered
            echo unpause...
            while [ `airflow unpause ${DAG} | grep \", paused: False\" | wc -l` -eq 0 ]; do
                echo waiting...
                sleep 60
            done

        else
            echo ${DAG} registered
        fi
        """
    )



    #
    ###
    # trigger another daq to handle the rest of the pipeline
    ###
    trigger = TriggerMultipleDagRunOperator( task_id='trigger',
        queue=str(args['data_transfer_queue']),
        trigger_dag_id="{{ ti.xcom_pull( task_ids='config', key='experiment' ) }}_{{ ti.xcom_pull( task_ids='config', key='sample' )['guid'] }}",
        dry_run=str(args['dry_run']),
    )

    ###
    # register the runs into the logbook
    ###
    #runs = LogbookCreateRunOperator( task_id='runs',
    #   http_hook=logbook_hook,
    #   experiment="{{ ti.xcom_pull( task_ids='config', key='experiment' ).split('_')[0] }}",
    #   retries=3, 
    #) 

    slack_channel = SlackAPIEnsureChannelOperator( task_id='slack_channel',
        queue=str(args['data_transfer_queue']),
        channel="{{ ti.xcom_pull( task_ids='config', key='experiment' ) | replace( ' ', '' ) | lower }}",
        token=Variable.get('slack_token'),
    )
    slack_users = SlackAPIInviteToChannelOperator( task_id='slack_users',
        queue=str(args['data_transfer_queue']),
        channel="{{ ti.xcom_pull( task_ids='config', key='experiment' ) | replace( ' ', '' ) | lower }}",
        token=Variable.get('slack_token'),
        users="{{ ti.xcom_pull( task_ids='config', key='collaborators' ) }}",
        usermap_file='/usr/local/airflow/dags/slack_users.yaml',
        default_users="W016RSAHFTN,W8WBPL02K,W9RUM1ET1,U03K7TYAMKM,W01AN7SMBR9"
    )

    ###
    # define pipeline
    ###
    config >> sample_directory >> touch >> rsync >> delete >> untouch
    sample_directory >> sample_symlink
    config >> last_rsync >> rsync >> trigger
    sample_directory >> setfacl
    config >> pipeline >> trigger
    #rsync >> runs
    config >> slack_channel >> slack_users
    last_rsync >> has_gain_refs >> gain_refs
