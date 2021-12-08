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

from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from bashplus_operators import BashPlusOperator
from cryoem_operators import LogbookConfigurationSensor, LogbookCreateRunOperator
from file_operators import RsyncOperator, ExtendedAclOperator, HasFilesOperator
from slack_operators import SlackAPIEnsureChannelOperator, SlackAPIInviteToChannelOperator
from trigger_operators import TriggerMultiDagRunOperator

from airflow.providers.slack.operators.slack import SlackAPIOperator, SlackAPIPostOperator
from slackclient import SlackClient

from airflow.exceptions import AirflowException, AirflowSkipException

from airflow.hooks.http_hook import HttpHook

from airflow.models import DagRun, DagBag
from airflow.utils.state import State

from pathlib import Path
from datetime import datetime
from time import sleep
import re
import glob
import os
import subprocess

import logging
LOG = logging.getLogger(__name__)

###
# default args
###
args = {
    'owner': 'cryo-daq',
    'provide_context': True,
    'start_date': datetime(2020,1,1), 
    'tem': 'FIB1',
    'source_directory': '/srv/cryoem/fib1',
    'source_excludes':  [ '*.bin', 'cifs48*' ],
    'destination_directory': '/sdf/group/cryoem/exp', 
    #'destination_directory': '/gpfs/slac/cryo/fs1/exp/',
    'logbook_connection_id': 'cryoem_logbook',
    'remove_files_after': '8 hours',
    'remove_files_larger_than': '+100M',
    'data_transfer_queue': 'dtn', # airflow worker queue for data transfers
    'dry_run': False,
}





class SlackAPIMultiUploadFileOperator(SlackAPIOperator):
    template_fields = ('channel',)
    ui_color = '#FFBA40'

    @apply_defaults
    def __init__(self,
                 channel='#general',
                 xcom='previews',
                 *args, **kwargs):
        self.method = 'files.upload'
        self.channel = channel
        self.xcom = xcom
        super(SlackAPIMultiUploadFileOperator, self).__init__(method=self.method,
                                                   *args, **kwargs)

    def construct_api_call_params(self, filepath ):
        title = '.'.join( os.path.basename(filepath).split('.')[:-1] )
        return {
            'channels': self.channel,
            'filename': title,
            'title': title,
        }

    def execute(self, context, **kwargs):
        sc = SlackClient(self.token)
        filepaths = context['ti'].xcom_pull( task_ids=self.xcom ) 
        LOG.info( f"FILEPATHS: {filepaths}" )
        for filepath in filepaths:
            params = self.construct_api_call_params(filepath)
            with open( filepath, 'rb' ) as f:
                params['file'] = f
                rc = sc.api_call(self.method, **params)
                logging.info("sending: %s" % (params,))
                if not rc['ok']:
                    logging.error("Slack API call failed {}".format(rc['error']))
                    raise AirflowException("Slack API call failed: {}".format(rc['error']))




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
        do_xcom_push=True
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
        do_xcom_push=True,
    )

    ###
    # use this file as an anchor for which files to care about
    ###
    touch = BashOperator( task_id='touch',
        queue=str(args['data_transfer_queue']),
        bash_command="""
        [ -d "{{ params.directory }}/../" ] && mkdir -p {{ params.directory }}
        TS=$(date '+%Y%m%d_%H%M%S')
        cd {{ params.directory }} && touch {{ params.prefix }}${TS}
        """,
        params={
            'directory': args['destination_directory'] + '/.daq/',
            'prefix': args['tem'] + '_sync_'
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
            'file_glob': "\( -name 'FoilHole_*_Data_*.mrc' -o -name 'FoilHole_*_Data_*.dm4' -o -name '*.tif' \)",
            'age': args['remove_files_after'],
            'size': args['remove_files_larger_than'],
            'dry_run': False
        }
    )

    def previews_operator(**kwargs):
      dir = kwargs['ti'].xcom_pull(task_ids='sample_directory')
      filepaths = []
      for f in kwargs['ti'].xcom_pull(task_ids='rsync'):
        these = glob.glob( f'{dir}/**/{f}', recursive=True )
        #LOG.info(f"found: {these}")
        filepaths = filepaths + these
      jpgs = []
      for f in filepaths:
        skip = True
        for ex in ( '.tif', '.tiff', '.jpg', '.png' ):
          if ex in f:
            skip = False
        if not skip:
          jpg = '.'.join( f.split('.')[:-1] ) + '.jpg'
          command = f"convert {f} {jpg}"
          LOG.info( f'Running {command}' )
          subprocess.call( command.split() )
          jpgs.append( jpg )
      if len(jpgs) == 0:
      #if len(filepaths) == 0:
        raise AirflowSkipException("No data")
      kwargs['ti'].do_xcom_push(key='return_value', value=jpgs)
      #kwargs['ti'].do_xcom_push(key='return_value', value=filepaths)

    previews = PythonOperator( task_id='previews',
        queue='cpu',
        provide_context=True,
        python_callable=previews_operator,
    )



    #
    ###
    # trigger another daq to handle the rest of the pipeline
    ###
    #trigger = TriggerMultiDagRunOperator( task_id='trigger',
    #    queue=str(args['data_transfer_queue']),
    #    trigger_dag_id="{{ ti.xcom_pull( task_ids='config', key='experiment' ) }}_{{ ti.xcom_pull( task_ids='config', key='sample' )['guid'] }}",
    #    dry_run=str(args['dry_run']),
    #)

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
        channel="{{ ti.xcom_pull( task_ids='config', key='experiment' )[:21] | replace( ' ', '' ) | lower }}",
        token=Variable.get('slack_token'),
    )
    slack_users = SlackAPIInviteToChannelOperator( task_id='slack_users',
        queue=str(args['data_transfer_queue']),
        channel="{{ ti.xcom_pull( task_ids='config', key='experiment' )[:21] | replace( ' ', '' ) | lower }}",
        token=Variable.get('slack_token'),
        users="{{ ti.xcom_pull( task_ids='config', key='collaborators' ) }}",
        usermap_file='/usr/local/airflow/dags/slack_users.yaml',
        default_users="W018287MVEK,W9RUM1ET1,WMLGZAGVD",
    )
    slack_previews = SlackAPIMultiUploadFileOperator( task_id='slack_previews',
        channel="{{ ti.xcom_pull( task_ids='config', key='experiment' )[:21] | replace( ' ', '' ) | lower }}",
        token=Variable.get('slack_' + os.path.splitext(os.path.basename(__file__))[0].split('_')[0].lower() ),
        xcom="previews",
    )

    ###
    # define pipeline
    ###
    config >> sample_directory >> touch >> rsync >> delete >> untouch
    sample_directory >> sample_symlink
    config >> last_rsync >> rsync # >> trigger
    sample_directory >> setfacl
    #config >> pipeline >> trigger
    
    #rsync >> runs
    config >> slack_channel >> slack_users
    slack_channel >> previews
    rsync >> previews
    previews >> slack_previews
