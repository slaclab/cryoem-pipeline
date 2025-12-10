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
from airflow.operators.trigger_plugin import TriggerMultipleDagRunOperator

from airflow.operators.cryoem_plugin import LogbookConfigurationSensor, LogbookCreateRunOperator
from airflow.operators.file_plugin import RsyncOperator, ExtendedAclOperator, HasFilesOperator

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
    'tem': 'TEMDELTA',
    'source_directory': '/srv/cryoem/temdelta',
    'source_excludes':  [ '*.bin', '*.downloading' ],
    #'destination_directory': '/gpfs/slac/cryo/fs1/exp/',
    'destination_directory': '/sdf/group/cryoem/exp',
    'logbook_connection_id': 'cryoem_logbook',
    'remove_files_after': '48 hours',
    'remove_files_larger_than': '+100M',
    'data_transfer_queue': 'dtn-temdelta', #'dtn-temdelta', # airflow worker queue for data transfers
    'dry_run': False,
}


from tempfile import gettempdir, NamedTemporaryFile
from airflow.utils.file import TemporaryDirectory
from subprocess import Popen, STDOUT, PIPE, call
import redis
class RsyncDiffOperator(BaseOperator):
    template_fields = ('env','source','target','includes','dry_run','newer')
    template_ext = ( '.sh', '.bash' )
    ui_color = '#f0ede4'

    @apply_defaults
    def __init__(self, source, target, newer=None, newer_offset='20 mins', xcom_push=True, env=None, output_encoding='utf-8', prune_empty_dirs=False, includes='', excludes='', flatten=False, dry_run=False, redis_host='redis', redis_port='6379', redis_db=4, redis_password=None, redis_key='temdelta', *args, **kwargs ):
        super(RsyncDiffOperator, self).__init__(*args,**kwargs)
        self.env = env
        self.output_encoding = output_encoding

        self.source = source
        self.target = target

        self.includes = includes
        self.excludes = excludes
        self.prune_empty_dirs = prune_empty_dirs
        self.flatten = flatten
        self.dry_run = dry_run

        self.xcom_push_flag = xcom_push

        self.redis_host = redis_host
        self.redis_port = redis_port
        self.redis_db = redis_db
        self.redis_password = redis_password
        self.redis_key = redis_key

        self.newer = newer
        self.newer_offset = newer_offset
        #logging.info("NEWER " + self.newer_offset)

        self.rsync_command = ''

    def execute(self, context):

        def build_find_files( input, exclude=False ):
            out = ''
            try:
                a = input
                if isinstance(input, str):
                    a = ast.literal_eval(input)
                array = [ "%s -name '%s'" % ('!' if exclude else '', i) for i in a ]
                out = ' '.join(array)
            except:
                if input:
                    out = " %s -name '%s'" % ('!' if exclude else '', input)
            return out

        output = []
        # LOG.info("tmp dir root location: " + gettempdir())
        with TemporaryDirectory(prefix='airflowtmp') as tmp_dir:
            with NamedTemporaryFile(dir=tmp_dir, prefix=self.task_id) as f:

                find_arg = build_find_files( self.excludes, exclude=True ) + build_find_files( self.includes )
                dry = True if self.dry_run.lower() == 'true' else False

                newer = None
                if self.newer and not self.newer == "None":
                    newer = 'date -d "$(date -r ' + self.newer + ') - ' + self.newer_offset + '" +"%Y-%m-%d %H:%M:%S"'

                # format rsync command
                rsync_command = """
STATUS=255
( \
  timeout 5  ls %s >/dev/null && \
  ( find '%s/$RECYCLE.BIN/' -type f -delete 2>&1 >/dev/null || true ) \
)
if [ $? -eq 0 ]; then
  rsync -a --chmod=o-rwx --exclude='$RECYCLE.BIN'  --exclude='System Volume Information' -f'+ */' -f'- *' %s %s %s && \
  cd %s && \
  find . -type f \( %s \) %s
  STATUS=$?
  [ $STATUS -eq 0 ] && find %s -type d -empty %s
fi
exit $STATUS
                    """ % ( \
                        self.source,
                        self.source,
                        # sync directories
                        ' --dry-run' if dry else '',
                        self.source,
                        self.target,
                        # cd
                        self.source,
                        # find
                        find_arg,
                        ' -newermt "`%s`" | head -1000'%(newer,) if newer else 'head -1000',
                        # delete empty dir
                        self.target,
                        '' if dry else ' -delete',
                 )
                f.write(bytes(rsync_command, 'utf_8'))
                f.flush()
                fname = f.name
                script_location = tmp_dir + "/" + fname
                logging.info("Temporary script "
                             "location :{0}".format(script_location))

                redis_client = redis.StrictRedis( host=self.redis_host, port=self.redis_port, db=self.redis_db, password=self.redis_password )

                logging.info("Running rsync command: " + rsync_command)
                sp = Popen(
                    ['bash', fname],
                    stdout=PIPE, stderr=STDOUT,
                    cwd=tmp_dir, env=self.env)

                self.sp = sp

                logging.info(f"Copy following files to {self.target}:")
                for line in iter(sp.stdout.readline, b''):
                    line = line.decode(self.output_encoding).strip()
                    # parse for file names here
                    if line.startswith( 'building file list' ) \
                      or line.startswith( 'sent ') \
                      or line.startswith( 'total size is ' ) \
                      or line.startswith('sending incremental file list') \
                      or '/sec' in line \
                      or 'speedup is ' in line \
                      or 'created directory ' in line \
                      or line in ('', './') \
                      or ': No such file or directory' in line \
                      or ': Permission denied' in line \
                      or ': failed to set permissions ' in line \
                      or 'rsync error: ' in line \
                      or '*** ' in line:
                        continue
                    else:
                        LOG.info(line)
                        k = f'{line} -> {self.target}'
                        redis_client.rpush( self.redis_key, k )
                        output.append( line )
                sp.wait()
                logging.info("Command exited with "
                             "return code {0}".format(sp.returncode))

                if self.xcom_push_flag:
                    return [ os.path.basename(o) for o in output ]


                if not sp.returncode == 0:
                    raise AirflowException("rsync command failed")

    def on_kill(self):
        LOG.info('Sending SIGTERM signal to bash subprocess')
        self.sp.terminate()





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
        parallel=5,
        newer="{{ ti.xcom_pull(task_ids='last_rsync') }}",
        newer_offset='30 mins'
    )

    file_diff = RsyncDiffOperator( task_id='file_diff',
        queue=str(args['data_transfer_queue']),
        dry_run=str(args['dry_run']),
        source=args['source_directory']+'/',
        target="{{ ti.xcom_pull(task_ids='sample_directory') }}",
        excludes=args['source_excludes'],
        prune_empty_dirs=True,
        flatten=False,
        priority_weight=50,
        newer="{{ ti.xcom_pull(task_ids='last_rsync') }}",
        newer_offset='30 mins',
        redis_password=Variable.get('redis_password'),
        redis_key=args['tem'].lower(),
    )

    has_gain_refs = HasFilesOperator( task_id='has_gain_refs',
        queue=str(args['data_transfer_queue']),
        target="{{ ti.xcom_pull(task_ids='sample_directory') }}GainRefs/",
    )

    gain_refs = RsyncOperator( task_id='gain_refs',
        queue=str(args['data_transfer_queue']),
        dry_run=str(args['dry_run']),
        source='%s-gainrefs/' % args['source_directory'],
        target="{{ ti.xcom_pull(task_ids='sample_directory') }}GainRefs/",
        includes="*.gain",
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
            'file_glob': "\( -name 'FoilHole_*_Data_*.mrc' -o -name 'FoilHole_*_Data_*.dm4' -o -name '*.tif' -o -name '*.tiff' -o -name '*.eer' \)",
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
        trigger_rule="all_done",
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
        #channel="{{ ti.xcom_pull( task_ids='config', key='experiment' ) | replace( ' ', '' ) | lower }}",
        token=Variable.get('slack_token'),
    )
    slack_users = SlackAPIInviteToChannelOperator( task_id='slack_users',
        queue=str(args['data_transfer_queue']),
        channel="{{ ti.xcom_pull( task_ids='config', key='experiment' ) | replace( ' ', '' ) | lower }}",
        #channel="{{ ti.xcom_pull( task_ids='config', key='experiment' ) | replace( ' ', '' ) | lower }}",
        token=Variable.get('slack_token'),
        users="{{ ti.xcom_pull( task_ids='config', key='collaborators' ) }}",
        usermap_file='/usr/local/airflow/dags/slack_users.yaml',
        default_users="U07SX64GK52,W8WBPL02K,W9RUM1ET1,U03K7TYAMKM"
    )

    ###
    # clear out zombie processes after transferring files
    ###
    kill_zombies = BashOperator(task_id='kill_zombies',
        queue=str(args['data_transfer_queue']),
        bash_command = """
        ZOMBIE_THRESHOLD=500
        ZOMBIE_COUNT=`ps auxw | grep "defunct" | wc -l`
        if [ $ZOMBIE_COUNT -gt $ZOMBIE_THRESHOLD ]; then
            echo "Zombie count exceeds $ZOMBIE_THRESHOLD, killing running container..."
            kill -INT 1
        fi
        """
    )
   
    ###
    # define pipeline
    ###
    config >> sample_directory >> touch >> rsync >> delete >> untouch >> kill_zombies
    touch >> file_diff
    sample_directory >> sample_symlink
    config >> last_rsync >> rsync >> trigger
    sample_directory >> setfacl
    config >> pipeline >> trigger
    #rsync >> runs
    config >> slack_channel >> slack_users
    last_rsync >> has_gain_refs >> gain_refs
