from airflow import DAG

from airflow.models import Variable

from airflow.hooks.http_hook import HttpHook

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.file_plugin import FileGlobSensor, FileInfoSensor
from airflow.operators.bash_operator import BashOperator
from airflow.operators.bashplus_plugin import BashPlusOperator
from airflow.operators.slack_operator import SlackAPIPostOperator
from airflow.operators.slack_plugin import SlackAPIUploadFileOperator
from airflow.operators.ctffind4_plugin import Ctffind4DataSensor
from airflow.operators.motioncor2_plugin import MotionCor2DataSensor
from airflow.operators.fei_epu_plugin import FeiEpuOperator
from airflow.operators.influx_plugin import FeiEpu2InfluxOperator, GenericInfluxOperator, PipelineInfluxOperator, PipelineStatsOperator, PipelineCtfOperator
from airflow.operators.cryoem_plugin import LogbookConfigurationSensor, LogbookRegisterFileOperator, LogbookRegisterRunParamsOperator, LogbookCreateRunOperator, PipelineRegisterRunOperator, PipelineRegisterFilesOperator

from airflow.exceptions import AirflowSkipException

import os
from datetime import datetime
import json

import logging
LOG = logging.getLogger(__name__)

args = {
    'owner': 'cryo-daq',
    'provide_context': True,
    'start_date': datetime( 2020,1,1 ),

    'logbook_connection_id':    'cryoem_logbook',
    'influx_host':              'influxdb.slac.stanford.edu',

    'pipeline_script': '/usr/local/airflow/pipeline-tomo.sh',
    #'pipeline_script': '/gpfs/slac/cryo/fs1/daq/prod/airflow/pipeline-tomo.sh',
    'directory_prefix': '/sdf/group/cryoem/exp',
    #'directory_prefix': '/gpfs/slac/cryo/fs1/exp',

    'apply_gainref':     True, 
    'apply_defect_file':  False,
    'daq_software':      'SerialEM',
    'max_active_runs':   50,
    # 'particle_size':     150,
    # 'create_run':         False
    # 'apix':              1.35,
    # 'fmdose':           1.75,
    # 'superres':         0,
    # 'imaging_format': '.tif',
    #'patch': '5 5 20',
}


def dummy(*args,**kwargs):
    pass


def uploadExperimentalParameters2Logbook(ds, **kwargs):
    """Push the parameter key-value pairs to the elogbook"""
    data = kwargs['ti'].xcom_pull( task_ids='parse_parameters' )
    LOG.warn("data: %s" % (data,))
    raise AirflowSkipException('not yet implemented')


class NotYetImplementedOperator(DummyOperator):
    ui_color = '#d3d3d3'


from airflow.operators.python_operator import PythonOperator, ShortCircuitOperator
def preTomogram(**kwargs):
    #mdoc = kwargs['ti'].xcom_pull( task_ids='mdoc_file' ).pop()
    log_file = kwargs['log_file']
    basename = '.'.join( os.path.basename(log_file).split('.')[:-1] ) + '.mrc'
    LOG.info(f"LOG FILE: {log_file}: {basename}")
    # check contents
    completed = False
    collected_stacks = []
    with open( log_file, 'r' ) as f:
        for l in f.readlines():
            #LOG.info(f'+ {l.strip()}')
            if ' frames were saved to ' in l:
                fp = l.split(' ').pop().split('\\').pop().strip()
                #LOG.info(f'    GOT {fp}')
                collected_stacks.append( fp )
            if l.startswith( 'Total dose in electrons' ):
            #if l.startswith( 'Focus area changed from stored position for item' ):
                completed = True
            if not l.startswith('Opened new file') and basename in l:
                completed = True
    # make sure this run is the last tilt
    this_stack = kwargs['ti'].xcom_pull( task_ids='stack_file' ).pop()
    #LOG.info(f"THIS: {this_stack}, {collected_stacks[-1]}")
    if completed and collected_stacks[-1] in this_stack:
        LOG.info("final tilt, generate tomogram")
        return True

    # skip others
    return False

class PreTomogramOperator(ShortCircuitOperator):
    """ will create directories specified if it doesn't already exist """
    #ui_color = '#006699'
    ui_color = '#b19cd9'
    #template_fields = ( 'directory', )
    def __init__(self,log_file,*args,**kwargs):
        super(PreTomogramOperator,self).__init__(python_callable=preTomogram, op_kwargs={'log_file': log_file}, *args, **kwargs)







###
# define the workflow
###
with DAG( os.path.splitext(os.path.basename(__file__))[0],
        description="Pre-processing of CryoEM data",
        schedule_interval=None,
        default_args=args,
        catchup=False,
        max_active_runs=args['max_active_runs'],
        concurrency=200,
        dagrun_timeout=2700,
    ) as dag:

    # hook to container host for lsf commands
    logbook_hook = HttpHook( http_conn_id=args['logbook_connection_id'], method='GET' )


    # parameters
    parameters = {
        'apply_gainref': args['apply_gainref'],
        'pipeline_script': args['pipeline_script'],
        'kev': args['kev'] if 'kev' in args else None,
        'cs': args['cs'] if 'cs' in args else None,
        'apix': args['apix'] if 'apix' in args else None,
        'fmdose': args['fmdose'] if 'fmdose' in args else None,
        'patch': args['patch'] if 'patch' in args else None,
        'superres': args['superres'] if 'superres' in args else None,
        'phase_plate': args['phase_plate'] if 'phase_plate' in args else None,
        'software': args['daq_software'] if 'daq_software' in args else None,
        'directory_prefix': args['directory_prefix']
    }


    stack_file = FileGlobSensor( task_id='stack_file',
        filepath="{% set imaging_format = params.imaging_format if params.imaging_format else dag_run.conf['imaging_format'] %}{{ params.directory_prefix }}/{{ dag_run.conf['data_directory'] }}/raw/**/{{ dag_run.conf['base'] }}{% if imaging_format == '.mrc' %}{% if params.daq_software == 'EPU' %}-{% else %}_{% endif %}*.mrc{% elif imaging_format == '.tif' %}*.tif{% endif %}",
        params={ 
            'daq_software': args['daq_software'],
            'imaging_format': args['imaging_format'] if 'imaging_format' in args  else None,
            'directory_prefix': args['directory_prefix'],
        },
        recursive=True,
        excludes=['gain-ref',],
        poke_interval=1,
    )
    
    ###
    stack_file
