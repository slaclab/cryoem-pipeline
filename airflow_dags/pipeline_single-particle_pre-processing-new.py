from airflow import DAG

from airflow.models import Variable

from airflow.models import BaseOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.sensors import BaseSensorOperator

from airflow.contrib.hooks import SSHHook
from airflow.hooks.http_hook import HttpHook

from airflow.utils.decorators import apply_defaults
from airflow.operators import FileGlobSensor, FileInfoSensor
from airflow.operators import LSFSubmitOperator, LSFJobSensor, LSFOperator, BaseSSHOperator

from airflow.operators.slack_operator import SlackAPIPostOperator
from airflow.operators import SlackAPIUploadFileOperator
from airflow.operators import Ctffind4DataSensor
from airflow.operators import MotionCor2DataSensor

from airflow.operators import FeiEpuOperator
from airflow.operators import FeiEpu2InfluxOperator, LSFJob2InfluxOperator, GenericInfluxOperator

from airflow.operators import LogbookConfigurationSensor, LogbookRegisterFileOperator, LogbookRegisterRunParamsOperator, LogbookCreateRunOperator

from airflow.exceptions import AirflowException, AirflowSkipException, AirflowSensorTimeout

import os
from datetime import datetime, timedelta
import yaml
import json

import influxdb

import logging
LOG = logging.getLogger(__name__)

args = {
    'owner': 'yee',
    'provide_context': True,
    'start_date': datetime( 2019,1,1 ),

    'ssh_connection_id':        'ssh_lsf_host',
    'logbook_connection_id':    'cryoem_logbook',
    'influx_host':              'influxdb01.slac.stanford.edu',

    'queue_name':   'cryoem-daq',
    'bsub':         'bsub',
    'bjobs':        'bjobs',
    'bkill':        'bkill',

    'pipeline_script': '/gpfs/slac/cryo/fs1/daq/cryoem-pipeline/pipeline.sh',

    'apply_gainref':     True, 
    'daq_software':      '__imaging_software__', # 'EPU', 'SerialEM', 
    'max_active_runs':   30,
    # 'particle_size':     150,
    # 'create_run':         False
    # 'apix':              1.35,
    # 'fmdose':           1.75,
    # 'superres':         0,
    # 'imaging_format': '.tif',
}

lsf_env = {
    'LSB_JOB_REPORT_MAIL': 'N',
    'MODULEPATH': '/afs/slac.stanford.edu/package/spack/share/spack/modules/linux-centos7-x86_64:/afs/slac/package/singularity/modules'
}




class LSFSyncJob( LSFSubmitOperator,LSFJobSensor ):

    """ Submit a job into LSF wait for the job to finish """
    template_fields = ("lsf_script", "env",)
    template_ext = (".sh", ".bash",)

    ui_color = '#006699'

    @apply_defaults
    def __init__(self,
                 lsf_script=None,
                 queue_name='cryoem-daq',
                 bsub='bsub',
                 bsub_args='',
                 bjobs='bjobs',
                 bkill='bkill',
                 poke_interval=10,
                 timeout=60*60,
                 soft_fail=False,
                 xcom_stdout=False,
                 *args, **kwargs):
        BaseSSHOperator.__init__( self, *args, **kwargs)
        self.bsub = bsub
        self.bsub_args = bsub_args
        self.bjobs = bjobs
        self.queue_name = queue_name
        self.lsf_script = lsf_script
        self.jobid = None
        self.timeout = timeout
        self.poke_interval = poke_interval
        self.soft_fail = soft_fail
        self.prevent_returncode = None
        self.xcom_stdout = xcom_stdout

    def get_status_command(self, context):
        return LSFJobSensor.get_bash_command(self,context)

    def execute(self, context):
        d = LSFSubmitOperator.execute(self, context)
        if not 'jobid' in d:
            raise AirflowException("Could not determine jobid")
        self.jobid = d['jobid']
        status = LSFJobSensor.execute(self, context, bash_command_function='get_status_command')
        LOG.info("STATUS %s" % (status,))
        # determine what the stdout is so we can try to parse it
        data = None
        stdout = None
        for l in self.lsf_script.splitlines():
            if l.startswith('#PIPELINE-YAML'):
                stdout = l.split()[-1] 
                LOG.info("Parsing output file: %s" % (stdout,))
                break
        if stdout:
            with open(stdout, 'r') as stream:
                data = yaml.safe_load(stream)
        LOG.info( data )
        context['ti'].xcom_push( key='data', value=data )
        return status

def dummy(*args,**kwargs):
    pass

class PipelineInfluxOperator(PythonOperator):
    ui_color = '#4bcf9a'
    template_fields = ('experiment','run','microscope')
    def __init__(self,host='localhost',port=8086,user='root',password='root',db='cryoem',microscope=None,measurement='microscope_image',experiment=None,run=None,timezone='America/Los_Angeles',*args,**kwargs):
        super(PipelineInfluxOperator,self).__init__(python_callable=dummy,*args,**kwargs)
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.db = db
        self.measurement = measurement
        self.microscope = microscope
        self.experiment = experiment
        self.run = run
        self.timezone = timezone


class PipelineStatsOperator(PipelineInfluxOperator):
    def execute(self, context):
        """Push the parameter key-value pairs to the elogbook"""
        data = []
        client = influxdb.InfluxDBClient( self.host, self.port, self.user, self.password, self.db )
        client.create_database(self.measurement)
        failed = []
        LOG.info("measurement: %s" % (self.measurement,))
        for task in self.upstream_task_ids:
          LOG.info("parent: %s" % (task,))
          try:
              for d,l in self.get( context, task ).items():
                  for item in l:
                      this_about = {}
                      this_data = {}
                      this_dt = None
                      if 'duration' in item:
                          this_about['job_name'] = item['task']
                          this_about['experiment'] = self.experiment
                          this_about['host'] = context['ti'].xcom_pull( task_ids=task, key='return_value' )['host']
                          this_data['runtime'] = item['duration']
                          this_dt = item['executed_at']
                          for k,v in this_data.items():
                              this_data[k] = float(v)
                          LOG.info("  %s tags: %s\tfields: %s" % (this_dt,this_about,this_data))
                          data.append( {
                              'measurement': self.measurement,
                              'tags': this_about,
                              'fields': this_data,
                              'time': this_dt,
                          } )
              #LOG.info('  wrote datapoint at %s to %s: tag %s fields %s' % (dt, self.measurement, about, data))
              failed.append( False )
          except Exception as e:
              LOG.warn("%s" % (e,))
              failed.append( True )
        LOG.info(" DATA: %s" % (data,))
        client.write_points( data )
        return False if True in failed else True
    def get(self, context, task, key='data' ):
        return context['ti'].xcom_pull( task_ids=task, key=key )


class PipelineCtfOperator(PipelineInfluxOperator):
    def execute(self, context):
        """Push the parameter key-value pairs to the elogbook"""
        data = []
        client = influxdb.InfluxDBClient( self.host, self.port, self.user, self.password, self.db )
        client.create_database(self.measurement)
        failed = []
        LOG.info("measurement: %s" % (self.measurement,))
        for task in self.upstream_task_ids:
          LOG.info("parent: %s" % (task,))
          try:
              this_dt = None
              this_about = {}
              this_data = {}
              for item in self.get( context, task )['single_particle_analysis']:
                  if item['task'] in [ 'ctf_summed_data', 'ctf_align_data' ]:
                      this_about['app'] = 'ctffind'
                      this_about['state'] = task
                      this_about['microscope'] = self.microscope
                      #LOG.info("ITEM: %s" % (item,))
                      this_data = item['data']
                      if 'cross_correlation' in this_data and this_data['cross_correlation'] == '-nan':
                          this_data['cross_correlation'] = 0.0
                  elif item['task'] in [ 'ctf_summed', 'ctf_align' ]:
                      this_dt = item['files'][0]['create_timestamp']
                           
              this = {
                  'measurement': self.measurement,
                  'tags': this_about, 
                  'fields': this_data,
                  'time': this_dt
              }
              LOG.info( "  %s" % (this,))
              data.append( this )
# 'app': 'ctffind', 'version': '4.1.10', 'state': 'aligned', 'microscope': 'TEM4', 'pixel_size': 1.096,
              #LOG.info('  wrote datapoint at %s to %s: tag %s fields %s' % (dt, self.measurement, about, data))
              failed.append( False )
          except Exception as e:
              LOG.warn("%s" % (e,))
              failed.append( True )
        LOG.info(" DATA: %s" % (data,))
        client.write_points( data )
        return False if True in failed else True
    def get(self, context, task, key='data' ):
        return context['ti'].xcom_pull( task_ids=task, key=key )


class PipelineRegisterFilesOperator(BaseOperator):
    """ register the file information into the logbook """
    template_fields = ('experiment','run')

    def __init__(self,http_hook,experiment,run,index=0,*args,**kwargs):
        super(PipelineRegisterFilesOperator,self).__init__(*args, **kwargs)
        self.experiment = experiment
        self.run = run
        self.http_hook = http_hook
        self.index = index

    def execute(self, context):
        files = []
        for task in self. upstream_task_ids:
            LOG.info("parent: %s" % (task,))
            try:
                for k,data in context['ti'].xcom_pull( task_ids=task, key='data' ).items():
                    #LOG.info(" %s\t%s" % (k,data)) 
                    for item in data:
                        if 'files' in item:
                            LOG.info("    %s" % (item['files'],))
                            for f in item['files']:
                                files.append(f)
                #this['run_num'] = self.run
            except Exception as e:
                LOG.warn(" %s" % (e,))
        try:
            self.http_hook.method = 'POST'
            LOG.info("%s" % (files,))
            #LOG.info("  %s" % (json.dumps(files, default=str),))
            r = self.http_hook.run( '/cryoem-data/lgbk/%s/ws/register_file' % (self.experiment,), data=json.dumps(data), headers={'content-type': 'application/json'} )
            if r.status_code in (403,404,500):
                raise Exception("could not register file %s on run %s for experiment %s: %s" % (self.file_info, self.run, self.experiment, r.text,))
        except Exception as e:
            LOG.warn(" %s" % (e,))
        return True

class PipelineRegisterRunOperator(BaseOperator):
    """ register the run information into the logbook """
    template_fields = ('experiment','run')

    def __init__(self,http_hook,experiment,run,*args,**kwargs):
        super(PipelineRegisterRunOperator,self).__init__(*args, **kwargs)
        self.experiment = experiment
        self.run = run
        self.http_hook = http_hook

    def execute(self, context):

        data = {}
        for item in context['ti'].xcom_pull( task_ids='align', key='data' )['single_particle_analysis']:
            if item['task'] in ['align_data', 'ctf_align_data']:
                for k,v in item['data'].items():
                    data['%s_%s' % ('aligned', k )] = v

        for item in context['ti'].xcom_pull( task_ids='sum', key='data' )['single_particle_analysis']:
            if item['task'] in ['summed_ctf_data',]:
                for k,v in item['data'].items():
                    data['%s_%s' % ('unaligned', k )] = v

        data['preview'] = context['ti'].xcom_pull( task_ids='previews', key='data' )['single_particle_analysis'][0]['files'][0]['path']

        #data['run_num'] = self.run
        LOG.warn("RUN: %s\tDATA: %s" % (self.run,data) )

        self.http_hook.method = 'POST'
        r = self.http_hook.run( '/cryoem-data/run_control/%s/ws/add_run_params?run_num=%s' % (self.experiment,self.run), data=json.dumps(data), headers={'content-type': 'application/json'} )
        if r.status_code in (403,404,500):
            logging.error(" could not register run params on run %s for experiment %s: %s" % (self.run, self.experiment, r.text,))
            return False
        return True


def uploadExperimentalParameters2Logbook(ds, **kwargs):
    """Push the parameter key-value pairs to the elogbook"""
    data = kwargs['ti'].xcom_pull( task_ids='parse_parameters' )
    LOG.warn("data: %s" % (data,))
    raise AirflowSkipException('not yet implemented')




class NotYetImplementedOperator(DummyOperator):
    ui_color = '#d3d3d3'







###
# define the workflow
###
with DAG( os.path.splitext(os.path.basename(__file__))[0],
        description="Pre-processing of CryoEM data",
        schedule_interval=None,
        default_args=args,
        catchup=False,
        max_active_runs=args['max_active_runs'],
        concurrency=100,
        dagrun_timeout=1800,
    ) as dag:

    # hook to container host for lsf commands
    hook = SSHHook(ssh_conn_id=args['ssh_connection_id'])
    logbook_hook = HttpHook( http_conn_id=args['logbook_connection_id'], method='GET' )


    # parameters
    parameters = {
        'apply_gainref': args['apply_gainref'],
        'pipeline_script': args['pipeline_script'],
        'kev': args['kev'] if 'kev' in args else None,
        'cs': args['cs'] if 'cs' in args else None,
        'apix': args['apix'] if 'apix' in args else None,
        'fmdose': args['fmdose'] if 'fmdose' in args else None,
        'superres': args['superres'] if 'superres' in args else None,
        'phase_plate': args['phase_plate'] if 'phase_plate' in args else None,
        'software': args['daq_software'] if 'daq_software' in args else None,
    }


    ###
    # parse the epu xml metadata file
    ###
    if args['daq_software'] == 'EPU':
        parameter_file = FileInfoSensor( task_id='parameter_file',
            filepath="{{ dag_run.conf['directory'] }}/**/{{ dag_run.conf['base'] }}.xml",
            recursive=True,
            poke_interval=1,
        )
        parse_parameters = FeiEpuOperator(task_id='parse_parameters',
            filepath="{{ ti.xcom_pull( task_ids='parameter_file' )[0] }}",
        )
        # upload to the logbook
        logbook_parameters = PythonOperator(task_id='logbook_parameters',
            python_callable=uploadExperimentalParameters2Logbook,
            op_kwargs={}
        )
        influx_parameters = FeiEpu2InfluxOperator( task_id='influx_parameters',
            xcom_task_id='parse_parameters',
            host=args['influx_host'],
            experiment="{{ dag_run.conf['experiment'] }}",
        )


    ###
    # get the summed jpg
    ###
    sum = LSFSyncJob( task_id='sum',
        ssh_hook=hook,
        env=lsf_env,
        bsub=args['bsub'],
        retries=2,
        retry_delay=timedelta(seconds=1),
        lsf_script="""#!/bin/bash -l

{%- set kev = params.kev %}{% if params.kev == None and 'keV' in dag_run.conf %}{% set kev = dag_run.conf['keV'] %}{% endif %}
{%- set cs = params.cs %}{% if params.cs == None and 'cs' in dag_run.conf %}{% set cs = dag_run.conf['cs'] %}{% endif %}
{%- set apix = params.apix %}{% if params.apix == None and 'apix' in dag_run.conf %}{% set apix = dag_run.conf['apix'] %}{% endif %}
{%- set fmdose = params.fmdose %}{% if params.fmdose == None and 'fmdose' in dag_run.conf %}{% set fmdose = dag_run.conf['fmdose'] %}{% endif %}
{%- set superres = params.superres %}{% if superres == None and 'superres' in dag_run.conf %}{% set superres = 1 if dag_run.conf['superres'] in ( '1', 1, 'y' ) else 0 %}{% endif %}
{%- set phase_plate = params.phase_plate %}{% if phase_plate == None and 'phase_plate' in dag_run.conf %}{% set phase_plate = 1 if dag_run.conf['phase_plate'] in ( '1', 1, 'y' ) else 0 %}{% endif %}

#BSUB -o {{ dag_run.conf['directory'] }}/logs/{{ dag_run.conf['base'] }}-sum.job
#BSUB -W 7 
#BSUB -We 1
#BSUB -n 1

#PIPELINE-YAML {{ dag_run.conf['directory'] }}/logs/{{ dag_run.conf['base'] }}-sum.yaml

. /usr/share/Modules/init/bash

{% set cwd = dag_run.conf['directory'].replace('//','/',10) -%}
cd {{ cwd }}
mkdir -p {{ cwd }}logs/

rm -f 'logs/{{ dag_run.conf['base'] }}-sum.yaml'
FORCE=1 NO_PREAMBLE=1 APIX={{ apix }} FMDOSE={{ fmdose }} SUPERRES={{ superres }} KV={{ kev }} CS={{ cs }} PHASE_PLATE={{ phase_plate }} {% if params.software == 'EPU' %}SUMMED_FILE='{{ ti.xcom_pull( task_ids='summed_file' )[0] | replace( cwd, "" ) }}'{% endif %}  \
  {{ params.pipeline_script }} -t sum \
  --gainref '{{ ti.xcom_pull( task_ids='gainref_file' )[0] | replace( cwd, "" ) }}' \
  --basename '{{ dag_run.conf['base'] }}' \
  '{{ ti.xcom_pull( task_ids='stack_file' )[-1] | replace( cwd, "" ) }}' \
    > 'logs/{{ dag_run.conf['base'] }}-sum.yaml' 

""",
        params=parameters,
    )


    stack_file = FileGlobSensor( task_id='stack_file',
        filepath="{% set imaging_format = params.imaging_format if params.imaging_format else dag_run.conf['imaging_format'] %}{{ dag_run.conf['directory'] }}/raw/**/{{ dag_run.conf['base'] }}{% if imaging_format == '.mrc' %}-*.mrc{% elif imaging_format == '.tif' %}*.tif{% endif %}",
        params={ 
            'imaging_format': args['imaging_format'] if 'imaging_format' in args  else None,
        },
        recursive=True,
        excludes=['gain-ref',],
        poke_interval=1,
    )
    
    gainref_file = FileGlobSensor( task_id='gainref_file',
        filepath="{% set superres = params.superres %}{% if superres == None and 'superres' in dag_run.conf %}{% set superres = dag_run.conf['superres'] in ( '1', 1, 'y' ) %}{% endif %}{{ dag_run.conf['directory'] }}/raw/GainRefs/*x1.m{% if superres %}3{% else %}2{% endif %}*.dm4",
        params={
            'daq_software': args['daq_software'],
            'superres': args['superres'] if 'superres' in args else None,
        },
        recursive=True,
        poke_interval=1,
    )
    
    ###
    # align the frame
    ###

    align = LSFSyncJob( task_id='align',
        ssh_hook=hook,
        env=lsf_env,
        bsub=args['bsub'],
        retries=2,
        retry_delay=timedelta(seconds=1),
        lsf_script="""#!/bin/bash -l

{%- set kev = params.kev %}{% if params.kev == None and 'keV' in dag_run.conf %}{% set kev = dag_run.conf['keV'] %}{% endif %}
{%- set cs = params.cs %}{% if params.cs == None and 'cs' in dag_run.conf %}{% set cs = dag_run.conf['cs'] %}{% endif %}
{%- set apix = params.apix %}{% if params.apix == None and 'apix' in dag_run.conf %}{% set apix = dag_run.conf['apix'] %}{% endif %}
{%- set fmdose = params.fmdose %}{% if params.fmdose == None and 'fmdose' in dag_run.conf %}{% set fmdose = dag_run.conf['fmdose'] %}{% endif %}
{%- set superres = params.superres %}{% if superres == None and 'superres' in dag_run.conf %}{% set superres = 1 if dag_run.conf['superres'] in ( '1', 1, 'y' ) else 0 %}{% endif %}
{%- set phase_plate = params.phase_plate %}{% if phase_plate == None and 'phase_plate' in dag_run.conf %}{% set phase_plate = 1 if dag_run.conf['phase_plate'] in ( '1', 1, 'y' ) else 0 %}{% endif %}

#BSUB -o {{ dag_run.conf['directory'] }}/logs/{{ dag_run.conf['base'] }}-align.job
#BSUB -W {% if superres %}9{% else %}6{% endif %}
#BSUB -We {% if superres %}3{% else %}1{% endif %} 
#BSUB2 -n 1
#BSUB -gpu "num=1:mode=exclusive_process:j_exclusive=yes:mps=no"

#PIPELINE-YAML {{ dag_run.conf['directory'] }}/logs/{{ dag_run.conf['base'] }}-align.yaml

. /usr/share/Modules/init/bash

{% set cwd = dag_run.conf['directory'].replace('//','/',10) -%}
cd {{ cwd }}
mkdir -p {{ cwd }}logs/

rm -f 'logs/{{ dag_run.conf['base'] }}-align.yaml'
FORCE=1 NO_FORCE_GAINREF=1 APIX={{ apix }} FMDOSE={{ fmdose }} SUPERRES={{ superres }} KV={{ kev }} CS={{ cs }} PHASE_PLATE={{ phase_plate }}   \
{{ params.pipeline_script }} -t align \
  --gainref '{{ ti.xcom_pull( task_ids='gainref_file' )[0] | replace( cwd, "" ) }}' \
  --basename '{{ dag_run.conf['base'] }}' \
  '{{ ti.xcom_pull( task_ids='stack_file' )[-1] | replace( cwd, "" ) }}' \
    > 'logs/{{ dag_run.conf['base'] }}-align.yaml'

""",

        params=parameters,
    )

    influx_drift_data = GenericInfluxOperator( task_id='influx_drift_data',
        host=args['influx_host'],
        experiment="{{ dag_run.conf['experiment'] }}",
        measurement="cryoem_data",
        dt="{{ ti.xcom_pull( task_ids='align', key='data' )['single_particle_analysis'][0]['files'][0]['create_timestamp'] }}",
        tags={
            'app': 'motioncor2',
            'state': 'aligned',
            'microscope': "{{ dag_run.conf['microscope'] }}",
        },
        fields="{{ ti.xcom_pull( task_ids='align', key='data' )['single_particle_analysis'][1]['data'] }}",
    )


    particle_pick = LSFSyncJob( task_id='particle_pick',
        ssh_hook=hook,
        env=lsf_env,
        bsub=args['bsub'],
        retries=2,
        retry_delay=timedelta(seconds=1),
        lsf_script="""#!/bin/bash -l

{%- set kev = params.kev %}{% if params.kev == None and 'keV' in dag_run.conf %}{% set kev = dag_run.conf['keV'] %}{% endif %}
{%- set cs = params.cs %}{% if params.cs == None and 'cs' in dag_run.conf %}{% set cs = dag_run.conf['cs'] %}{% endif %}
{%- set apix = params.apix %}{% if params.apix == None and 'apix' in dag_run.conf %}{% set apix = dag_run.conf['apix'] %}{% endif %}
{%- set fmdose = params.fmdose %}{% if params.fmdose == None and 'fmdose' in dag_run.conf %}{% set fmdose = dag_run.conf['fmdose'] %}{% endif %}
{%- set superres = params.superres %}{% if superres == None and 'superres' in dag_run.conf %}{% set superres = 1 if dag_run.conf['superres'] in ( '1', 1, 'y' ) else 0 %}{% endif %}
{%- set phase_plate = params.phase_plate %}{% if phase_plate == None and 'phase_plate' in dag_run.conf %}{% set phase_plate = 1 if dag_run.conf['phase_plate'] in ( '1', 1, 'y' ) else 0 %}{% endif %}

#BSUB -o {{ dag_run.conf['directory'] }}/logs/{{ dag_run.conf['base'] }}-pick.job
#BSUB -W 7
#BSUB -We 1
#BSUB -n 1

#PIPELINE-YAML {{ dag_run.conf['directory'] }}/logs/{{ dag_run.conf['base'] }}-pick.yaml

. /usr/share/Modules/init/bash

{% set cwd = dag_run.conf['directory'].replace('//','/',10) -%}
cd {{ cwd }}
mkdir -p {{ cwd }}logs/

rm -f 'logs/{{ dag_run.conf['base'] }}-pick.yaml'
FORCE=1 NO_PREAMBLE=1 APIX={{ apix }}   \
{{ params.pipeline_script }} -t pick \
  --gainref '{{ ti.xcom_pull( task_ids='gainref_file' )[0] | replace( cwd, "" ) }}' \
  --basename '{{ dag_run.conf['base'] }}' \
  '{{ ti.xcom_pull( task_ids='stack_file' )[-1] | replace( cwd, "" ) }}' \
    > 'logs/{{ dag_run.conf['base'] }}-pick.yaml'

""",
        params=parameters,
    )

    
    previews = LSFSyncJob( task_id='previews',
        ssh_hook=hook,
        env=lsf_env,
        bsub=args['bsub'],
        retries=2,
        retry_delay=timedelta(seconds=1),
        lsf_script="""#!/bin/bash -l

{%- set kev = params.kev %}{% if params.kev == None and 'keV' in dag_run.conf %}{% set kev = dag_run.conf['keV'] %}{% endif %}
{%- set cs = params.cs %}{% if params.cs == None and 'cs' in dag_run.conf %}{% set cs = dag_run.conf['cs'] %}{% endif %}
{%- set apix = params.apix %}{% if params.apix == None and 'apix' in dag_run.conf %}{% set apix = dag_run.conf['apix'] %}{% endif %}
{%- set fmdose = params.fmdose %}{% if params.fmdose == None and 'fmdose' in dag_run.conf %}{% set fmdose = dag_run.conf['fmdose'] %}{% endif %}
{%- set superres = params.superres %}{% if superres == None and 'superres' in dag_run.conf %}{% set superres = 1 if dag_run.conf['superres'] in ( '1', 1, 'y' ) else 0 %}{% endif %}
{%- set phase_plate = params.phase_plate %}{% if phase_plate == None and 'phase_plate' in dag_run.conf %}{% set phase_plate = 1 if dag_run.conf['phase_plate'] in ( '1', 1, 'y' ) else 0 %}{% endif %}

#BSUB -o {{ dag_run.conf['directory'] }}/logs/{{ dag_run.conf['base'] }}-preview.job
#BSUB -W 4
#BSUB -We 1
#BSUB -n 1 

#PIPELINE-YAML {{ dag_run.conf['directory'] }}/logs/{{ dag_run.conf['base'] }}-preview.yaml

. /usr/share/Modules/init/bash

{% set cwd = dag_run.conf['directory'].replace('//','/',10) -%}
cd {{ cwd }}
mkdir -p {{ cwd }}logs/

{%- set sum_ctf = ti.xcom_pull( task_ids='sum', key='data' )['single_particle_analysis'][-1]['data'] %}
{%- set align = ti.xcom_pull( task_ids='align', key='data' )['single_particle_analysis'] %}
{%- set drift = align[1]['data'] %}
{%- set align_ctf = align[-1]['data'] %}

FORCE=1 NO_PREAMBLE=1 APIX={{ apix }} {% if params.software == 'EPU' %}SUMMED_FILE='{{ ti.xcom_pull( task_ids='summed_file' )[0] | replace( cwd, "" ) }}'{% endif %}  \
PROCESSED_SUM_RESOLUTION={{ sum_ctf['resolution'] }} PROCESSED_SUM_RESOLUTION_PERFORMANCE={{ sum_ctf['resolution_performance'] }}  \
PROCESSED_ALIGN_FIRST1={{ drift['first1'] }} PROCESSED_ALIGN_FIRST5={{ drift['first5'] }} PROCESSED_ALIGN_ALL={{ drift['all'] }} \
PROCESSED_ALIGN_RESOLUTION={{ align_ctf['resolution'] }} PROCESSED_ALIGN_RESOLUTION_PERFORMANCE={{ align_ctf['resolution_performance'] }} PROCESSED_ALIGN_ASTIGMATISM={{ align_ctf['astigmatism'] }} PROCESSED_ALIGN_CROSS_CORRELATION={{ align_ctf['cross_correlation'] }}  \
{{ params.pipeline_script }} -t preview \
  --gainref '{{ ti.xcom_pull( task_ids='gainref_file' )[0] | replace( cwd, "" ) }}' \
  --basename '{{ dag_run.conf['base'] }}' \
  '{{ ti.xcom_pull( task_ids='stack_file' )[-1] | replace( cwd, "" ) }}' \
    > 'logs/{{ dag_run.conf['base'] }}-preview.yaml'

""",

        params=parameters,
    )

    slack_preview = SlackAPIUploadFileOperator( task_id='slack_preview',
        channel="{{ dag_run.conf['experiment'][:21] | replace( ' ', '' ) | lower }}",
        token=Variable.get('slack_token'),
        filepath="{{ dag_run.conf['directory'] }}/{{ ti.xcom_pull( task_ids='previews', key='data' )['single_particle_analysis'][0]['files'][0]['path'] }}",
        #retries=2,
    )

    logbook_run = PipelineRegisterRunOperator( task_id='logbook_run',
        http_hook=logbook_hook,
        experiment="{{ dag_run.conf['experiment'].split('_')[0] }}",
        run="{{ dag_run.conf['base'] }}",
        #retries=2,
    )

    update_star_file = BashOperator( task_id='update_star_file',
        retries=2,
        bash_command="""
            export STAR_FILE=images.star
            cd {{ dag_run.conf['directory'] }}
            # add header if necessary
            if [ ! -f $STAR_FILE ]; then
                echo "creating $STAR_FILE"
                cat << EOT > $STAR_FILE
# RELION; version 3.0-beta-2

data_

loop_
_rlnMicrographName #1
_rlnCtfImage #2
_rlnDefocusU #3
_rlnDefocusV #4
_rlnCtfAstigmatism #5
_rlnDefocusAngle #6
_rlnVoltage #7
_rlnSphericalAberration #8
_rlnAmplitudeContrast #9
_rlnMagnification #10
_rlnDetectorPixelSize #11
_rlnCtfFigureOfMerit #12
_rlnCtfMaxResolution #13
EOT
            fi
            {
              {% set data = ti.xcom_pull( task_ids='align', key='data' ) -%}
              {% set spa = data['single_particle_analysis'] -%}
              {% set aligned_file = spa[0]['files'][1]['path'] -%}
              {% set ctf = spa[-1]['data'] -%}
              {% set pre = data['pre-pipeline'][0]['data'] -%}
              flock -x 3 || return
              # remove existing entry if exists
              if grep -q "{{ aligned_file }}" $STAR_FILE; then
echo 'clearing old value'
                  sed -i '/^{{ aligned_file.replace('/','\/') }}/d' $STAR_FILE 
              fi

              echo "{{ aligned_file }}  {{ spa[-2]['files'][0]['path'] }}:mrc   {{ ctf['defocus_1']  }}  {{ ctf['defocus_2']  }}  {{ ctf['astigmatism'] }}  {{ ctf['phase_shift'] }}  {{ pre['kev'] }}  {{ pre['astigmatism'] }}  {{ pre['amplitude_contrast'] }}  10000  {{ pre.apix  }}  {{ ctf['cross_correlation']  }}  {{ ctf['resolution']  }}" >> $STAR_FILE
            } 3<>$STAR_FILE
            
        """,
    )


    influx_jobs = PipelineStatsOperator( task_id='influx_jobs',
        host=args['influx_host'],
        experiment="{{ dag_run.conf['experiment'] }}",
        measurement='preprocessing',
    )

    logbook_files = PipelineRegisterFilesOperator( task_id='logbook_files',
        http_hook=logbook_hook,
        experiment="{{ dag_run.conf['experiment'].split('_')[0] }}",
        run="{{ dag_run.conf['base'] }}"
    )

    influx_ctf = PipelineCtfOperator( task_id='influx_ctf',
        host=args['influx_host'],
        experiment="{{ dag_run.conf['experiment'].split('_')[0] }}",
        run="{{ dag_run.conf['base'] }}",
        measurement='preprocessing',
        microscope="{{ dag_run.conf['microscope'] }}",
    )

    ###
    # define pipeline
    ###

    if 'create_run' in args and args['create_run']:
        create_run = LogbookCreateRunOperator( task_id='create_run',
            http_hook=logbook_hook,
            experiment="{{ dag_run.conf['experiment'].split('_')[0] }}",
            run="{{ dag_run.conf['base'] }}"
        )

        create_run >> stack_file 
        create_run >> gainref_file >> logbook_gainref_file


    if args['daq_software'] == 'EPU':
        summed_file = FileGlobSensor( task_id='summed_file',
            filepath="{% if params.daq_software == 'SerialEM' %}{{ dag_run.conf['directory'] }}/summed/imod/{{ params.software.imod.version }}/{{ dag_run.conf['base'] }}_avg_gainrefd.mrc{% else %}{{ dag_run.conf['directory'] }}/**/{{ dag_run.conf['base'] }}.mrc{% endif %}",
            params={
                'imaging_format': args['imaging_format'] if 'imaging_format' in args  else None,
            },
            recursive=True,
            poke_interval=1,
        )

        summed_file >> sum


    gainref_file >> sum
    stack_file >> sum
    gainref_file >> align
    stack_file >> align

    if args['daq_software'] == 'EPU':
        parameter_file >> parse_parameters >> logbook_parameters
        #sum  >> logbook_parameters
        parse_parameters >> influx_parameters

    sum >> previews
    sum >> logbook_run

    align >> influx_drift_data 
    align >> previews
    align >> particle_pick >> previews

    align >> logbook_run
    previews >> logbook_run
    previews >> slack_preview

    align >> update_star_file

    sum >> influx_jobs
    align >> influx_jobs
    particle_pick >> influx_jobs
    previews >> influx_jobs

    sum >> logbook_files
    align >> logbook_files
    particle_pick >> logbook_files
    previews >> logbook_files

    sum >> influx_ctf
    align >> influx_ctf
