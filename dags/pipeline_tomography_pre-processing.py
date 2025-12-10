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


    ###
    # parse the epu xml metadata file
    ###
    if args['daq_software'] == 'EPU':
        parameter_file = FileInfoSensor( task_id='parameter_file',
            filepath="{{ params.directory_prefix }}/{{ dag_run.conf['data_directory'] }}/raw/**/{{ dag_run.conf['base'] }}.xml",
            params={
                'directory_prefix': args['directory_prefix'],
            },
            recursive=True,
            poke_interval=1,
            retries=2,
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
            port=443,
            ssl=True,
            experiment="{{ dag_run.conf['experiment'] }}",
        )


    ###
    # get the summed jpg
    ###
    sum = BashPlusOperator( task_id='sum',
        queue='cpu',
        retries=2,
        bash_command="""
{%- set kev = params.kev %}{% if params.kev == None and 'keV' in dag_run.conf %}{% set kev = dag_run.conf['keV'] %}{% endif %}
{%- set cs = params.cs %}{% if params.cs == None and 'cs' in dag_run.conf %}{% set cs = dag_run.conf['cs'] %}{% endif %}
{%- set apix = params.apix %}{% if params.apix == None and 'apix' in dag_run.conf %}{% set apix = dag_run.conf['apix'] %}{% endif %}
{%- set fmdose = params.fmdose %}{% if params.fmdose == None and 'fmdose' in dag_run.conf %}{% set fmdose = dag_run.conf['fmdose'] %}{% endif %}
{%- set superres = params.superres %}{% if superres == None and 'superres' in dag_run.conf %}{% set superres = 1 if dag_run.conf['superres'] in ( '1', 1, 'y' ) else 0 %}{% endif %}
{%- set phase_plate = params.phase_plate %}{% if phase_plate == None and 'phase_plate' in dag_run.conf %}{% set phase_plate = 1 if dag_run.conf['phase_plate'] in ( '1', 1, 'y' ) else 0 %}{% endif %}
{% set cwd = params.directory_prefix + '/' + dag_run.conf['data_directory'].replace('//','/',10) -%}
{%- set basename = '_'.join( dag_run.conf['base'].split('_')[:-1] ) %}
#DATA {{ cwd }}/logs/{{ dag_run.conf['base'] }}-sum.yaml
#OUT {{ cwd }}/logs/{{ dag_run.conf['base'] }}-sum.job
cd {{ cwd }} && mkdir -p {{ cwd }}/logs/
FORCE=1 NO_PREAMBLE=1 APIX={{ apix }} FMDOSE={{ fmdose }} SUPERRES={{ superres }} KV={{ kev }} CS={{ cs }} PHASE_PLATE={{ phase_plate }} {% if params.software == 'EPU' %}SUMMED_FILE='{{ ti.xcom_pull( task_ids='summed_file' )[0] | replace( cwd, "." ) }}'{% endif %}  \
  {{ params.pipeline_script }} -m tomo -t sum \
  {% if params.apply_gairef %}--gainref '{{ ti.xcom_pull( task_ids='gainref_file' )[0] | replace( cwd, "." ) }}'{% endif %} \
  --basename '{{ basename }}' \
  --mdoc '{{ ti.xcom_pull( task_ids='mdoc_file' )[0] | replace( cwd, "." ) }}' \
  '{{ ti.xcom_pull( task_ids='stack_file' )[-1] | replace( cwd, "." ) }}' \
    > './logs/{{ dag_run.conf['base'] }}-sum.yaml' 
""",
        params=parameters,
    )

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
    
    if parameters['apply_gainref']:
        gainref_file = FileGlobSensor( task_id='gainref_file',
            #filepath="{% set superres = params.superres %}{% if superres == None and 'superres' in dag_run.conf %}{% set superres = dag_run.conf['superres'] in ( '1', 1, 'y' ) %}{% endif %}{{ params.directory_prefix }}/{{ dag_run.conf['data_directory'] }}/raw/GainRefs/*x1.m{% if superres %}1{% else %}2{% endif %}*.dm4",
            filepath="{% set m_file = 'm1' %}{% if dag_run.conf['microscope'] in ( 'blah', ) %}{% set m_file = 'm3' %}{% endif %}{{ params.directory_prefix }}/{{ dag_run.conf['data_directory'] }}/raw/GainRefs/*x1.{{ m_file }}*.dm4",
            params={
                'daq_software': args['daq_software'],
                'superres': args['superres'] if 'superres' in args else None,
                'directory_prefix': args['directory_prefix'],
            },
            recursive=True,
            poke_interval=1,
        )

    mdoc_file = FileGlobSensor( task_id='mdoc_file',
        filepath="{% set imaging_format = params.imaging_format if params.imaging_format else dag_run.conf['imaging_format'] %}{%- set basename = '_'.join( dag_run.conf['base'].split('_')[:-1]) %}{% if basename[-3] == '_' and basename[-2:].isnumeric() %}{% set basename = '_'.join( dag_run.conf['base'].split('_')[:-2]) %}{% endif %}{{ params.directory_prefix }}/{{ dag_run.conf['data_directory'] }}/raw/**/{{ basename }}{% if imaging_format == '.mrc' %}.mrc{% else %}.*{% endif %}.mdoc",
        params={
            'daq_software': args['daq_software'],
            'directory_prefix': args['directory_prefix'],
            'imaging_format': args['imaging_format'] if 'imaging_format' in args  else None,
        },
        recursive=True,
        retries=2,
        poke_interval=1,
    )

    if args.get('apply_defect_file'):
      defect_file = FileGlobSensor( task_id='defect_file',
          filepath="{%- set basename = '_'.join( dag_run.conf['base'].split('_')[:-1]) %}{% if basename[-3] == '_' and basename[-2:].isnumeric() %}{% set basename = '_'.join( dag_run.conf['base'].split('_')[:-2]) %}{% endif %}{{ params.directory_prefix }}/{{ dag_run.conf['data_directory'] }}/raw/**/defects_*.txt",
          params={
             'daq_software': args['daq_software'],
              'directory_prefix': args['directory_prefix'],
              'imaging_format': args['imaging_format'] if 'imaging_format' in args  else None,
          },
          recursive=True,
          poke_interval=1,
      )
    
    ###
    # align the frame
    ###

    align = BashPlusOperator( task_id='align',
        queue='gpu',
        bash_command="""
{%- set kev = params.kev %}{% if params.kev == None and 'keV' in dag_run.conf %}{% set kev = dag_run.conf['keV'] %}{% endif %}
{%- set cs = params.cs %}{% if params.cs == None and 'cs' in dag_run.conf %}{% set cs = dag_run.conf['cs'] %}{% endif %}
{%- set apix = params.apix %}{% if params.apix == None and 'apix' in dag_run.conf %}{% set apix = dag_run.conf['apix'] %}{% endif %}
{%- set fmdose = params.fmdose %}{% if params.fmdose == None and 'fmdose' in dag_run.conf %}{% set fmdose = dag_run.conf['fmdose'] %}{% endif %}
{%- set patch = params.patch %}{% if params.patch == None and 'preprocess/align/motioncor2/patch' in dag_run.conf %}{% set patch = dag_run.conf['preprocess/align/motioncor2/patch'] %}{% endif %}{% if patch == None %}{% set patch = '5 5 20' %}{% endif %}
{%- set superres = params.superres %}{% if superres == None and 'superres' in dag_run.conf %}{% set superres = 1 if dag_run.conf['superres'] in ( '1', 1, 'y' ) else 0 %}{% endif %}
{%- set phase_plate = params.phase_plate %}{% if phase_plate == None and 'phase_plate' in dag_run.conf %}{% set phase_plate = 1 if dag_run.conf['phase_plate'] in ( '1', 1, 'y' ) else 0 %}{% endif %}
{%- set cwd = params.directory_prefix + '/' + dag_run.conf['data_directory'].replace('//','/',10) %}
{%- set basename = '_'.join( dag_run.conf['base'].split('_')[:-1] ) %}
#DATA  {{ cwd }}/logs/{{ dag_run.conf['base'] }}-align.yaml
#OUT  {{ cwd }}/logs/{{ dag_run.conf['base'] }}-align.job
cd {{ cwd }} && mkdir -p {{ cwd }}/logs/

RET=1
until [[ $RET != 1 ]]
do

  GPU=""
  until [[ $GPU != ""  ]]
  do
	  GPU=$(nvidia-smi pmon -c 1 | grep '-' | awk -v r=$RANDOM '{ a[n++]=$1 } END{ print a[int(r/32767*n)] }')
	  if [ "$GPU" == "" ]; then sleep $((1 + RANDOM % 10)); fi
  done
  echo "attempting to use gpu $GPU..."

  # check if locked (not guaranteed, but better than nothing)
  HANDLE=$((${GPU}+10))
  LOCK=/tmp/$(hostname -s)-${SLURM_JOB_ID}-${GPU}
  eval "exec ${HANDLE}>${LOCK}"
  flock -n $HANDLE
  RET=$?

done

eval "flock -x $HANDLE"
echo "using gpu $GPU..."
nvidia-smi pmon -c 1

FORCE=1 NO_FORCE_GAINREF=1 APIX={{ apix }} FMDOSE={{ fmdose }} PATCH='{{ patch }}' SUPERRES={{ superres }} KV={{ kev }} CS={{ cs }} PHASE_PLATE={{ phase_plate }} DEFECT='{{ ti.xcom_pull( task_ids='defect_file' )[0] | replace( cwd, "." ) }}'   \
GPU=$GPU \
{{ params.pipeline_script }} -m tomo -t align \
  {% if params.apply_gainref %}--gainref '{{ ti.xcom_pull( task_ids='gainref_file' )[0] | replace( cwd, "." ) }}'{% endif %} \
  --basename '{{ basename }}' \
  --mdoc '{{ ti.xcom_pull( task_ids='mdoc_file' )[0] | replace( cwd, "." ) }}' \
  '{{ ti.xcom_pull( task_ids='stack_file' )[-1] | replace( cwd, "." ) }}' \
    > './logs/{{ dag_run.conf['base'] }}-align.yaml'
RC=$?

eval "flock -u $HANDLE"
exit $RC
""",
        params=parameters,
    )

    influx_drift_data = GenericInfluxOperator( task_id='influx_drift_data',
        host=args['influx_host'],
        port=443,
        ssl=True,
        verify_ssl=False,
        experiment="{{ dag_run.conf['experiment'] }}",
        measurement="cryoem_data",
        dt="{{ ti.xcom_pull( task_ids='align', key='data' )['tomographic_analysis'][0]['files'][0]['create_timestamp'] }}",
        tags={
            'app': 'motioncor2',
            'state': 'aligned',
            'microscope': "{{ dag_run.conf['microscope'] }}",
        },
        fields="{{ ti.xcom_pull( task_ids='align', key='data' )['tomographic_analysis'][1]['data'] }}",
    )


    previews = BashPlusOperator( task_id='previews',
        queue='cpu',
        bash_command="""
{%- set kev = params.kev %}{% if params.kev == None and 'keV' in dag_run.conf %}{% set kev = dag_run.conf['keV'] %}{% endif %}
{%- set cs = params.cs %}{% if params.cs == None and 'cs' in dag_run.conf %}{% set cs = dag_run.conf['cs'] %}{% endif %}
{%- set apix = params.apix %}{% if params.apix == None and 'apix' in dag_run.conf %}{% set apix = dag_run.conf['apix'] %}{% endif %}
{%- set fmdose = params.fmdose %}{% if params.fmdose == None and 'fmdose' in dag_run.conf %}{% set fmdose = dag_run.conf['fmdose'] %}{% endif %}
{%- set superres = params.superres %}{% if superres == None and 'superres' in dag_run.conf %}{% set superres = 1 if dag_run.conf['superres'] in ( '1', 1, 'y' ) else 0 %}{% endif %}
{%- set phase_plate = params.phase_plate %}{% if phase_plate == None and 'phase_plate' in dag_run.conf %}{% set phase_plate = 1 if dag_run.conf['phase_plate'] in ( '1', 1, 'y' ) else 0 %}{% endif -%}
{%- set cwd = params.directory_prefix + '/' + dag_run.conf['data_directory'].replace('//','/',10) -%}
{%- set basename = '_'.join( dag_run.conf['base'].split('_')[:-1] ) %}
#DATA {{ cwd }}/logs/{{ dag_run.conf['base'] }}-preview.yaml
#OUT {{ cwd }}/logs/{{ dag_run.conf['base'] }}-preview.job
cd {{ cwd }} && mkdir -p {{ cwd }}/logs/
{%- set sum_ctf = ti.xcom_pull( task_ids='sum', key='data' )['tomographic_analysis'][-1]['data'] %}
{%- set align = ti.xcom_pull( task_ids='align', key='data' )['tomographic_analysis'] %}
{%- set drift = align[1]['data'] %}
{%- set align_ctf = align[-1]['data'] %}
FORCE=1 NO_PREAMBLE=1 APIX={{ apix }} {% if params.software == 'EPU' %}SUMMED_FILE='{{ ti.xcom_pull( task_ids='summed_file' )[0] | replace( cwd, "." ) }}'{% endif %}  \
PROCESSED_SUM_RESOLUTION={{ sum_ctf['resolution'] }} PROCESSED_SUM_RESOLUTION_PERFORMANCE={{ sum_ctf['resolution_performance'] }}  \
PROCESSED_ALIGN_FIRST1={{ drift['first1'] }} PROCESSED_ALIGN_FIRST5={{ drift['first5'] }} PROCESSED_ALIGN_ALL={{ drift['all'] }} \
PROCESSED_ALIGN_RESOLUTION={{ align_ctf['resolution'] }} PROCESSED_ALIGN_RESOLUTION_PERFORMANCE={{ align_ctf['resolution_performance'] }} PROCESSED_ALIGN_ASTIGMATISM={{ align_ctf['astigmatism'] }} PROCESSED_ALIGN_CROSS_CORRELATION={{ align_ctf['cross_correlation'] }}  \
{{ params.pipeline_script }} -m tomo -t preview \
  {% if params.apply_gainref %}--gainref '{{ ti.xcom_pull( task_ids='gainref_file' )[0] | replace( cwd, "." ) }}' {% endif %}\
  --mdoc '{{ ti.xcom_pull( task_ids='mdoc_file' )[0] | replace( cwd, "." ) }}' \
  --basename '{{ basename }}' \
  '{{ ti.xcom_pull( task_ids='stack_file' )[-1] | replace( cwd, "." ) }}' \
    > './logs/{{ dag_run.conf['base'] }}-preview.yaml'

""",

        params=parameters,
    )

#    slack_preview = SlackAPIUploadFileOperator( task_id='slack_preview',
#        channel="{{ dag_run.conf['experiment'] | replace( ' ', '' ) | lower }}",
#        token=Variable.get('slack_token'),
#        filepath="%s/{{ dag_run.conf['data_directory'] }}/{{ ti.xcom_pull( task_ids='previews', key='data' )['tomographic_analysis'][0]['files'][0]['path'] }}" % (args['directory_prefix'],),
#    )
 
    slack_preview = BashPlusOperator( task_id='slack_preview',
        queue='cpu',
    	bash_command="""
{% set cwd = params.directory_prefix + '/' + dag_run.conf['data_directory'].replace('//','/',10) -%}
#OUT {{ cwd }}/logs/{{ dag_run.conf['base'] }}-slack_preview.job
export TOKEN="{{ params.token }}"
export FILEPATH="{{ params.directory_prefix }}/{{ dag_run.conf['data_directory'] }}/{{ ti.xcom_pull( task_ids='previews', key='data' )['tomographic_analysis'][0]['files'][0]['path'] }}"
export CHANNEL="{{ dag_run.conf['experiment'] | replace( ' ', '' ) | lower }}"
if [ -f "${FILEPATH}" ]; then
    echo "Uploading preview image ${FILEPATH} to channel ${CHANNEL}"
    {{ params.directory_prefix }}/slack_preview_upload.sh -t ${TOKEN} -f ${FILEPATH} -c ${CHANNEL}
else
    echo "Error: Preview image ${FILEPATH} not found."
fi	

""",
        params={
            'directory_prefix': args['directory_prefix'],
            'token': Variable.get('slack_' + os.path.splitext(os.path.basename(__file__))[0].split('_')[1].lower() ),
        }
    )
  
    logbook_run = PipelineRegisterRunOperator( task_id='logbook_run',
        http_hook=logbook_hook,
        experiment="{{ dag_run.conf['experiment'].split('_')[0] }}",
        run="{{ dag_run.conf['base'] }}",
        imaging_method='tomographic_analysis',
        #retries=2,
    )

    influx_jobs = PipelineStatsOperator( task_id='influx_jobs',
        host=args['influx_host'],
        port=443,
        experiment="{{ dag_run.conf['experiment'] }}",
        measurement='preprocessing',
        ssl=True,
        verify_ssl=False,
    )

    logbook_files = PipelineRegisterFilesOperator( task_id='logbook_files',
        http_hook=logbook_hook,
        experiment="{{ dag_run.conf['experiment'].split('_')[0] }}",
        run="{{ dag_run.conf['base'] }}"
    )

    influx_ctf = PipelineCtfOperator( task_id='influx_ctf',
        host=args['influx_host'],
        port=443,
        ssl=True,
        verify_ssl=False,
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
        #create_run >> gainref_file >> logbook_gainref_file
        create_run >> gainref_file 

    if args['daq_software'] == 'EPU':
        summed_file = FileGlobSensor( task_id='summed_file',
            filepath="{% if params.daq_software == 'SerialEM' %}{{ params.directory_prefix }}/{{ dag_run.conf['data_directory'] }}/summed/imod/{{ params.software.imod.version }}/{{ dag_run.conf['base'] }}_avg_gainrefd.mrc{% else %}{{ params.directory_prefix }}/{{ dag_run.conf['data_directory'] }}/raw/**/{{ dag_run.conf['base'] }}.mrc{% endif %}",
            params={
                'imaging_format': args['imaging_format'] if 'imaging_format' in args  else None,
                'directory_prefix': args['directory_prefix'],
            },
            recursive=True,
            poke_interval=1,
        )

        summed_file >> sum

    # create tomogram
    check = PreTomogramOperator( task_id='check',
        log_file = "{% set imaging_format = params.imaging_format if params.imaging_format else dag_run.conf['imaging_format'] %}{% if imaging_format == '.tif' %}{% set imaging_format = '.mrc' %}{% endif %}{% set ext = imaging_format + '.mdoc' %}{{ ti.xcom_pull( task_ids='mdoc_file' ).pop().replace( ext, '.log' ) }}",
        params={
            'imaging_format': args['imaging_format'] if 'imaging_format' in args  else None,
        }
    ) 

    create = BashPlusOperator( task_id='create',
        queue='cpu',
        bash_command="""
{%- set kev = params.kev %}{% if params.kev == None and 'keV' in dag_run.conf %}{% set kev = dag_run.conf['keV'] %}{% endif %}
{%- set cs = params.cs %}{% if params.cs == None and 'cs' in dag_run.conf %}{% set cs = dag_run.conf['cs'] %}{% endif %}
{%- set apix = params.apix %}{% if params.apix == None and 'apix' in dag_run.conf %}{% set apix = dag_run.conf['apix'] %}{% endif %}
{%- set fmdose = params.fmdose %}{% if params.fmdose == None and 'fmdose' in dag_run.conf %}{% set fmdose = dag_run.conf['fmdose'] %}{% endif %}
{%- set patch = params.patch %}{% if params.patch == None and 'preprocess/align/motioncor2/patch' in dag_run.conf %}{% set patch = dag_run.conf['preprocess/align/motioncor2/patch'] %}{% endif %}
{%- set superres = params.superres %}{% if superres == None and 'superres' in dag_run.conf %}{% set superres = 1 if dag_run.conf['superres'] in ( '1', 1, 'y' ) else 0 %}{% endif %}
{%- set phase_plate = params.phase_plate %}{% if phase_plate == None and 'phase_plate' in dag_run.conf %}{% set phase_plate = 1 if dag_run.conf['phase_plate'] in ( '1', 1, 'y' ) else 0 %}{% endif %}
{%- set imaging_format = params.imaging_format if params.imaging_format else dag_run.conf['imaging_format'] %}
{%- set cwd = params.directory_prefix + '/' + dag_run.conf['data_directory'].replace('//','/',10) %}
{%- set basename = '_'.join( dag_run.conf['base'].split('_')[:-1] ) %}
#DATA  {{ cwd }}/logs/{{ basename }}-create.yaml
#OUT  {{ cwd }}/logs/{{ basename }}-create.job
cd {{ cwd }} && mkdir -p {{ cwd }}/logs/
FORCE=1 NO_PREAMBLE=1 NO_FORCE_GAINREF=1 APIX={{ apix }} FMDOSE={{ fmdose }} PATCH='{{ patch }}' SUPERRES={{ superres }} KV={{ kev }} CS={{ cs }} PHASE_PLATE={{ phase_plate }}   \
{{ params.pipeline_script }} -m tomo -t create \
  {% if params.apply_gainref %}--gainref '{{ ti.xcom_pull( task_ids='gainref_file' )[0] | replace( cwd, "." ) }}'{% endif %} \
  --basename '{{ basename }}' \
  --mdoc '{{ ti.xcom_pull( task_ids='mdoc_file' )[0] | replace( cwd, "." ) }}' \
  {{ '/'.join( ti.xcom_pull( task_ids='stack_file' )[-1].split('/')[:-1] ) | replace( cwd, "." ) }}/{{ basename }}_*{{ imaging_format }} \
    > './logs/{{ basename }}-create.yaml'

""",
        params=parameters,
    ) 

    tomo_files = PipelineRegisterFilesOperator( task_id='tomo_files',
        http_hook=logbook_hook,
        experiment="{{ dag_run.conf['experiment'].split('_')[0] }}",
        run="{%- set basename = '_'.join( dag_run.conf['base'].split('_')[:-1] ) %}{{ basename }}"
    )

    tomo_run = PipelineRegisterRunOperator( task_id='tomo_run',
        http_hook=logbook_hook,
        experiment="{{ dag_run.conf['experiment'].split('_')[0] }}",
        run="{%- set basename = '_'.join( dag_run.conf['base'].split('_')[:-1] ) %}{{ basename }}",
        imaging_method='tomographic_analysis',
        run_type="SUMMARY",
        #retries=2,
    )

    influx_tomo = PipelineStatsOperator( task_id='influx_tomo',
        host=args['influx_host'],
        port=443,
        experiment="{{ dag_run.conf['experiment'] }}",
        measurement='preprocessing',
        ssl=True,
        verify_ssl=False,
    )

    slack_tomo = NotYetImplementedOperator( task_id='slack_tomo'
    )

    if parameters['apply_gainref']:
        gainref_file >> sum
        gainref_file >> align
    stack_file >> sum
    stack_file >> align
    mdoc_file >> align
    if args.get('apply_defect_file'):
      defect_file >> align
    mdoc_file >> sum

    if args['daq_software'] == 'EPU':
        parameter_file >> parse_parameters >> logbook_parameters
        #sum  >> logbook_parameters
        parse_parameters >> influx_parameters

    sum >> previews
    sum >> logbook_run

    align >> influx_drift_data 
    align >> previews

    align >> logbook_run
    previews >> logbook_run
    previews >> slack_preview

    sum >> influx_jobs
    align >> influx_jobs
    previews >> influx_jobs

    sum >> logbook_files
    align >> logbook_files
    previews >> logbook_files

    sum >> influx_ctf
    align >> influx_ctf

    align >> check
    check >> create >> tomo_files
    create >> tomo_run
    create >> influx_tomo
    create >> slack_tomo
