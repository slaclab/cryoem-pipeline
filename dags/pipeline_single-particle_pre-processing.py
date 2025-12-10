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

MICROSCOPE = os.path.splitext(os.path.basename(__file__))[0].split('_')[1]

args = {
    'owner': 'cryo-daq',
    'provide_context': True,
    'start_date': datetime( 2020,1,1 ),

    'logbook_connection_id':    'cryoem_logbook',
    'influx_host':              'influxdb.slac.stanford.edu',

    'pipeline_script': '/usr/local/airflow/pipeline.sh',
    #'directory_prefix': '/sdf/group/cryoem/exp' if MICROSCOPE in ( 'TEM1', 'TEM2', 'TEM3', 'TEM4', 'TEMALPHA', 'TEMBETA', 'TEMGAMMA' ) else '/gpfs/slac/cryo/fs1/exp',
    'directory_prefix': '/sdf/group/cryoem/exp', 

    'apply_gainref':     False if MICROSCOPE in ( 'TEM3', 'TEM4', 'TEMALPHA', 'TEMGAMMA' ) else True, 
    'daq_software':      '__imaging_software__',
    'max_active_runs':   30,
    # 'particle_size':     150,
    # 'create_run':         False
    # 'apix':              1.35,
    # 'fmdose':           1.75,
    # 'superres':         0,
    # 'imaging_format': '.tif',
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
        dagrun_timeout=900,
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
        'superres': args['superres'] if 'superres' in args else None,
        'phase_plate': args['phase_plate'] if 'phase_plate' in args else None,
        'software': args['daq_software'] if 'daq_software' in args else None,
        'directory_prefix': args['directory_prefix']
    }


    ###
    # parse the epu xml metadata file
    ###
    #if args['daq_software'] == 'EPU':
    #    parameter_file = FileInfoSensor( task_id='parameter_file',
    #        filepath="{{ params.directory_prefix }}/{{ dag_run.conf['data_directory'] }}/raw/**/{{ dag_run.conf['base'] }}.xml",
    #        params={
    #            'directory_prefix': args['directory_prefix'],
    #        },
    #        recursive=True,
    #        poke_interval=1,
    #        retries=1,
    #    )
    #    parse_parameters = FeiEpuOperator(task_id='parse_parameters',
    #        filepath="{{ ti.xcom_pull( task_ids='parameter_file' )[0] }}",
    #    )
    #    # upload to the logbook
    #    logbook_parameters = PythonOperator(task_id='logbook_parameters',
    #        python_callable=uploadExperimentalParameters2Logbook,
    #        op_kwargs={}
    #    )
    #    influx_parameters = FeiEpu2InfluxOperator( task_id='influx_parameters',
    #        xcom_task_id='parse_parameters',
    #        host=args['influx_host'],
    #        port=443,
    #        ssl=True,
    #        experiment="{{ dag_run.conf['experiment'] }}",
    #    )


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
#DATA {{ cwd }}/logs/{{ dag_run.conf['base'] }}-sum.yaml
#OUT {{ cwd }}/logs/{{ dag_run.conf['base'] }}-sum.job
cd {{ cwd }} && mkdir -p {{ cwd }}/logs/
rm -f 'logs/{{ dag_run.conf['base'] }}-sum.yaml'
FORCE=1 NO_FORCE_GAINREF=1 NO_PREAMBLE=1 APIX={{ apix }} FMDOSE={{ fmdose }} SUPERRES={{ superres }} KV={{ kev }} CS={{ cs }} PHASE_PLATE={{ phase_plate }} {% if params.software == 'EPU' %}SUMMED_FILE='{{ ti.xcom_pull( task_ids='summed_file' )[0] | replace( cwd, "." ) }}'{% endif %}  \
  {{ params.pipeline_script }} -t sum \
  {% if params.apply_gainref %}--gainref '{{ ti.xcom_pull( task_ids='gainref_file' )[0] | replace( cwd, "." ) }}'{% endif %} \
  --basename '{{ dag_run.conf['base'] }}' \
  '{{ ti.xcom_pull( task_ids='stack_file' )[-1] | replace( cwd, "." ) }}' \
    > './logs/{{ dag_run.conf['base'] }}-sum.yaml' 
""",
        params=parameters,
    )

    stack_file = FileGlobSensor( task_id='stack_file',
        filepath="{% set imaging_format = params.imaging_format if params.imaging_format else dag_run.conf['imaging_format'] %}{{ params.directory_prefix }}/{{ dag_run.conf['data_directory'] }}/raw/**/{{ dag_run.conf['base'] }}{% if imaging_format == '.mrc' %}?*.mrc{% elif imaging_format == '.tif' %}*.tif{% if params.daq_software == 'EPU' %}f{% endif %}{% endif %}",
        params={ 
            'daq_software': args['daq_software'],
            'imaging_format': args['imaging_format'] if 'imaging_format' in args  else None,
            'directory_prefix': args['directory_prefix'],
        },
        recursive=True,
        excludes=['gain-ref','_gain'],
        poke_interval=1,
        retries=2,
    )
    
    # always give the superres imgae and let the pipeline bin it down if necessary
    if args['apply_gainref'] == True:
        filepath="{% set m_file = 'm1' %}{{ params.directory_prefix }}/{{ dag_run.conf['data_directory'] }}/raw/GainRefs/*x1.{{ m_file }}*.dm4"
        if MICROSCOPE in ( 'TEMDELTA' ): 
            filepath="{% set m_file = 'm1' %}{{ params.directory_prefix }}/{{ dag_run.conf['data_directory'] }}/raw/GainRefs/*.gain"
        gainref_file = FileGlobSensor( task_id='gainref_file',
        #filepath="{% set superres = params.superres %}{% if superres == None and 'superres' in dag_run.conf %}{% set superres = dag_run.conf['superres'] in ( '1', 1, 'y' ) %}{% endif %}{{ params.directory_prefix }}/{{ dag_run.conf['data_directory'] }}/raw/GainRefs/*x1.m{% if superres %}1{% else %}2{% endif %}*.dm4",
            filepath=filepath,
            params={
            'daq_software': args['daq_software'],
            'superres': args['superres'] if 'superres' in args else None,
            'microscope': "{{ dag_run.conf['microscope'] }}",
            'directory_prefix': args['directory_prefix'],
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
{%- set superres = params.superres %}{% if superres == None and 'superres' in dag_run.conf %}{% set superres = 1 if dag_run.conf['superres'] in ( '1', 1, 'y' ) else 0 %}{% endif %}
{%- set phase_plate = params.phase_plate %}{% if phase_plate == None and 'phase_plate' in dag_run.conf %}{% set phase_plate = 1 if dag_run.conf['phase_plate'] in ( '1', 1, 'y' ) else 0 %}{% endif %}
{%- set cwd = params.directory_prefix + '/' + dag_run.conf['data_directory'].replace('//','/',10) %}
#DATA  {{ cwd }}/logs/{{ dag_run.conf['base'] }}-align.yaml
#OUT  {{ cwd }}/logs/{{ dag_run.conf['base'] }}-align.job
cd {{ cwd }} && mkdir -p {{ cwd }}/logs/
rm -f 'logs/{{ dag_run.conf['base'] }}-align.yaml' || true

RET=1
until [[ $RET != 1 ]]
do

  GPU=""
  until [[ $GPU != ""  ]]
  do
	  GPU=$(nvidia-smi pmon -c 1 | grep '\-   \-' | awk -v r=$RANDOM '{ a[n++]=$1 } END{ print a[int(r/32767*n)] }')
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

FORCE=1 LOCAL=/tmp NO_FORCE_GAINREF=1 APIX={{ apix }} FMDOSE={{ fmdose }} SUPERRES={{ superres }} KV={{ kev }} CS={{ cs }} PHASE_PLATE={{ phase_plate }}   \
GPU=$GPU \
{{ params.pipeline_script }} -t align \
  --gainref '{{ ti.xcom_pull( task_ids='gainref_file' )[0] | replace( cwd, "." ) }}' \
  --basename '{{ dag_run.conf['base'] }}' \
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
        dt="{{ ti.xcom_pull( task_ids='align', key='data' )['single_particle_analysis'][0]['files'][0]['create_timestamp'] }}",
        tags={
            'app': 'motioncor2',
            'state': 'aligned',
            'microscope': "{{ dag_run.conf['microscope'] }}",
        },
        fields="{{ ti.xcom_pull( task_ids='align', key='data' )['single_particle_analysis'][1]['data'] }}",
    )


    particle_pick = BashPlusOperator( task_id='particle_pick',
        queue='cpu',
        bash_command="""
{%- set kev = params.kev %}{% if params.kev == None and 'keV' in dag_run.conf %}{% set kev = dag_run.conf['keV'] %}{% endif %}
{%- set cs = params.cs %}{% if params.cs == None and 'cs' in dag_run.conf %}{% set cs = dag_run.conf['cs'] %}{% endif %}
{%- set apix = params.apix %}{% if params.apix == None and 'apix' in dag_run.conf %}{% set apix = dag_run.conf['apix'] %}{% endif %}
{%- set fmdose = params.fmdose %}{% if params.fmdose == None and 'fmdose' in dag_run.conf %}{% set fmdose = dag_run.conf['fmdose'] %}{% endif %}
{%- set superres = params.superres %}{% if superres == None and 'superres' in dag_run.conf %}{% set superres = 1 if dag_run.conf['superres'] in ( '1', 1, 'y' ) else 0 %}{% endif %}
{%- set phase_plate = params.phase_plate %}{% if phase_plate == None and 'phase_plate' in dag_run.conf %}{% set phase_plate = 1 if dag_run.conf['phase_plate'] in ( '1', 1, 'y' ) else 0 %}{% endif %}
{%- set cwd = params.directory_prefix + '/' + dag_run.conf['data_directory'].replace('//','/',10) -%}
#OUT {{ cwd }}/logs/{{ dag_run.conf['base'] }}-pick.job
#DATA {{ cwd }}/logs/{{ dag_run.conf['base'] }}-pick.yaml
cd {{ cwd }} && mkdir -p {{ cwd }}/logs/
rm -f 'logs/{{ dag_run.conf['base'] }}-pick.yaml'
FORCE=1 NO_PREAMBLE=1 NO_FORCE_GAINREF=1 APIX={{ apix }}   \
{{ params.pipeline_script }} -t pick \
  --gainref '{{ ti.xcom_pull( task_ids='gainref_file' )[0] | replace( cwd, "." ) }}' \
  --basename '{{ dag_run.conf['base'] }}' \
  '{{ ti.xcom_pull( task_ids='stack_file' )[-1] | replace( cwd, "." ) }}' \
    > './logs/{{ dag_run.conf['base'] }}-pick.yaml'
""",
        params=parameters,
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
#DATA {{ cwd }}/logs/{{ dag_run.conf['base'] }}-preview.yaml
#OUT {{ cwd }}/logs/{{ dag_run.conf['base'] }}-preview.job
cd {{ cwd }} && mkdir -p {{ cwd }}/logs/
{%- set sum_ctf = ti.xcom_pull( task_ids='sum', key='data' )['single_particle_analysis'][-1]['data'] %}
{%- set align = ti.xcom_pull( task_ids='align', key='data' )['single_particle_analysis'] %}
{%- set drift = align[1]['data'] %}
{%- set align_ctf = align[-1]['data'] %}
FORCE=1 NO_PREAMBLE=1 NO_FORCE_GAINREF=1 APIX={{ apix }} {% if params.software == 'EPU' %}SUMMED_FILE='{{ ti.xcom_pull( task_ids='summed_file' )[0] | replace( cwd, "." ) }}'{% endif %}  \
PROCESSED_SUM_RESOLUTION={{ sum_ctf['resolution'] }} PROCESSED_SUM_RESOLUTION_PERFORMANCE={{ sum_ctf['resolution_performance'] }}  \
PROCESSED_ALIGN_FIRST1={{ drift['first1'] }} PROCESSED_ALIGN_FIRST5={{ drift['first5'] }} PROCESSED_ALIGN_ALL={{ drift['all'] }} \
PROCESSED_ALIGN_RESOLUTION={{ align_ctf['resolution'] }} PROCESSED_ALIGN_RESOLUTION_PERFORMANCE={{ align_ctf['resolution_performance'] }} PROCESSED_ALIGN_ASTIGMATISM={{ align_ctf['astigmatism'] }} PROCESSED_ALIGN_CROSS_CORRELATION={{ align_ctf['cross_correlation'] }}  \
{{ params.pipeline_script }} -t preview \
  --gainref '{{ ti.xcom_pull( task_ids='gainref_file' )[0] | replace( cwd, "." ) }}' \
  --basename '{{ dag_run.conf['base'] }}' \
  '{{ ti.xcom_pull( task_ids='stack_file' )[-1] | replace( cwd, "." ) }}' \
    > './logs/{{ dag_run.conf['base'] }}-preview.yaml'

""",

        params=parameters,
    )

#    slack_preview = SlackAPIUploadFileOperator( task_id='slack_preview',
#        channel="{{ dag_run.conf['experiment'] | replace( ' ', '' ) | lower }}",
#        #channel="{{ dag_run.conf['experiment'][:21] | replace( ' ', '' ) | lower }}",
#        token=Variable.get('slack_' + os.path.splitext(os.path.basename(__file__))[0].split('_')[1].lower() ),
#        filepath="%s/{{ dag_run.conf['data_directory'] }}/{{ ti.xcom_pull( task_ids='previews', key='data' )['single_particle_analysis'][0]['files'][0]['path'] }}" % (args['directory_prefix'],),
#    )
  
    slack_preview = BashPlusOperator( task_id='slack_preview',
        queue='cpu',
    	bash_command="""
{% set cwd = params.directory_prefix + '/' + dag_run.conf['data_directory'].replace('//','/',10) -%}
#OUT {{ cwd }}/logs/{{ dag_run.conf['base'] }}-slack_preview.job
export TOKEN="{{ params.token }}"
export FILEPATH="{{ params.directory_prefix }}/{{ dag_run.conf['data_directory'] }}/{{ ti.xcom_pull( task_ids='previews', key='data' )['single_particle_analysis'][0]['files'][0]['path'] }}"
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
        #retries=2,
    )

    update_star_file = BashOperator( task_id='update_star_file',
        retries=2,
        bash_command="""
            export STAR_FILE=images.star
            cd {{ params.directory_prefix }}/{{ dag_run.conf['data_directory'] }}
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
        params={
            'directory_prefix': args['directory_prefix'],
        }
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
            filepath="{{ params.directory_prefix }}/{{ dag_run.conf['data_directory'] }}/raw/**/{{ dag_run.conf['base'] }}.*",
            params={
                'directory_prefix': args['directory_prefix'],
            },
            extensions=[ '.mrc', '.tiff' ],
            recursive=True,
            timeout=10,
            retries=3,
            poke_interval=1,
        )

        summed_file >> sum

    if args['apply_gainref'] == True:
        gainref_file >> sum
        gainref_file >> align

    stack_file >> sum
    stack_file >> align

    #if args['daq_software'] == 'EPU':
    #    parameter_file >> parse_parameters >> logbook_parameters
    #    #sum  >> logbook_parameters
    #    parse_parameters >> influx_parameters

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
