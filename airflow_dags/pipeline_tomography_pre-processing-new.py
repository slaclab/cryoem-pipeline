from airflow import DAG

from airflow.models import Variable

from airflow.models import BaseOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.sensors import BaseSensorOperator

from airflow.contrib.hooks import SSHHook
from airflow.hooks.http_hook import HttpHook

from airflow.operators import FileInfoSensor
from airflow.operators import LSFSubmitOperator, LSFJobSensor, LSFOperator

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


import logging
LOG = logging.getLogger(__name__)

args = {
    'owner': 'yee',
    'provide_context': True,
    'start_date': datetime( 2018,1,1 ),

    'ssh_connection_id':        'ssh_lsf_host',
    'logbook_connection_id':    'cryoem_logbook',
    'influx_host':              'influxdb01.slac.stanford.edu',

    'queue_name':   'cryoem-daq',
    'bsub':         'bsub',
    'bjobs':        'bjobs',
    'bkill':        'bkill',

    'convert_gainref':   False,
    'apply_gainref':     False,
    'daq_software':      '__imaging_software__',
    'max_active_runs':   10,
    # 'create_run':         False
    # 'apix':              1.35,
    # 'fmdose':           1.75,
    #'superres':         0,
    # 'imaging_format': '.tif',
}

software = {
    'imod':     { 'version': '4.9.7',   'module': 'imod-4.9.7-intel-17.0.4-2kdesbi' },
    'eman2':    { 'version': 'develop', 'module': 'eman2-develop-gcc-4.9.4-e5ufzef' },
    'ctffind4': { 'version': '4.1.10',  'module': 'ctffind4-4.1.10-intel-17.0.4-rhn26cm' },
    'motioncor2': { 'version': '1.1.0', 'module': 'motioncor2-1.1.0-gcc-4.8.5-zhoi3ww' },
}


def uploadExperimentalParameters2Logbook(ds, **kwargs):
    """Push the parameter key-value pairs to the elogbook"""
    data = kwargs['ti'].xcom_pull( task_ids='parse_parameters' )
    LOG.warn("data: %s" % (data,))
    raise AirflowSkipException('not yet implemented')









class FileSkipSensor(FileInfoSensor):
    def poke(self, context):
        LOG.info('Waiting for file %s (ext %s)' % (self.filepath,self.extensions) )
        files = []
        for f in glob.iglob( self.filepath, recursive=self.recursive ):
            add = False
            if self.extensions:
                for e in self.extensions:
                    if f.endswith( e ):
                        add = True
            else:
                add = True
            if add:
                if self.excludes:
                    for e in self.excludes:
                        if e in f:
                            add = False
                if add:
                    files.append(f)
        if len(files):
            LOG.info('found files: %s' % (files) )
            context['task_instance'].xcom_push(key='return_value',value=files)
            return True
        return False





class NotYetImplementedOperator(DummyOperator):
    ui_color = '#d3d3d3'





# 1) first a _basename_.txt file is created
# 2) then the raw data comes out in 
#  _basename_<\d+>_<\d{,3}>\[(\-)?\d+\.\d+]\]-<\d+>.mrc
# 3) when all the tilt angles are complete, a file is created
#  _basename_.rawtlt




###
# define the workflow
###
with DAG( os.path.splitext(os.path.basename(__file__))[0],
        description="Pre-processing of CryoEM data",
        schedule_interval=None,
        default_args=args,
        catchup=False,
        max_active_runs=args['max_active_runs'],
        concurrency=72,
        dagrun_timeout=1800,
    ) as dag:

    # hook to container host for lsf commands
    hook = SSHHook(ssh_conn_id=args['ssh_connection_id'])
    logbook_hook = HttpHook( http_conn_id=args['logbook_connection_id'], method='GET' )

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
    sum = LSFOperator( task_id='sum',
        ssh_hook=hook,
        env={
            'LSB_JOB_REPORT_MAIL': 'N',
            'MODULEPATH': '/afs/slac.stanford.edu/package/spack/share/spack/modules/linux-centos7-x86_64'
        },
        bsub=args['bsub'],
        retries=2,
        retry_delay=timedelta(seconds=1),
        lsf_script="""#!/bin/bash -l
#BSUB -o '{{ dag_run.conf['directory'] }}/summed/imod/{{ params.software.imod.version }}/{{ dag_run.conf['base'] }}_avg_gainrefd.job'
{% if params.convert_gainref %}#BSUB -w done({{ ti.xcom_pull( task_ids='convert_gainref' )['jobid'] }}){% endif %}
#BSUB -W 5
#BSUB -We 1
#BSUB -n 1

mkdir -p {{ dag_run.conf['directory'] }}/summed/imod/{{ params.software.imod.version }}/
module load {{ params.software.imod.module }}
cd -- "$( dirname {{ ti.xcom_pull( task_ids='stack_file' )[-1].replace(' ','\ ') }} )"
avgstack > '{{ dag_run.conf['directory'] }}/summed/imod/{{ params.software.imod.version }}/{{ dag_run.conf['base'] }}_avg_gainrefd.log' <<-'__AVGSTACK_EOF__'
{{ ti.xcom_pull( task_ids='stack_file' )[-1] }}
{%- if params.apply_gainref %}
/tmp/{{ dag_run.conf['base'] }}_avg.mrcs
/
__AVGSTACK_EOF__

clip mult -n 16 \
'/tmp/{{ dag_run.conf['base'] }}_avg.mrcs' \
{{ ti.xcom_pull( task_ids='gainref_file')[0] | replace( '.dm4', '.mrc' ) }} \
'/tmp/{{ dag_run.conf['base'] }}_avg_gainrefd.mrc'

{%- else %}
/tmp/{{ dag_run.conf['base'] }}_avg.mrcs
/
__AVGSTACK_EOF__

clip flipx \
  '/tmp/{{ dag_run.conf['base'] }}_avg.mrcs' \
  '/tmp/{{ dag_run.conf['base'] }}_avg_gainrefd.mrc'
{% endif %}

module load {{ params.software.eman2.module }}
export PYTHON_EGG_CACHE='/tmp'
{%- set imaging_format = params.imaging_format if params.imaging_format else dag_run.conf['imaging_format'] %}
e2proc2d.py \
'/tmp/{{ dag_run.conf['base'] }}_avg_gainrefd.mrc'  \
'{{ ti.xcom_pull( task_ids='stack_file' )[-1].replace( imaging_format, '.jpg' ) }}' \
--process filter.lowpass.gauss:cutoff_freq=0.05

# copy files
cp -f '/tmp/{{ dag_run.conf['base'] }}_avg_gainrefd.mrc' '{{ dag_run.conf['directory'] }}/summed/imod/{{ params.software.imod.version }}/{{ dag_run.conf['base'] }}_avg_gainrefd.mrc'
rm -f '/tmp/{{ dag_run.conf['base'] }}_avg.mrcs'
rm -f '/tmp/{{ dag_run.conf['base'] }}_avg_gainrefd.mrc'
""",
        params={
            'apply_gainref': args['apply_gainref'],
            'convert_gainref': args['convert_gainref'],
            'software': software,
            'imaging_format': args['imaging_format'] if 'imaging_format' in args  else None,
        }
    )

    influx_sum = LSFJob2InfluxOperator( task_id='influx_sum',
        job_name='sum',
        xcom_task_id='sum',
        host=args['influx_host'],
        experiment="{{ dag_run.conf['experiment'] }}",
    )

    summed_file = FileInfoSensor( task_id='summed_file',
        filepath="{{ dag_run.conf['directory'] }}/summed/imod/{{ params.software.imod.version }}/{{ dag_run.conf['base'].replace(']','[]]',1).replace('[','[[]',1)  }}_avg_gainrefd.mrc",
        params={
            'daq_software': args['daq_software'],
            'software': software,
        },
        recursive=True,
        poke_interval=1,
    )

    logbook_summed_file = LogbookRegisterFileOperator( task_id='logbook_summed_file',
        file_info='summed_file',
        http_hook=logbook_hook,
        experiment="{{ dag_run.conf['experiment'].split('_')[0] }}",
        run="{{ dag_run.conf['base'] }}"
    )

    summed_preview = FileInfoSensor( task_id='summed_preview',
        filepath="{%- set imaging_format = params.imaging_format if params.imaging_format else dag_run.conf['imaging_format'] %}{{ ti.xcom_pull( task_ids='stack_file' )[-1].replace( imaging_format, '.jpg' ).replace(']','[]]',1).replace('[','[[]',1) }}",
        params={
            'software': software,
            'imaging_format': args['imaging_format'] if 'imaging_format' in args  else None,
        },
        recursive=True,
        poke_interval=1,
    )


    ctffind_summed = LSFSubmitOperator( task_id='ctffind_summed',
        ssh_hook=hook,
        env={
            'LSB_JOB_REPORT_MAIL': 'N',
            'MODULEPATH': '/afs/slac.stanford.edu/package/spack/share/spack/modules/linux-centos7-x86_64'
        },
        bsub=args['bsub'],
        retries=2,
        retry_delay=timedelta(seconds=1),
        lsf_script="""#!/bin/bash -l
#BSUB -o '{{ dag_run.conf['directory'] }}/summed/{% if params.daq_software == 'SerialEM' %}imod/{{ params.software.imod.version }}/{% endif %}ctffind4/{{ params.software.ctffind4.version }}/{{ dag_run.conf['base'] }}_ctf.job'
#BSUB -W 6
#BSUB -We 3
#BSUB -n 1

module load {{ params.software.ctffind4.module }}
mkdir -p {{ dag_run.conf['directory'] }}/summed/{% if params.daq_software == 'SerialEM' %}imod/{{ params.software.imod.version }}/{% endif %}ctffind4/{{ params.software.ctffind4.version }}/
cd {{ dag_run.conf['directory'] }}/summed/{% if params.daq_software == 'SerialEM' %}imod/{{ params.software.imod.version }}/{% endif %}ctffind4/{{ params.software.ctffind4.version }}/
ctffind > '{{ dag_run.conf['base'] }}_ctf.log' <<-'__CTFFIND_EOF__'
{{ ti.xcom_pull( task_ids='summed_file' )[0] }}
{{ dag_run.conf['base'] }}_ctf.mrc
{% if params.superres in ( '1', 1, 'y' ) or ( 'superres' in dag_run.conf and dag_run.conf['superres'] in ( '1', 1, 'y' ) ) %}{% if params.apix %}{{ params.apix | float / 2 }}{% else %}{{ dag_run.conf['apix'] | float / 2 }}{% endif %}{% else %}{% if params.apix %}{{ params.apix }}{% else %}{{ dag_run.conf['apix'] }}{% endif %}{% endif %}
{{ dag_run.conf['keV'] }}
{{ dag_run.conf['cs'] or 2.7 }}
0.1
512
30
4
1000
50000
200
no
no
yes
100
{% if 'phase_plate' in dag_run.conf and dag_run.conf['phase_plate'] %}yes
0
1.571
0.1
{%- else %}no{% endif %}
no
__CTFFIND_EOF__
""",
        params={
            'daq_software': args['daq_software'],
            'apix': args['apix'] if 'apix' in args else None,
            'superres': args['superres'] if 'superres' in args else None,
            'software': software,
        }
    )

    convert_summed_ctf_preview = LSFOperator( task_id='convert_summed_ctf_preview',
        ssh_hook=hook,
        bsub=args['bsub'],
        bjobs=args['bjobs'],
        env={
            'LSB_JOB_REPORT_MAIL': 'N',
            'MODULEPATH': '/afs/slac.stanford.edu/package/spack/share/spack/modules/linux-centos7-x86_64'
        },
        poke_interval=1,
        retries=2,
        retry_delay=timedelta(seconds=1),
        lsf_script="""#!/bin/bash -l
#BSUB -o {{ dag_run.conf['directory'] }}/summed/{% if params.daq_software == 'SerialEM' %}imod/{{ params.software.imod.version }}/{% endif %}ctffind4/4.1.10/{{ dag_run.conf['base'] }}_ctf_preview.job
#BSUB -w "done({{ ti.xcom_pull( task_ids='ctffind_summed' )['jobid'] }})"
#BSUB -W 5
#BSUB -We 1
#BSUB -n 1

module load {{ params.software.eman2.module }}
export PYTHON_EGG_CACHE='/tmp'
cd {{ dag_run.conf['directory'] }}/summed/{% if params.daq_software == 'SerialEM' %}imod/{{ params.software.imod.version }}/{% endif %}ctffind4/{{ params.software.ctffind4.version }}/
e2proc2d.py --writejunk \
    {{ dag_run.conf['base'] }}_ctf.mrc \
    {{ dag_run.conf['base'] }}_ctf.jpg
""",
        params={ 
            'daq_software': args['daq_software'],
            'software': software,
        },
    )

    influx_summed_preview = LSFJob2InfluxOperator( task_id='influx_summed_preview',
        job_name='summed_preview',
        xcom_task_id='convert_summed_ctf_preview',
        host=args['influx_host'],
        experiment="{{ dag_run.conf['experiment'] }}",
    )

    ctf_summed = LSFJobSensor( task_id='ctf_summed',
        ssh_hook=hook,
        jobid="{{ ti.xcom_pull( task_ids='ctffind_summed' )['jobid'] }}",
        retries=2,
        retry_delay=timedelta(seconds=1),
        poke_interval=1,
    )

    influx_summed_ctf = LSFJob2InfluxOperator( task_id='influx_summed_ctf',
        job_name='ctf_summed',
        xcom_task_id='ctf_summed',
        host=args['influx_host'],
        experiment="{{ dag_run.conf['experiment'] }}",
    )

    summed_ctf_preview = FileInfoSensor( task_id='summed_ctf_preview',
        filepath="{{ dag_run.conf['directory'] }}/**/{{ dag_run.conf['base'].replace(']','[]]',1).replace('[','[[]',1)  }}_ctf.jpg",
        recursive=True,
        poke_interval=1,
    )

    summed_ctf_file = FileInfoSensor( task_id='summed_ctf_file',
        filepath="{{ dag_run.conf['directory'] }}/**/{{ dag_run.conf['base'].replace(']','[]]',1).replace('[','[[]',1)  }}_ctf.mrc",
        recursive=True,
        poke_interval=1,
    )

    logbook_summed_ctf_file= LogbookRegisterFileOperator( task_id='logbook_summed_ctf_file',
        file_info='summed_ctf_file',
        http_hook=logbook_hook,
        experiment="{{ dag_run.conf['experiment'].split('_')[0] }}",
        run="{{ dag_run.conf['base'] }}"
    )


    summed_ctf_data = Ctffind4DataSensor( task_id='summed_ctf_data',
        filepath="{{ dag_run.conf['directory'] }}/**/{{ dag_run.conf['base'].replace(']','[]]',1).replace('[','[[]',1)  }}_ctf.txt",
        recursive=True,
        poke_interval=1,
    )

    influx_summed_ctf_data = GenericInfluxOperator( task_id='influx_summed_ctf_data',
        host=args['influx_host'],
        experiment="{{ dag_run.conf['experiment'] }}",
        measurement="cryoem_data",
        dt="{{ ti.xcom_pull( task_ids='stack_file' )[-1] }}",
        tags={
            'app': 'ctffind',
            'version': software['ctffind4']['version'],
            'state': 'unaligned',
            'microscope': "{{ dag_run.conf['microscope'] }}",
        },
        tags2="{{ ti.xcom_pull( task_ids='summed_ctf_data', key='context' ) }}",
        fields="{{ ti.xcom_pull( task_ids='summed_ctf_data' ) }}",
    )

    resubmit_ctffind_summed = BashOperator( task_id='resubmit_ctffind_summed',
        trigger_rule='one_failed',
        bash_command="""
        airflow clear -t ctffind_summed -c -d -s {{ ts }} -e {{ ts }} {{ dag | replace( '<DAG: ', '' ) | replace( '>', '' ) }} &
        ( sleep 10; airflow clear -t resubmit_ctffind_summed -c -d -s {{ ts }} -e {{ ts }} {{ dag | replace( '<DAG: ', '' ) | replace( '>', '' ) }} ) &
        """
    )

    convert_summed_ctf_preview >> resubmit_ctffind_summed
    ctf_summed >> resubmit_ctffind_summed

    ###
    #
    ###
    stack_file = FileInfoSensor( task_id='stack_file',
        filepath="{% set imaging_format = params.imaging_format if params.imaging_format else dag_run.conf['imaging_format'] %}{{ dag_run.conf['directory'] }}/raw/**/{{ dag_run.conf['base'].replace(']','[]]',1).replace('[','[[]',1)  }}*{{ imaging_format }}",
        params={ 
            'imaging_format': args['imaging_format'] if 'imaging_format' in args  else None,
        },
        recursive=True,
        excludes=['gain-ref',],
        poke_interval=1,
    )
    

    fix_header = LSFOperator( task_id='fix_header',
        ssh_hook=hook,
        env={
            'LSB_JOB_REPORT_MAIL': 'N',
            'MODULEPATH': '/afs/slac.stanford.edu/package/spack/share/spack/modules/linux-centos7-x86_64'
        },
        bsub=args['bsub'],
        bjobs=args['bjobs'],
        poke_interval=1,
        retries=2,
        retry_delay=timedelta(seconds=1),
        lsf_script="""#!/bin/bash -l
#BSUB -o {{ ti.xcom_pull( task_ids='aligned_file' )[-1] | replace( dag_run.conf['imaging_format'], '_fix_header.job' ) }}
#BSUB -W 1
#BSUB -We 1
#BSUB -n 1

###
# fix the header
###
module load {{ params.software.eman2.module }}
export PYTHON_EGG_CACHE='/tmp'
cd -- "$( dirname {{ ti.xcom_pull( task_ids='stack_file' )[0].replace(' ','\ ') }} )"
e2procheader.py \
    '{{ ti.xcom_pull( task_ids='aligned_file' )[0] }}' \
    --stem apix --stemval {{ dag_run.conf['apix'] }} --valtype float
""",
        params={
            'software': software,
        }
    )

    logbook_stack_file = LogbookRegisterFileOperator( task_id='logbook_stack_file',
        file_info='stack_file',
        http_hook=logbook_hook,
        experiment="{{ dag_run.conf['experiment'].split('_')[0] }}",
        run="{{ dag_run.conf['base'] }}"
    )
    

    if args['convert_gainref']:

        gainref_file = FileInfoSensor( task_id='gainref_file',
            filepath="{{ dag_run.conf['directory'] }}/**/{% if params.daq_software == 'SerialEM' %}{% if params.superres in ( '1', 1, 'y' ) or ( 'superres' in dag_run.conf and dag_run.conf['superres'] in ( '1', 1, 'y' ) ) %}Super{% else %}Count{% endif %}Ref*.dm4{% else %}{{ dag_run.conf['base'] }}-gain-ref.dm4{% endif %}",
            params={
                'daq_software': args['daq_software'],
                'superres': args['superres'] if 'superres' in args else None,
            },
            recursive=True,
            poke_interval=1,
        )
        
        logbook_gainref_file = LogbookRegisterFileOperator( task_id='logbook_gainref_file',
            file_info='gainref_file',
            http_hook=logbook_hook,
            experiment="{{ dag_run.conf['experiment'].split('_')[0] }}",
            run="{{ dag_run.conf['base'] }}"
        )

        ####
        # convert gain ref to mrc
        ####
        convert_gainref = LSFSubmitOperator( task_id='convert_gainref',
            ssh_hook=hook,
            env={
                'LSB_JOB_REPORT_MAIL': 'N',
                'MODULEPATH': '/afs/slac.stanford.edu/package/spack/share/spack/modules/linux-centos7-x86_64'
            },
            bsub=args['bsub'],
            lsf_script="""#!/bin/bash -l
#BSUB -o {{ ti.xcom_pull( task_ids='gainref_file' )[0] | replace( '.dm4', '.job' ) }}
#BSUB -W 3
#BSUB -We 1
#BSUB -n 1

if [ -e {{ ti.xcom_pull( task_ids='gainref_file' )[0] | replace( '.dm4', '.mrc' ) }} ]; then
  echo "gainref file {{ ti.xcom_pull( task_ids='gainref_file' )[0] | replace( '.dm4', '.mrc' ) }} already exists"
else
  module load {{ params.software.eman2.module }}
  export PYTHON_EGG_CACHE='/tmp'
  cd -- "$( dirname {{ ti.xcom_pull( task_ids='gainref_file' )[0] }} )"
  echo e2proc2d.py {% if params.rotate_gainref > 0 %}--rotate {{ params.rotate_gainref }}{% endif %}{{ ti.xcom_pull( task_ids='gainref_file' )[0] }} {{ ti.xcom_pull( task_ids='gainref_file' )[0] | replace( '.dm4', '.mrc' ) }}
  e2proc2d.py {% if params.rotate_gainref > 0 %}--rotate {{ params.rotate_gainref }}{% endif %}{{ ti.xcom_pull( task_ids='gainref_file' )[0] }} {{ ti.xcom_pull( task_ids='gainref_file' )[0] | replace( '.dm4', '.mrc' ) }}  --inplace
fi
            """,
            params={
                'daq_software': args['daq_software'],
                'rotate_gainref': 0,
                'software': software,
            }
                
        )

        new_gainref = LSFJobSensor( task_id='new_gainref',
            ssh_hook=hook,
            jobid="{{ ti.xcom_pull( task_ids='convert_gainref' )['jobid'] }}",
            poke_interval=1,
        )

        influx_new_gainref = LSFJob2InfluxOperator( task_id='influx_new_gainref',
            job_name='convert_gainref',
            xcom_task_id='new_gainref',
            host=args['influx_host'],
            experiment="{{ dag_run.conf['experiment'] }}",
        )

    if args['apply_gainref']:

        new_gainref_file = FileInfoSensor( task_id='new_gainref_file',
            filepath="{% if not params.convert_gainref %}{{ dag_run.conf['directory'] }}/**/gain-ref.mrc{% else %}{{ dag_run.conf['directory'] }}/**/{% if params.daq_software == 'SerialEM' %}{% if params.superres in ( '1', 1, 'y' ) or ( 'superres' in dag_run.conf and dag_run.conf['superres'] in ( '1', 1, 'y' ) ) %}Super{% else %}Count{% endif %}Ref*.mrc{% else %}{{ dag_run.conf['base'] }}-gain-ref.mrc{% endif %}{% endif %}",
            recursive=True,
            params={
                'daq_software': args['daq_software'],
                'convert_gainref': args['convert_gainref'],
                'superres': args['superres'] if 'superres' in args else None,
            },
            poke_interval=1,
        )

    ###
    # align the frame
    ###

    motioncorr_stack = LSFSubmitOperator( task_id='motioncorr_stack',
        ssh_hook=hook,
        env={
            'LSB_JOB_REPORT_MAIL': 'N',
            'MODULEPATH': '/afs/slac.stanford.edu/package/spack/share/spack/modules/linux-centos7-x86_64'
        },
        bsub=args['bsub'],
        retries=2,
        retry_delay=timedelta(seconds=1),
        lsf_script="""#!/bin/bash -l
#BSUB -o '{{ dag_run.conf['directory'] }}/aligned/motioncor2/1.1.0/{{ dag_run.conf['base'] }}_aligned.job'
{% if params.convert_gainref %}#BSUB -w "done({{ ti.xcom_pull( task_ids='convert_gainref' )['jobid'] }})"{% endif %}
#BSUB -gpu "num=1"
#BSUB -W 15
#BSUB -We 7
#BSUB -n 1

module load {{ params.software.motioncor2.module }}
mkdir -p {{ dag_run.conf['directory'] }}/aligned/motioncor2/{{ params.software.motioncor2.version }}/
cd {{ dag_run.conf['directory'] }}/aligned/motioncor2/{{ params.software.motioncor2.version }}/
MotionCor2  \
    -In{% set format = params.imaging_format %}{% if format == None %}{% set format = dag_run.conf['imaging_format'] %}{% endif %}{% if format == '.mrc' %}Mrc{% elif format == '.tif' %}Tiff{% endif %} '{{ ti.xcom_pull( task_ids='stack_file' )[-1] }}' \
{% if params.apply_gainref %}{% if params.convert_gainref %}   -Gain {{ ti.xcom_pull( task_ids='gainref_file' )[0] | replace( '.dm4', '.mrc' ) }} {% else %}    -Gain {{ ti.xcom_pull( task_ids='new_gainref_file' )[-1] }} {% endif %}{% endif -%}\
    -OutMrc   '{{ dag_run.conf['base'] }}_aligned.mrc' \
    -LogFile  '{{ dag_run.conf['base'] }}_aligned.log' \
    -kV       {{ dag_run.conf['keV'] }} \
    -Bft      {% if 'preprocess/align/motioncor2/bft' in dag_run.conf %}{{ dag_run.conf['preprocess/align/motioncor2/bft'] }}{% else %}500 150{% endif %} \
    -PixSize  {% if params.superres in ( '1', 1, 'y' ) or ( 'superres' in dag_run.conf and dag_run.conf['superres'] in ( '1', 1, 'y' ) ) %}{% if params.apix %}{{ params.apix | float / 2 }}{% else %}{{ dag_run.conf['apix'] | float / 2 }}{% endif %}{% else %}{% if params.apix %}{{ params.apix }}{% else %}{{ dag_run.conf['apix'] }}{% endif %}{% endif %} \
    -FtBin    {% if 'superres' in dag_run.conf and dag_run.conf['superres'] in ( '1', 1, 'y' ) %}2{% else %}1{% endif %} \
    -Patch    {% if 'preprocess/align/motioncor2/patch' in dag_run.conf %}{{ dag_run.conf['preprocess/align/motioncor2/patch'] }}{% else %}5 5{% endif %} \
    -Throw    {% if 'preprocess/align/motioncor2/throw' in dag_run.conf %}{{ dag_run.conf['preprocess/align/motioncor2/throw'] }}{% else %}0{% endif %} \
    -Trunc    {% if 'preprocess/align/motioncor2/trunc' in dag_run.conf %}{{ dag_run.conf['preprocess/align/motioncor2/trunc'] }}{% else %}0{% endif %} \
    -Iter     {% if 'preprocess/align/motioncor2/iter' in dag_run.conf %}{{ dag_run.conf['preprocess/align/motioncor2/iter'] }}{% else %}10{% endif %} \
    -Tol      {% if 'preprocess/align/motioncor2/tol' in dag_run.conf %}{{ dag_run.conf['preprocess/align/motioncor2/tol'] }}{% else %}0.5{% endif %} \
    -OutStack {% if 'preprocess/align/motioncor2/outstack' in dag_run.conf %}{{ dag_run.conf['preprocess/align/motioncor2/outstack'] }}{% else %}0{% endif %}  \
    -InFmMotion {% if 'preprocess/align/motioncor2/infmmotion' in dag_run.conf %}{{ dag_run.conf['preprocess/align/motioncor2/infmmotion'] }}{% else %}1{% endif %}  \
    -Gpu      {{ params.gpu }}
""",
        params={
            'gpu': 0,
            'apply_gainref': args['apply_gainref'],
            'convert_gainref': args['convert_gainref'],
            'apix': args['apix'] if 'apix' in args else None,
            'fmdose': args['fmdose'] if 'fmdose' in args else None,
            'superres': args['superres'] if 'superres' in args else None,
            'software': software,
            'imaging_format': args['imaging_format'] if 'imaging_format' in args  else None,
        },
    )
    
    align = LSFJobSensor( task_id='align',
        ssh_hook=hook,
        jobid="{{ ti.xcom_pull( task_ids='motioncorr_stack' )['jobid'] }}",
        poke_interval=5,
        retries=2,
    )

    influx_aligned = LSFJob2InfluxOperator( task_id='influx_aligned',
        job_name='align_stack',
        xcom_task_id='align',
        host=args['influx_host'],
        experiment="{{ dag_run.conf['experiment'] }}",
    )

    drift_data = MotionCor2DataSensor( task_id='drift_data',
        filepath="{{ dag_run.conf['directory'] }}/aligned/motioncor2/1.1.0/{{ dag_run.conf['base'].replace(']','[]]',1).replace('[','[[]',1)  }}_aligned.log0-*Full.log",
        poke_interval=5,
        timeout=30,
    )

    influx_drift_data = GenericInfluxOperator( task_id='influx_drift_data',
        host=args['influx_host'],
        experiment="{{ dag_run.conf['experiment'] }}",
        measurement="cryoem_data",
        dt="{{ ti.xcom_pull( task_ids='stack_file' )[-1] }}",
        tags={
            'app': 'motioncor2',
            'version': software['motioncor2']['version'],
            'state': 'aligned',
            'microscope': "{{ dag_run.conf['microscope'] }}",
        },
        fields="{{ ti.xcom_pull( task_ids='drift_data' ) }}",
    )


    convert_aligned_preview = LSFOperator( task_id='convert_aligned_preview',
        ssh_hook=hook,
        env={
            'LSB_JOB_REPORT_MAIL': 'N',
            'MODULEPATH': '/afs/slac.stanford.edu/package/spack/share/spack/modules/linux-centos7-x86_64'
        },
        bsub=args['bsub'],
        retries=2,
        retry_delay=timedelta(seconds=1),
        poke_interval=1,
        lsf_script="""#!/bin/bash -l
#BSUB -o {{ dag_run.conf['directory'] }}/aligned/motioncor2/1.1.0/{{ dag_run.conf['base'] }}_aligned_preview.job
#BSUB -w "done({{ ti.xcom_pull( task_ids='motioncorr_stack' )['jobid'] }})"
#BSUB -W 10
#BSUB -We 2
#BSUB -n 1

module load {{ params.software.eman2.module }}
export PYTHON_EGG_CACHE='/tmp'
cd {{ dag_run.conf['directory'] }}/aligned/motioncor2/{{ params.software.motioncor2.version }}/
e2proc2d.py \
    {{ dag_run.conf['base'] }}_aligned.mrc \
    {{ dag_run.conf['base'] }}_aligned.jpg \
    --process filter.lowpass.gauss:cutoff_freq=0.05
""",
        params={
            'software': software,
        }
    )


    influx_aligned_preview = LSFJob2InfluxOperator( task_id='influx_aligned_preview',
        job_name='aligned_preview',
        xcom_task_id='convert_aligned_preview',
        host=args['influx_host'],
        experiment="{{ dag_run.conf['experiment'] }}",
    )

    aligned_file = FileInfoSensor( task_id='aligned_file',
        filepath="{{ dag_run.conf['directory'] }}/**/{{ dag_run.conf['base'].replace(']','[]]',1).replace('[','[[]',1)  }}_aligned.mrc",
        recursive=True,
        poke_interval=1,
    )
    
    logbook_aligned_file = LogbookRegisterFileOperator( task_id='logbook_aligned_file',
        file_info='aligned_file',
        http_hook=logbook_hook,
        experiment="{{ dag_run.conf['experiment'].split('_')[0] }}",
        run="{{ dag_run.conf['base'] }}"
    )

    aligned_preview = FileInfoSensor( task_id='aligned_preview',
        filepath="{{ dag_run.conf['directory'] }}/**/{{ dag_run.conf['base'].replace(']','[]]',1).replace('[','[[]',1)  }}_aligned.jpg",
        recursive=True,
        poke_interval=1,
    )


    ctffind_aligned = LSFSubmitOperator( task_id='ctffind_aligned',
        # beware we do not use aligned_file's xcom as it would not have completed yet
        ssh_hook=hook,
        env={
            'LSB_JOB_REPORT_MAIL': 'N',
            'MODULEPATH': '/afs/slac.stanford.edu/package/spack/share/spack/modules/linux-centos7-x86_64'
        },
        bsub=args['bsub'],
        retries=2,
        retry_delay=timedelta(seconds=1),
        lsf_script="""#!/bin/bash -l
#BSUB -o {{ dag_run.conf['directory'] }}/aligned/motioncor2/{{ params.software.motioncor2.version }}/ctffind4/{{ params.software.ctffind4.version }}/{{ dag_run.conf['base'] }}_aligned_ctf.job
{% if True %}#BSUB -w "done({{ ti.xcom_pull( task_ids='motioncorr_stack' )['jobid'] }})"{% endif %}
#BSUB -W 3
#BSUB -We 1
#BSUB -n 1

module load ctffind4-4.1.10-intel-17.0.4-rhn26cm
mkdir -p {{ dag_run.conf['directory'] }}/aligned/motioncor2/{{ params.software.motioncor2.version }}/ctffind4/{{ params.software.ctffind4.version }}/
cd {{ dag_run.conf['directory'] }}/aligned/motioncor2/{{ params.software.motioncor2.version }}/ctffind4/{{ params.software.ctffind4.version }}/
ctffind > {{ dag_run.conf['base'] }}_aligned_ctf.log <<-'__CTFFIND_EOF__'
{{ dag_run.conf['directory'] }}/aligned/motioncor2/{{ params.software.motioncor2.version }}/{{ dag_run.conf['base'] }}_aligned.mrc
{{ dag_run.conf['base'] }}_aligned_ctf.mrc
{% if params.apix %}{{ params.apix }}{% else %}{{ dag_run.conf['apix'] }}{% endif %}
{{ dag_run.conf['keV'] }}
{{ dag_run.conf['cs'] or 2.7 }}
0.1
512
30
4
1000
50000
200
no
no
yes
100
{% if 'phase_plate' in dag_run.conf and dag_run.conf['phase_plate'] %}yes
0
1.571
0.1
{%- else %}no{% endif %}
no
__CTFFIND_EOF__
""",
        params={
            'apix': args['apix'] if 'apix' in args else None,
            'software': software,
        }
    )

    convert_aligned_ctf_preview = LSFOperator( task_id='convert_aligned_ctf_preview',
        ssh_hook=hook,
        env={
            'LSB_JOB_REPORT_MAIL': 'N',
            'MODULEPATH': '/afs/slac.stanford.edu/package/spack/share/spack/modules/linux-centos7-x86_64'
        },
        bsub=args['bsub'],
        poke_interval=1,
        retries=2,
        retry_delay=timedelta(seconds=1),
        lsf_script="""#!/bin/bash -l
#BSUB -o {{ dag_run.conf['directory'] }}/aligned/motioncor2/{{ params.software.motioncor2.version }}/ctffind4/{{ params.software.ctffind4.version }}/{{ dag_run.conf['base'] }}_aligned_ctf.job
#BSUB -w "done({{ ti.xcom_pull( task_ids='ctffind_aligned' )['jobid'] }})"
#BSUB -W 5 
#BSUB -We 1
#BSUB -n 1

module load {{ params.software.eman2.module }}
export PYTHON_EGG_CACHE='/tmp'
cd {{ dag_run.conf['directory'] }}/aligned/motioncor2/{{ params.software.motioncor2.version }}/ctffind4/{{ params.software.ctffind4.version }}/
e2proc2d.py \
    {{ dag_run.conf['base'] }}_aligned_ctf.mrc \
    {{ dag_run.conf['base'] }}_aligned_ctf.jpg
""",
        params={
            'software': software,
        }
    )

    influx_ctf_preview = LSFJob2InfluxOperator( task_id='influx_ctf_preview',
        job_name='ctf_preview',
        xcom_task_id='convert_aligned_ctf_preview',
        host=args['influx_host'],
        experiment="{{ dag_run.conf['experiment'] }}",
    )


    ctf_aligned = LSFJobSensor( task_id='ctf_aligned',
        ssh_hook=hook,
        jobid="{{ ti.xcom_pull( task_ids='ctffind_aligned' )['jobid'] }}",
        retries=2,
        retry_delay=timedelta(seconds=1),
        poke_interval=1,
    )

    influx_ctf_aligned = LSFJob2InfluxOperator( task_id='influx_ctf_aligned',
        job_name='ctf_aligned',
        xcom_task_id='ctf_aligned',
        host=args['influx_host'],
        experiment="{{ dag_run.conf['experiment'] }}",
    )

    aligned_ctf_file = FileInfoSensor( task_id='aligned_ctf_file',
        filepath="{{ dag_run.conf['directory'] }}/**/{{ dag_run.conf['base'].replace(']','[]]',1).replace('[','[[]',1)  }}_aligned_ctf.mrc",
        recursive=True,
        poke_interval=1,
    )
    
    logbook_aligned_ctf_file = LogbookRegisterFileOperator( task_id='logbook_aligned_ctf_file',
        file_info='aligned_ctf_file',
        http_hook=logbook_hook,
        experiment="{{ dag_run.conf['experiment'].split('_')[0] }}",
        run="{{ dag_run.conf['base'] }}"
    )

    aligned_ctf_preview = FileInfoSensor( task_id='aligned_ctf_preview',
        filepath="{{ dag_run.conf['directory'] }}/**/{{ dag_run.conf['base'].replace(']','[]]',1).replace('[','[[]',1)  }}_aligned_ctf.jpg",
        recursive=1,
        poke_interval=1,
    )

    aligned_ctf_data = Ctffind4DataSensor( task_id='aligned_ctf_data',
        filepath="{{ dag_run.conf['directory'] }}/**/{{ dag_run.conf['base'].replace(']','[]]',1).replace('[','[[]',1)  }}_aligned_ctf.txt",
        recursive=True,
    )

    influx_aligned_ctf_data = GenericInfluxOperator( task_id='influx_aligned_ctf_data',
        host=args['influx_host'],
        experiment="{{ dag_run.conf['experiment'] }}",
        measurement="cryoem_data",
        dt="{{ ti.xcom_pull( task_ids='stack_file' )[-1] }}",
        tags={
            'app': 'ctffind',
            'version': software['ctffind4']['version'],
            'state': 'aligned',
            'microscope': "{{ dag_run.conf['microscope'] }}",
        },
        tags2="{{ ti.xcom_pull( task_ids='aligned_ctf_data', key='context' ) }}",
        fields="{{ ti.xcom_pull( task_ids='aligned_ctf_data' ) }}",
    )

    previews = BashOperator( task_id='previews',
        bash_command="""
            timestamp=$(TZ=America/Los_Angeles date +"%Y-%m-%d %H:%M:%S" -r '{{ ti.xcom_pull( task_ids='stack_file' )[0] }}')

            # summed preview
            mkdir -p {{ dag_run.conf['directory'] }}/summed/previews
            cd {{ dag_run.conf['directory'] }}/summed/previews/
            convert \
                -resize '512x512^' -extent '512x512' \
                '{{ ti.xcom_pull( task_ids='summed_preview' )[0] }}' \
                -flip '{{ ti.xcom_pull( task_ids='summed_ctf_preview' )[0] }}' \
                +append -font DejaVu-Sans -pointsize 28 -fill yellow -draw 'text 520,492 "'${timestamp}'"' \
                +append -font DejaVu-Sans -pointsize 28 -fill yellow -draw 'text 844,492 \"{{ '%0.1f' | format(ti.xcom_pull( task_ids='summed_ctf_data' )['resolution']) }}Å ({{ '%d' | format(ti.xcom_pull( task_ids='summed_ctf_data' )['resolution_performance'] * 100) }}%)\"' \
                '/tmp/{{ dag_run.conf['base'] }}_sidebyside.jpg'

            # aligned preview
            mkdir -p {{ dag_run.conf['directory'] }}/aligned/previews/
            cd {{ dag_run.conf['directory'] }}/aligned/previews/
            convert \
                -resize '512x512^' -extent '512x512' \
                '{{ ti.xcom_pull( task_ids='aligned_preview' )[0] }}' \
                '{{ ti.xcom_pull( task_ids='aligned_ctf_preview' )[0] }}' \
                +append \
                -font DejaVu-Sans -pointsize 28 -fill orange -draw 'text 402,46 \"{{ '%0.3f' | format(ti.xcom_pull( task_ids='drift_data' )['drift']) }}\"' \
                +append  \
                -font DejaVu-Sans -pointsize 28 -fill orange -draw 'text 844,46 \"{{ '%0.1f' | format(ti.xcom_pull( task_ids='aligned_ctf_data' )['resolution']) }}Å ({{ '%d' | format(ti.xcom_pull( task_ids='aligned_ctf_data' )['resolution_performance'] * 100) }}%)\"' \
                '/tmp/{{ dag_run.conf['base'] }}_aligned_sidebyside.jpg'

            # quad preview
            mkdir -p {{ dag_run.conf['directory'] }}/previews/
            cd {{ dag_run.conf['directory'] }}/previews/
            convert \
                '/tmp/{{ dag_run.conf['base'] }}_sidebyside.jpg' \
                '/tmp/{{ dag_run.conf['base'] }}_aligned_sidebyside.jpg' \
                -append \
                '{{ dag_run.conf['base'] }}_full_sidebyside.jpg'

            #  cleanup
            rm -f /tmp/{{ dag_run.conf['base'] }}_sidebyside.jpg /tmp/{{ dag_run.conf['base'] }}_aligned_sidebyside.jpg
        """
    )

    previews_file = FileInfoSensor( task_id='previews_file',
        filepath="{{ dag_run.conf['directory'] }}/**/{{ dag_run.conf['base'].replace(']','[]]',1).replace('[','[[]',1)  }}_full_sidebyside.jpg"
    )

    logbook_previews_file = LogbookRegisterFileOperator( task_id='logbook_previews_file',
        file_info='previews_file',
        http_hook=logbook_hook,
        experiment="{{ dag_run.conf['experiment'].split('_')[0] }}",
        run="{{ dag_run.conf['base'] }}"
    )

    slack_full_preview = SlackAPIUploadFileOperator( task_id='slack_full_preview',
        channel="{{ dag_run.conf['experiment'][:21] | replace( ' ', '' ) | lower }}",
        token=Variable.get('slack_token'),
        filepath="{{ dag_run.conf['directory'] }}/previews/{{ dag_run.conf['base'] }}_full_sidebyside.jpg",
        retries=2,
    )

    logbook_run_params = LogbookRegisterRunParamsOperator(task_id='logbook_run_params',
        http_hook=logbook_hook,
        experiment="{{ dag_run.conf['experiment'].split('_')[0] }}",
        run="{{ dag_run.conf['base'] }}",
        retries=2,
    )


    # check for end of tilt files
    tilts_complete = FileInfoSensor( task_id='previews_file',
        filepath="{{ dag_run.conf['directory'] }}/**/{{ dag_run.conf['base'].replace(']','[]]',1).replace('[','[[]',1)  }}.rawtlt"
    )

    reconstruct = DummyOperator( task_id='reconstruct' )



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

        
    stack_file >> sum >> summed_preview
    sum >> summed_file
    sum >> influx_sum
        
    stack_file >> logbook_stack_file
    stack_file >> motioncorr_stack
    
    summed_file >> ctffind_summed
    summed_file >> logbook_summed_file
    
    ctffind_summed >> ctf_summed
    
    ctffind_summed >> convert_summed_ctf_preview >> influx_summed_preview
    ctf_summed >> influx_summed_ctf

    previews >> previews_file >> logbook_previews_file
    summed_preview >> previews
    summed_ctf_preview >> previews

    ctf_summed >> logbook_run_params
    convert_summed_ctf_preview >> summed_ctf_preview
    ctf_summed >> summed_ctf_file >> logbook_summed_ctf_file
    ctf_summed >> summed_ctf_data

    summed_ctf_data >> previews
    summed_ctf_data >> influx_summed_ctf_data

    
    motioncorr_stack >> convert_aligned_preview

    if args['convert_gainref']:
        gainref_file >> convert_gainref
        new_gainref >> influx_new_gainref
        convert_gainref >> new_gainref
        new_gainref >> new_gainref_file
        gainref_file >> logbook_gainref_file

    if args['apply_gainref']:
        if not args['convert_gainref']:
            new_gainref_file >> motioncorr_stack
            if args['daq_software'] == 'SerialEM':
                new_gainref_file >> sum
        else:
            convert_gainref >> motioncorr_stack
            if args['daq_software'] == 'SerialEM':
                convert_gainref >> sum       

    motioncorr_stack >> align
    #align >> aligned_stack_file
    align >> influx_aligned
    align >> drift_data >> influx_drift_data 
    drift_data >> previews

    motioncorr_stack >> previews
    
    ctf_aligned >> aligned_ctf_file >> logbook_aligned_ctf_file
    convert_aligned_ctf_preview >> aligned_ctf_preview
    convert_aligned_ctf_preview >> influx_ctf_preview

    ctf_aligned >> aligned_ctf_data
    aligned_ctf_data >> previews
    aligned_ctf_data >> influx_aligned_ctf_data

    align >> logbook_run_params
    previews_file >> logbook_run_params
    
    align >> aligned_file >> logbook_aligned_file
    aligned_file >> fix_header
    motioncorr_stack >> ctffind_aligned >> ctf_aligned >> logbook_run_params
    ctffind_aligned >> convert_aligned_ctf_preview
    convert_aligned_preview >> aligned_preview
    convert_aligned_preview >> influx_aligned_preview

    aligned_preview >> previews
    aligned_ctf_preview >> previews

    previews >> slack_full_preview

    ctf_aligned >> influx_ctf_aligned


    aligned_file >> tilts_complete >> reconstruct
