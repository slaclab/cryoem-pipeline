from airflow import DAG
import os
from datetime import datetime
import pickle
from pprint import pformat
import re

from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.bash_operator import BashOperator

import signal
from subprocess import Popen, STDOUT, PIPE
from tempfile import gettempdir, NamedTemporaryFile


from airflow.exceptions import AirflowSkipException, AirflowException
from airflow.utils.decorators import apply_defaults

from airflow.utils.decorators import apply_defaults
from airflow.utils.file import TemporaryDirectory

import logging
LOG = logging.getLogger(__name__)

class MyPostgresQuerySensor(PostgresOperator):
    def execute(self, context):
        self.hook = PostgresHook(postgres_conn_id=self.postgres_conn_id,
                                 schema=self.database)
        rows = self.hook.get_records(self.sql, parameters=self.parameters)
        context['ti'].xcom_push( key='return_value', value=rows )

class MyPostgresOperator(PostgresOperator):
    def execute(self, context):
        self.hook = PostgresHook(postgres_conn_id=self.postgres_conn_id,
                                 schema=self.database)
        self.hook.run(self.sql, parameters=self.parameters)

class StuckTasksSensor(PostgresOperator):
    """
    looks for tasks that seem to be stuck in queued
    """
    @apply_defaults
    def __init__(
            self, database, experiments_from_xcom, pickle_file,
            sql=None, postgres_conn_id='postgres_default', autocommit=False, parameters=None,
            *args, **kwargs):
        super(PostgresOperator, self).__init__( database=database, sql=sql, *args, **kwargs )
        self.exps = experiments_from_xcom
        self.sql = sql
        self.pickle_file = pickle_file
        self.postgres_conn_id = postgres_conn_id
        self.autocommit = autocommit
        self.parameters = parameters
        self.database = database

    def execute(self, context):
        self.hook = PostgresHook(postgres_conn_id=self.postgres_conn_id,
                                 schema=self.database)

        # load up the pickle file for the last time this ran
        if os.path.isfile( self.pickle_file ):
            with open(self.pickle_file, 'rb') as f:
                last = pickle.load(f)
        else:
            last = {}

        this = {}
        for exp in [ x[0] for x in context['ti'].xcom_pull( task_ids=self.exps )]:
            sql = """
                SELECT dr.run_id, ti.task_id, ti.execution_date
                FROM dag_run as dr
                INNER JOIN
                (
                    SELECT dag_id, task_id, state, execution_date
                    FROM task_instance
                    WHERE state = ANY( ARRAY['queued','shutdown'] )
                ) as ti
                ON dr.dag_id = ti.dag_id AND dr.execution_date = ti.execution_date  AND dr.state = 'running' AND  dr.dag_id='{}'  order by run_id;
            """.format( exp )
            # LOG.info("sql %s: %s" % (exp,sql,))
            rows = self.hook.get_records(sql, parameters=self.parameters)
            for r in rows:
                run_id = r[0]
                task_id = r[1]
                at = r[2]
                if not exp in this:
                    this[exp] = {}
                if not run_id in this[exp]:
                    this[exp][run_id] = {}
                this[exp][run_id][task_id] = at

        LOG.info("LAST: \n%s" % (pformat(last),))
        LOG.info("THIS: \n%s" % (pformat(this),))

        stuck_jobs = []
        # go through the list of experiments, if we have items that still show up, assumed that its stuck
        for exp, runs in this.items():
            if exp in last:
                for run_id, task_ids in runs.items():
                    if run_id in last[exp]:
                        for task_id, at in task_ids.items():
                            if task_id in last[exp][run_id]:
                                LOG.warn( "Still queuing %s: %s %s" % (exp, run_id, task_id))
                                stuck_jobs.append( "airflow run %s %s %s" % (exp, task_id, str(at).replace(' ', 'T')))

        context['ti'].xcom_push( key='return_value', value=stuck_jobs )

        # save state
        with open(self.pickle_file, 'wb') as f:
            pickle.dump( this, f )


class SkippableBashOperator(BashOperator):
    """ will skip if there is no text to execute """
    def execute(self,context):
        """ if the bash command contains only spaces, then send a skip exception """
        LOG.info("Running command: %s" % (self.bash_command,))
        if self.bash_command.strip() == '':
            raise AirflowSkipException('empty bash command script')

        # LOG.info("Tmp dir root location: \n %s", gettempdir())
        self.lineage_data = self.bash_command

        with TemporaryDirectory(prefix='airflowtmp') as tmp_dir:
            with NamedTemporaryFile(dir=tmp_dir, prefix=self.task_id) as f:

                f.write(bytes(self.bash_command, 'utf_8'))
                f.flush()
                fname = f.name
                script_location = os.path.abspath(fname)
                LOG.info(
                    "Temporary script location: %s",
                    script_location
                )

                def pre_exec():
                    # Restore default signal disposition and invoke setsid
                    for sig in ('SIGPIPE', 'SIGXFZ', 'SIGXFSZ'):
                        if hasattr(signal, sig):
                            signal.signal(getattr(signal, sig), signal.SIG_DFL)
                    os.setsid()

                # LOG.info("Running command: %s", self.bash_command)
                sp = Popen(
                    ['bash', fname],
                    stdout=PIPE, stderr=STDOUT,
                    cwd=tmp_dir, env=self.env,
                    preexec_fn=pre_exec)

                self.sp = sp

                LOG.info("Output:")
                line = ''
                for line in iter(sp.stdout.readline, b''):
                    line = line.decode(self.output_encoding).rstrip()
                    LOG.info(line)
                sp.wait()
                LOG.info(
                    "Command exited with return code %s",
                    sp.returncode
                )

                if sp.returncode:
                    raise AirflowException("Bash command failed")

        if self.xcom_push_flag:
            return line


class FailedTasksSensor(PostgresOperator):
    """
    looks for dags that have failed and attempts to succeed them
    """
    @apply_defaults
    def __init__(
            self, database, experiments_from_xcom, 
            sql=None, postgres_conn_id='postgres_default', autocommit=False, parameters=None,
            *args, **kwargs):
        super(FailedTasksSensor, self).__init__( database=database, sql=sql, *args, **kwargs )
        self.exps = experiments_from_xcom
        self.sql = sql
        self.postgres_conn_id = postgres_conn_id
        self.autocommit = autocommit
        self.parameters = parameters
        self.database = database

    def execute(self, context):
        self.hook = PostgresHook(postgres_conn_id=self.postgres_conn_id,
                                 schema=self.database)

        this = {}
        for exp in [ x[0] for x in context['ti'].xcom_pull( task_ids=self.exps )]:
            
            if exp.endswith('_daq') or exp in ( 'airflow_stats_to_influx', 'purge_operational_history', 'experiment_cleanup' ):
                continue
            
            sql = """
                SELECT dr.dag_id, dr.run_id, dr.state as DAGstate, ti.task_id, ti.state, ti.execution_date
                FROM dag_run as dr
                INNER JOIN
                (
                    SELECT dag_id, task_id, state, execution_date
                    FROM task_instance
                    WHERE ( state != 'success' AND state != 'skipped' ) OR state IS NULL
                ) as ti
                ON dr.dag_id = ti.dag_id AND dr.execution_date = ti.execution_date  AND ( dr.state = 'failed' ) AND  dr.dag_id = '{}'   order by execution_date;
            """.format( exp )
            # LOG.info("sql %s: %s" % (exp,sql,))
            rows = self.hook.get_records(sql, parameters=self.parameters)
            # context['ti'].xcom_push( key=exp, value=rows )
            for r in rows:
                dag_id = r[0]
                run_id = r[1]
                task_id = r[3]
                at = r[5]
                if not exp in this:
                    this[exp] = {}
                if not run_id in this[exp]:
                    this[exp][run_id] = { 'tasks': [], 'at': at }
                this[exp][run_id]['tasks'].append( task_id )

        # LOG.info("THIS: \n%s" % (pformat(this),))
        retry = []
        for exp, d in this.items():
            context['ti'].xcom_push( key=exp, value=d )
            for run_id, e in d.items():
                at = str(e['at']).replace(' ','T')
                # no data!
                if len( list(set(['stack_file','gainref_file']) & set(e['tasks'])) ):
                    LOG.error("%s / %s skipping unrecoverable error (no raw data): %s" % (exp,run_id,e['tasks'],))
                    #retry.append( 'airflow clear -c %s -s %s -e %s  -t stack_file --downstream   # %s' % (exp, at, at, run_id ) )
                # need to redo motion correction
                elif len( list(set(['align','aligned_file','new_gainref_file','convert_aligned_ctf_preview']) & set(e['tasks'])) ):
                    LOG.info("%s / %s alignment failed - resubmit: %s" % (exp,run_id,e['tasks'],))
                    retry.append( 'airflow clear -c %s -s %s -e %s  -t convert_gainref --downstream   # %s' % (exp, at, at, run_id ) )
                # need to redo sum ctf
                elif len( list(set(['sum','summed_file','summed_preview']) & set(e['tasks'])) ):
                    LOG.info("%s / %s sum failed - resubmit: %s" % (exp,run_id,e['tasks'],))
                    retry.append( 'airflow clear -c %s -s %s -e %s  -t sum --downstream   # %s' % (exp, at, at, run_id ) )
                # particle picking
                elif len( list(set(['particle_pick']) & set(e['tasks'])) ):
                    LOG.info("%s / %s particle picking failed - resubmit: %s" % (exp,run_id,e['tasks'],))
                    retry.append( 'airflow clear -c %s -s %s -e %s  -t particle_pick --downstream   # %s' % (exp, at, at, run_id ) )
                # dunno...
                #elif len( list(set(['particle_pick_preview']) & set(e['tasks'])) ):
                #    LOG.error("%s / %s skipping unrecoverable error (particle picking failed): %s" % (exp,run_id,e['tasks'],))
                # just needs a kick, so initiate a null task
                elif len( list(set(['resubmit_stack']) & set(e['tasks'])) ):
                    LOG.info("%s / %s stalled: %s" % (exp,run_id,e['tasks'],))
                    # use a nulloperator task to start the dag again
                    retry.append( 'airflow clear -c %s -s %s -e %s  -t invite_slack_users   # %s' % (exp, at, at, run_id ) )
                elif len( list(set(['convert_summed_ctf_preview','ctf_summed']) & set(e['tasks'])) ):
                    LOG.info("%s / %s summed failed - resubmit: %s" % (exp,run_id,e['tasks'],))
                    retry.append( 'airflow clear -c %s -s %s -e %s  -t ctffind_summed --downstream   # %s' % (exp, at, at, run_id ) )
                # singular tasks
                else:
                    LOG.info("%s / %s unknown: %s" % (exp,run_id,e['tasks'],))
                    for t in e['tasks']:
                        retry.append( 'airflow clear -c %s -s %s -e %s  -t %s   # %s' % (exp, at, at, t, run_id ) )


        context['ti'].xcom_push( key='return_value', value=retry )

with DAG( os.path.splitext(os.path.basename(__file__))[0],
        description="Attempt to fix common failures",
        schedule_interval="33 * * * *",
        catchup=False,
        start_date=datetime(2018,1,1),
        max_active_runs=1,
        concurrency=1
    ) as dag:

    experiments = MyPostgresQuerySensor(task_id='experiments',
        database="airflow",
        sql="""
        select dag_id from dag where is_paused='f' and dag_id NOT LIKE 'tem%';
        """
    )
    stuck = StuckTasksSensor(task_id='stuck',
        database="airflow",
        experiments_from_xcom='experiments',
        pickle_file='/gpfs/slac/cryo/fs1/exp/.daq/experiment_cleanup-paused.pickle',
    )
    experiments >> stuck

    unstick = SkippableBashOperator( task_id='unstick',
        bash_command="""
          {%- for l in ti.xcom_pull(task_ids='stuck') %}
            {{ l }}
          {% endfor -%}
        """
    )
    stuck >> unstick
    
    failed = FailedTasksSensor( task_id='failed',
        database="airflow",
        experiments_from_xcom='experiments',
    )
    experiments >> failed
    
    unfail = SkippableBashOperator( task_id='unfail',
        bash_command="""
          {%- for l in ti.xcom_pull(task_ids='failed') -%}
            {{ l }}
          {% endfor -%}
        """
    )
    failed >> unfail
