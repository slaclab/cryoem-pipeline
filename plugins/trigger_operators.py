

from airflow.plugins_manager import AirflowPlugin

from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow_multi_dagrun.operators import TriggerMultiDagRunOperator
from airflow.exceptions import AirflowException, AirflowSkipException

from airflow import settings
import contextlib


from airflow.models import DagRun, DagBag
from airflow.utils.state import State

from pathlib import Path
from datetime import datetime
from time import sleep
import re

import logging
LOG = logging.getLogger(__name__)


def trigger_preprocessing(context):
    """ calls the preprocessing dag: pass the filenames of the stuff to process """
    # we have a jpg, xml, small mrc and large mrc, and gainref dm4 file
    # assume the common filename is the same and allow the  preprocessing dag wait for the other files? what happens if two separate calls to the same dag occur?
    found = {}
    if context == None:
        return

    for f in context['ti'].xcom_pull( task_ids='rsync', key='return_value' ):
        this = Path(f).resolve().stem
        for pattern in ( r'\-\d+$', r'\-gain\-ref$' ):
            if re.search( pattern, this):
                this = re.sub( pattern, '', this)
            #LOG.info("mapped: %s -> %s" % (f, this))
        # LOG.info("this: %s, f: %s" % (this,f))

        # EPU SPA
        # epu2
        if '_Fractions' in f and f.endswith('.xml'):
            base = re.sub( '_Fractions', '', this )
            found[base] = True

        # epu1
        elif f.endswith('.xml') and not f.startswith('Atlas') and not f.startswith('Tile_') and '_Data_' in f and not '_Fractions' in f:
            found[this] = True

        # tomo4 tomography file
        elif '[' in this and ']' in this:
            t = this.split(']')[0] + ']'
            found[t] = True

        # serialem
        else:

            # tomogram
            m = re.match( r'^(?P<base>.*)\_(?P<seq>\d{5}\_\d{3})\_(?P<angle>\-?\d{1,2}\.\d)\_(?P<other>.*)?\.(mrc|tif)$', f )
            if m:
                n = m.groupdict()
                fn = "%s_%s[%s]" % (n['base'], n['seq'], n['angle'])
                # ignore countref files
                if not 'CountRef' in n['base']:
                  found[ fn ] = True
    
            # single particle
            else:
                m = re.match( r'^(?P<base>.*\_\d\d\d\d\d)(\_.*)?\.(tif|tiff|mrc)$', f )
                if m:
                    b = m.groupdict()['base']
                    LOG.info('found %s' % (b,))
                    found[b] = True
                else:
                    # stupid new epu
                    m = re.match( r'^(?P<base>.*)\_fractions\.(tiff|mrc)$', f )
                    if m:
                        n = m.groupdict()['base']
                        if n != 'PreviewDoseFraction':
                            found[n] = True

    # LOG.info("FOUND: %s" % (found,))
    for base_filename,_ in sorted(found.items()):
        sample = context['ti'].xcom_pull( task_ids='config', key='sample' )
        inst = context['ti'].xcom_pull( task_ids='config', key='instrument' )
        name = context['ti'].xcom_pull( task_ids='config', key='experiment' )

        run_id = '%s__%s' % (name, base_filename)
        #dro = DagRunOrder(run_id=run_id)

        d = sample['params']

        d['data_directory'] = context['ti'].xcom_pull( task_ids='config', key='directory_suffix' ) + '/' + sample['guid'] 
        d['base'] = base_filename
        d['experiment'] = name
        d['microscope'] = inst['_id']
        d['cs'] = inst['params']['cs']
        d['keV'] = inst['params']['keV']

        # only do single-particle 
        LOG.info('triggering run id %s with %s' % (run_id,d))
        #dro.payload = d
        #yield dro
        dro = {'run_id': run_id, 'payload': payload}
        yield dro
    return


def trigger_null(context):
    raise AirflowSkipException('Intentionally not doing it')

@contextlib.contextmanager
def create_session():
    session = settings.Session()
    try:
        yield session
        session.expunge_all()
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()

class TriggerMultiDagRunOperator(TriggerDagRunOperator):
    template_fields = ('trigger_dag_id', 'dry_run' )
    def __init__(self, dry_run=False, *args,**kwargs):
        self.dry_run = dry_run
        self.python_callable = trigger_preprocessing
        super( TriggerMultiDagRunOperator, self ).__init__( *args, **kwargs )
    def execute(self, context):
        count = 0
        self.python_callable = trigger_preprocessing
        dry = True if self.dry_run.lower() == 'true' else False
        try:
            with create_session() as session:
                dbag = DagBag(settings.DAGS_FOLDER)
                trigger_dag = dbag.get_dag(self.trigger_dag_id)
                # get dro's
                for dro in self.python_callable(context):
                    if dro and not dry:
                        try:
                            dr = trigger_dag.create_dagrun(
                                run_id=dro['run_id'],
                                state=State.RUNNING,
                                conf=dro['payload'],
                                external_trigger=True)
                            # LOG.info("Creating DagRun %s", dr)
                            session.add(dr)
                            count = count + 1
                        except Exception as e:
                            LOG.error("Could not add %s: %s" % (dro.run_id,e))
                        session.commit()
        except Exception as e:
            LOG.error("Could not connect to airflow")
        if count == 0:
            raise AirflowSkipException('No external dags triggered')


class TriggerPlugin(AirflowPlugin):
    name = 'trigger_plugin'
    operators = [TriggerMultiDagRunOperator,]
