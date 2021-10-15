
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, ShortCircuitOperator
from airflow.models import BaseOperator

from airflow.exceptions import AirflowException, AirflowSkipException

import json
import yaml
from pathlib import Path
import re

from datetime import datetime, timedelta
from datetime import timezone
from pytz import utc

import logging
LOG = logging.getLogger(__name__)

def dtAsZ(o):
    if isinstance(o, datetime):
        return o.astimezone(utc).isoformat().replace('+00:00','Z')

###
# grab some experimental details that we need ###
###
def parseConfigurationFile(**kwargs):
    with open( kwargs['configuration_file'], 'r' ) as stream:
        try:
            d = yaml.load(stream)
            for k,v in d.items():
                kwargs['task_instance'].xcom_push(key=k,value=v)
            exp_dir = '%s_%s' % (d['experiment']['name'], d['experiment']['microscope'])
            kwargs['task_instance'].xcom_push(key='experiment_directory', value='%s/%s/' % ( kwargs['destination_directory'], exp_dir))
            kwargs['task_instance'].xcom_push(key='dry_run', value=d['experiment']['dry_run'] if 'dry_run' in d['experiment'] else True)
        except Exception as e:
            raise AirflowException('Error creating destination directory: %s' % (e,))
    return kwargs['configuration_file']
class EnsureConfigurationExistsSensor(ShortCircuitOperator):
    """ monitor for configuration defined on tem """
    def __init__(self,configuration_file,destination_directory,*args,**kwargs):
        super(EnsureConfigurationExistsSensor,self).__init__(
            python_callable=parseConfigurationFile, 
            op_kwargs={ 'configuration_file': configuration_file, 'destination_directory': destination_directory }, 
            *args, **kwargs)


    
def logbook_configuration(**kwargs):

    http_hook = kwargs['http_hook']
    microscope = kwargs['microscope']
    #experiment_directory = kwargs['base_directory']
    
    http_hook.method = 'GET'
    
    # get instrument info
    r = http_hook.run( '/cryoem-data/lgbk/ws/instruments' )
    if r.status_code in (403,404,500):
        logging.error(" could not fetch instruments: %s" % (r.text,))
        return False
    # logging.info("instruments: %s" % i )
    found = False
    for inst in json.loads( r.text )['value']:
        if '_id' in inst and microscope.lower() == inst['_id'].lower():
            kwargs['task_instance'].xcom_push(key='instrument', value=inst)
            found = True
    if not found:
        logging.error(" could not find instrument %s" % (microscope,))
        return False
        
    # get experiment and sample
    r = http_hook.run( '/cryoem-data/lgbk/ws/activeexperiments' )
    if r.status_code in (403,404,500):
        logging.error(" could not fetch active experiment: %s" % (r.text,))
        return False

    active = json.loads( r.text )
    if active['success']:
        # print("Active experiments: %s" % active['value'])
        for tem in active['value']:
            # print("Found %s" % (tem,))
            if 'instrument' in tem and tem['instrument'].lower() == microscope.lower():
                # logging.info("found %s: %s" % (args['tem'],tem))

                ###
                # get the active experiment and sample
                ###
                if not 'name' in tem:
                    return False
                experiment_name = tem['name']
                if not re.match( r'^\d{8}\-C\w{3,4}$', experiment_name ):
                    logging.error("Experiment name %s invalid" % (experiment_name,))
                    return False

                s = http_hook.run( '/cryoem-data/lgbk/%s/ws/current_sample_name' % ( experiment_name, ) )
                sample_name = json.loads( s.text )['value']
                logging.info("active sample: %s" % (sample_name,))
                if sample_name == None:
                    logging.error("No active sample on %s" % (tem['name'],))
                    return False
                    
                # get sample parameters
                p = http_hook.run( '/cryoem-data/lgbk/%s/ws/samples' % ( experiment_name, ) )
                sample_params = {}
                sample_guid = None
                for d in json.loads( p.text )['value']:
                    #logging.info("SAMPLE: %s" % (d,))
                    if d['name'] == sample_name and 'current' in d and d['current']:
                        sample_params = d['params']
                        sample_guid = d['_id']
                kwargs['task_instance'].xcom_push(key='sample', value={
                    'name': sample_name,
                    'guid': sample_guid,
                    'params': sample_params
                } )
                
                parent_fileset = experiment_name[:6]
                folder = experiment_name + '_' + microscope.upper()
                kwargs['task_instance'].xcom_push(key='experiment', value=folder )
                suffix = parent_fileset + '/' + experiment_name + '_' + microscope.upper()
                kwargs['task_instance'].xcom_push(key='directory_suffix', value=suffix )
                kwargs['task_instance'].xcom_push(key='base_directory', value=kwargs['base_directory'] )
                
                ###
                # get list of collaborators for file permissions
                ###
                a = http_hook.run( '/cryoem-data/lgbk/%s/ws/collaborators' % (experiment_name, ) )
                collaborators = json.loads( a.text )
                logging.info( " collaborators: %s" % (collaborators,))
                kwargs['task_instance'].xcom_push(key='collaborators', value=[ u['uidNumber'] for u in collaborators['value'] if u['is_group'] == False ] if 'value' in collaborators else [] )

                # check if exp is older than
                if kwargs['ignore_older_than']:
                    exp_date = datetime.strptime(experiment_name.split('-')[0], '%Y%m%d')
                    now = datetime.now()
                    logging.info("EXP: %s / %s (days %s)" % (exp_date,now,kwargs['ignore_older_than']) )
                    if ( now - exp_date > timedelta(kwargs['ignore_older_than']) ):
                        logging.info("Experiment is too old to be active!") 
                        return False
 
                return True
                

class LogbookConfigurationSensor(ShortCircuitOperator):
    """ monitor for configuration defined on tem """
    def __init__(self,http_hook,microscope,base_directory,ignore_older_than=None,*args,**kwargs):
        super(LogbookConfigurationSensor,self).__init__(
            python_callable=logbook_configuration, 
            op_kwargs={ 'http_hook': http_hook, 'microscope': microscope, 'base_directory': base_directory, 'ignore_older_than': ignore_older_than }, 
            *args, **kwargs)



# this shoudl probably be run as part of teh daq
#class LogbookCreateRunOperator(BaseOperator):
#    """ monitor for configuration defined on tem """
#    template_fields = ('experiment','run')
#    
#    def __init__(self,http_hook,experiment,run,*args,**kwargs):
#        super(LogbookCreateRunOperator,self).__init__(*args, **kwargs)
#        self.experiment = experiment
#        self.run = run
#        self.http_hook = http_hook
#
#    def execute(self, context): 
#        self.http_hook.method = 'GET'
#        r = self.http_hook.run( '/cryoem-data/run_control/%s/ws/start_run?run_num=%s' % (self.experiment, self.run ) )
#        if r.status_code in (403,404,500):
#            logging.error(" could not initiate run %s for experiment %s: %s" % (self.run, self.experiment, r.text,))
#            return False
#        return True


class LogbookCreateRunOperator(BaseOperator):
    template_fields = ('experiment',)
    def __init__(self,http_hook,experiment,from_task='rsync',from_key='return_value',*args,**kwargs):
        super(LogbookCreateRunOperator,self).__init__(*args, **kwargs)
        self.http_hook = http_hook
        self.experiment = experiment
        self.from_task = from_task
        self.from_key = from_key
    def execute(self, context):
        found = {}
        self.http_hook.method = 'GET'
        for f in context['task_instance'].xcom_pull(task_ids=self.from_task,key=self.from_key):
            this = Path(f).resolve().stem
            for pattern in ( r'\-\d+$', r'\-gain\-ref$' ):
                if re.search( pattern, this):
                    this = re.sub( pattern, '', this)
                # LOG.warn("mapped: %s -> %s" % (f, this))
            #LOG.info("this: %s, f: %s" % (this,f))
            # EPU: only care about the xml file for now (let the dag deal with the other files
            if f.endswith('.xml') and not f.startswith('Atlas') and not f.startswith('Tile_') and '_Data_' in f:
                #LOG.warn("found EPU metadata %s" % this )
                found[this] = True
            # tomo
            elif '[' in this and ']' in this and ( f.endswith('.mrc') or f.endswith('.tif') ) :
                #LOG.info("FOUND: %s" % (this,))
                found[this] = True
            # serialEM: just look for tifs
            elif f.endswith('.tif') or f.endswith('.mrc'):
                m = re.match( r'^(?P<base>.*\_\d\d\d\d\d)(\_.*)?\.(tif|mrc)$', f )
                if m:
                    #LOG.info('found %s' % (m.groupdict()['base'],) )
                    found[m.groupdict()['base']] = True
            
        if len(found.keys()) == 0:
            raise AirflowSkipException('No runs')

        self.http_hook.method = 'GET'
        for base_filename,_ in sorted(found.items()):
            try:
                r = self.http_hook.run( '/cryoem-data/run_control/%s/ws/start_run?run_num=%s' % (self.experiment, base_filename ) )
                LOG.info('run %s: %s' % (base_filename,r.text)) 
            except Exception as e:
                # if r.status_code in (403,404,500):
                LOG.error(" could not initiate run %s for experiment %s: %s" % (base_filename, self.experiment, e,))

class LogbookRegisterFileOperator(BaseOperator):
    """ register the file information into the logbook """
    template_fields = ('experiment','run')
    
    def __init__(self,http_hook,file_info,experiment,run,index=0,*args,**kwargs):
        super(LogbookRegisterFileOperator,self).__init__(*args, **kwargs)
        self.experiment = experiment
        self.run = run
        self.http_hook = http_hook
        self.file_info = file_info
        self.index = index

    def execute(self, context): 

        data = context['task_instance'].xcom_pull( task_ids=self.file_info, key='info' )[self.index]
        data['run_num'] = self.run
        
        self.http_hook.method = 'POST'
        r = self.http_hook.run( '/cryoem-data/lgbk/%s/ws/register_file' % (self.experiment,), data=json.dumps([data,]), headers={'content-type': 'application/json'} )
        #logging.info("%s" % (json.dumps([data,]),))
        if r.status_code in (403,404,500):
            logging.error(" could not register file %s on run %s for experiment %s: %s" % (self.file_info, self.run, self.experiment, r.text,))
            return False
        return True
    

class LogbookRegisterRunParamsOperator(BaseOperator):
    """ register the run information into the logbook """
    template_fields = ('experiment','run')
    
    def __init__(self,http_hook,experiment,run,*args,**kwargs):
        super(LogbookRegisterRunParamsOperator,self).__init__(*args, **kwargs)
        self.experiment = experiment
        self.run = run
        self.http_hook = http_hook

    def execute(self, context): 

        data = {}
        for k,v in context['task_instance'].xcom_pull( task_ids='drift_data', key='return_value' ).items():
            data[k] = v
        for task in ( 'summed_ctf_data', 'aligned_ctf_data' ):
            prefix = 'unaligned' if task == 'summed_ctf_data' else 'aligned'
            for k, v in context['task_instance'].xcom_pull( task_ids=task, key='context' ).items():
                if k == 'pixel_size':
                    k = '%s_pixel_size' % prefix
                data[k] = v
            for k, v in context['task_instance'].xcom_pull( task_ids=task, key='return_value' ).items():
                data['%s_%s' % (prefix,k)] = v

        data['preview'] = context['task_instance'].xcom_pull( task_ids='previews_file', key='info' )[0]['path']
        
        # data['run_num'] = self.run
        LOG.warn("DATA: %s" % data )

        self.http_hook.method = 'POST'
        r = self.http_hook.run( '/cryoem-data/run_control/%s/ws/add_run_params?run_num=%s' % (self.experiment,self.run), data=json.dumps(data), headers={'content-type': 'application/json'} )
        if r.status_code in (403,404,500):
            logging.error(" could not register run params on run %s for experiment %s: %s" % (self.run, self.experiment, r.text,))
            return False
        return True


class MultiLogbookRegisterFileOperator(BaseOperator):
    """ register the file information into the logbook """
    template_fields = ('experiment','run')

    def __init__(self,http_hook,experiment,run,index=0,*args,**kwargs):
        super(MultiLogbookRegisterFileOperator,self).__init__(*args, **kwargs)
        self.experiment = experiment
        self.run = run
        self.http_hook = http_hook
        self.index = index

    def execute(self, context):
        data = []
        for task in self. upstream_task_ids:
            LOG.info("parent: %s" % (task,))
            try:
                this = context['task_instance'].xcom_pull( task_ids=task, key='info' )[self.index]
                this['run_num'] = self.run
                data.append(this)
            except Exception as e:
                LOG.warn(" %s" % (e,))
        try:
            self.http_hook.method = 'POST'
            LOG.info("  %s" % (json.dumps(data),))
            r = self.http_hook.run( '/cryoem-data/lgbk/%s/ws/register_file' % (self.experiment,), data=json.dumps(data), headers={'content-type': 'application/json'} )
            if r.status_code in (403,404,500):
                raise Exception("could not register file %s on run %s for experiment %s: %s" % (self.file_info, self.run, self.experiment, r.text,))
        except Exception as e:
            LOG.warn(" %s" % (e,))
        return True



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
                                f['run_num'] = self.run
                                files.append(f)
            except Exception as e:
                LOG.warn(" %s" % (e,))
        try:
            self.http_hook.method = 'POST'
            LOG.info("%s: %s" % (self.run, files,))
            #LOG.info(">>>>  %s" % (json.dumps(files, default=dtAsZ ),) )
            r = self.http_hook.run( '/cryoem-data/lgbk/%s/ws/register_file' % (self.experiment,), data=json.dumps(files, default=dtAsZ), headers={'content-type': 'application/json'} )
            if r.status_code in (403,404,500):
                raise Exception("could not register file %s on run %s for experiment %s: %s" % (self.file_info, self.run, self.experiment, r.text,))
        except Exception as e:
            LOG.warn(" %s" % (e,))
        return True

class PipelineRegisterRunOperator(BaseOperator):
    """ register the run information into the logbook """
    template_fields = ('experiment','run')

    def __init__(self,http_hook,experiment,run,imaging_method='single_particle_analysis',run_type=None,*args,**kwargs):
        super(PipelineRegisterRunOperator,self).__init__(*args, **kwargs)
        self.experiment = experiment
        self.run = run
        self.http_hook = http_hook
        self.imaging_method = imaging_method
        self.run_type = run_type

    def execute(self, context):

        # start time
        data = {}
        run_start = None
        #LOG.warn("METHOD: %s" % (self.imaging_method,))
        align_xcom = context['ti'].xcom_pull( task_ids='align', key='data' )
        for item in align_xcom['pre-pipeline']:
            if item['task'] in ['micrograph',]:
                run_start = item['files'][0]['create_timestamp']
                data['imaging_time'] = run_start

        for item in align_xcom[self.imaging_method]:
            if item['task'] in ['align_data', 'ctf_align_data']:
                for k,v in item['data'].items():
                    data['%s_%s' % ('aligned', k )] = v

        for item in context['ti'].xcom_pull( task_ids='sum', key='data' )[self.imaging_method]:
            if item['task'] in ['ctf_summed_data',]:
                for k,v in item['data'].items():
                    data['%s_%s' % ('unaligned', k )] = v

        previews_xcom = context['ti'].xcom_pull( task_ids='previews', key='data' )
        data['preview'] = previews_xcom[self.imaging_method][0]['files'][0]['path']
        run_end = previews_xcom[self.imaging_method][0]['files'][0]['create_timestamp']
        #try:
        #    for item in context['task_instance'].xcom_pull( task_ids='align', key='data' )['pre-pipeline']:
        #        if item['task'] in ['micrograph']:
        #            data_time = item['files'][0]['create_timestamp']
        #            #LOG.warn("DATA_TIME: %s", data_time.isoformat())
        #            data['imaging_time'] = data_time.isoformat() + 'Z'
        #except Exception as e:
        #    pass

        LOG.warn("RUN: %s (%s->%s)\tDATA: %s" % (self.run,run_start,run_end,data) )

        # create run
        self.http_hook.method = 'POST'
        try:
            LOG.info(f"creating run type {self.run_type}: {self.run}...")
            p = { 'run_num': self.run }
            if self.run_type:
                p['run_type'] = self.run_type
            r = self.http_hook.run( f'/cryoem-data/run_control/{self.experiment}/ws/start_run', params=p, data=json.dumps( {'start_time': run_start }, default=dtAsZ ), headers={'content-type': 'application/json'} )

            #d = {'start_time': run_start }
            #if self.run_type:
            #    d['run_type'] = self.run_type
            #r = self.http_hook.run( '/cryoem-data/run_control/%s/ws/start_run?run_num=%s' % (self.experiment, self.run ), data=json.dumps( d, default=dtAsZ ), headers={'content-type': 'application/json'} )
        except:
            pass
            # if r.status_code in (403,404):
            #     logging.error(" could not register run %s for experiment %s: %s" % (self.run, self.experiment, r.text,))
            #     return False
            # elif r.status_code == 500:
            #     LOG.info("run already exists...")

        # send params
        LOG.info("setting run %s params %s..." % (self.run,data))
        r = self.http_hook.run( '/cryoem-data/run_control/%s/ws/add_run_params?run_num=%s' % (self.experiment,self.run), data=json.dumps(data,default=dtAsZ), headers={'content-type': 'application/json'} )
        if r.status_code in (403,404,500):
            logging.error(" could not register run params on run %s for experiment %s: %s" % (self.run, self.experiment, r.text,))
            return False
        # end
        LOG.info("closing run %s..." % (self.run,))
        r = self.http_hook.run( '/cryoem-data/run_control/%s/ws/end_run?run_num=%s' % (self.experiment, self.run ), data=json.dumps( {'start_time': run_end }, default=dtAsZ ), headers={'content-type': 'application/json'} )
        if r.status_code in (403,404,500):
            logging.error(" could not register run %s for experiment %s: %s" % (self.run, self.experiment, r.text,))
            return False


        return True


class CryoEMPlugin(AirflowPlugin):
    name = 'cryoem_plugin'
    operators = [EnsureConfigurationExistsSensor,LogbookConfigurationSensor,LogbookCreateRunOperator,LogbookRegisterFileOperator,LogbookRegisterRunParamsOperator,MultiLogbookRegisterFileOperator,PipelineRegisterFilesOperator,PipelineRegisterRunOperator]
