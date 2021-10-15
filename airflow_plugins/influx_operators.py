
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults

from airflow.models import BaseOperator
from airflow.operators.python_operator import PythonOperator, ShortCircuitOperator

from collections import defaultdict, MutableMapping
from ast import literal_eval
from datetime import datetime, timedelta
from dateutil import parser, tz
import pytz
from pytz import timezone
import re
import os

# import ast
import influxdb

import logging
LOG = logging.getLogger(__name__)



# from https://stackoverflow.com/questions/6027558/flatten-nested-python-dictionaries-compressing-keys
def flatten(d, parent_key='', sep='.'):
    """ flatten the dict d so that we can export as key/value pairs """
    items = []
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, MutableMapping):
            items.extend(flatten(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)

def dummy(*args,**kwargs):
    pass

class InfluxOperator(PythonOperator):
    ui_color = '#4bcf9a'
    template_fields = ('experiment',)
    def __init__(self,host='localhost',port=8086,user='root',password='root',db='cryoem',measurement='microscope_image',experiment=None,timezone='America/Los_Angeles',ssl=False,verify_ssl=False,*args,**kwargs):
        super(InfluxOperator,self).__init__(python_callable=dummy,*args,**kwargs)
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.db = db
        self.measurement = measurement
        self.experiment = experiment
        self.timezone = timezone
        self.ssl = ssl
        self.verify_ssl = verify_ssl
    def execute(self, context):
        """Push the parameter key-value pairs to the elogbook"""
        about = {}
        data = {} 
        dt, about, data = self.process(context)
        client = influxdb.InfluxDBClient( self.host, self.port, self.user, self.password, self.db, ssl=self.ssl, verify_ssl=self.verify_ssl )
        #LOG.info("DB: %s:%s/%s (%s %s)" % (self.host, self.port, self.db, self.user, self.password) )
        #LOG.info( '%s @ %s: \n%s\n%s' % (self.measurement,dt.strftime('%s'),about,data) ) )
        client.create_database(self.measurement)
        if self.experiment:
           about['experiment'] = self.experiment
        LOG.info('writing datapoint at %s to %s: tag %s fields %s' % (dt, self.measurement, about, data))
        client.write_points([{
            "measurement": self.measurement,
            "tags": about,
            "fields": data,
            "time": dt,
        }])
        return
    def process(self, context):
        return NotImplementedError('not implemented here')

class Xcom2InfluxOperator(InfluxOperator):
    def __init__(self,xcom_task_id=None,xcom_key='return_value',*args,**kwargs):
        super(Xcom2InfluxOperator,self).__init__(*args,**kwargs)
        self.xcom_task_id = xcom_task_id
        self.xcom_key = xcom_key

class FeiEpu2InfluxOperator(Xcom2InfluxOperator):
    def process(self, context):
        LOG.info("CONTEXT: %s" % (context,))
        d = context['ti'].xcom_pull( task_ids=self.xcom_task_id, key=self.xcom_key )['MicroscopeImage']
        dt = parser.parse( d['microscopeData']['acquisition']['acquisitionDateTime'] )
        dd = flatten(d, sep='_')
        about = {}
        data = {}
        for k,v in dd.items():
            # ignore these entries
            LOG.info("k=%s, v=%s" % (k,v))
            if k in ( 'microscopeData_acquisition_acquisitionDateTime', 'CustomData_FindFoilHoleCenterResults_@type', 'uniqueID', 'microscopeData_core_Guid' ):
                continue
            # force about
            elif k in ( 'microscopeData_instrument_InstrumentID', ):
                v = '%s' % v
            if isinstance( v, (str,bool) ) or v == None:
                # LOG.info("  about")
                vv = "'%s'" % v if isinstance(v,str) and ' ' in v else v
                about[k] = vv
            else:
                # LOG.info("  data %s" % v)
                data[k] = float(v)
        return dt, about, data


def parse_dt_timezone( dt, tz='America/Los_Angeles' ):
    # convert to UTC
    def is_dst(tz):
        now = pytz.utc.localize(datetime.utcnow())
        return now.astimezone(tz).dst() != timedelta(0)
    host_tz = pytz.timezone( tz )
    if dt.tzinfo and str(dt.tzinfo.utcoffset(dt)) == '0:00:00':
       return dt
       # print( "OFFSET: %s", dt.tzinfo.utcoffset(dt) )
    return host_tz.normalize( host_tz.localize( dt, is_dst=is_dst(host_tz) ) ).astimezone( pytz.utc )
    

class LSFJob2InfluxOperator(Xcom2InfluxOperator):
    def __init__(self, measurement='preprocessing', job_name='lsf', *args, **kwargs):
        super(LSFJob2InfluxOperator,self).__init__(*args,**kwargs)
        self.job_name = job_name
        self.measurement = measurement

    def process(self, context):
        d = context['ti'].xcom_pull( task_ids=self.xcom_task_id, key=self.xcom_key )
        about = {
            'job_name': self.job_name,
            'experiment': self.experiment,
            'host': d['host'] if 'host' in d else 'influxdb01.slac.stanford.edu',
        }
        #LOG.info("ABOUT: %s, DATA: %s" % (about,d,))
        # use more accurate duration if available
        runtime = None
        if 'runtime' in d:
            try:
                runtime = d['runtime'].total_seconds()
            except:
                pass
        if runtime == None:
            m = re.search( '(?P<seconds>\d+\.\d+) seconds', d['duration'] )
            if m:
                this = m.groupdict()
                runtime = float(this['seconds'])
        data = {
            'runtime': runtime,
            # 'duration': d['duration'],
        }
        if 'inertia' in d:
            data['inertia'] = d['inertia'].total_seconds()
        LOG.info("OUT: %s (%s)" % (data,d))
        dt = parse_dt_timezone( d['submitted_at'], tz=self.timezone )
        return dt, about, data

class GenericInfluxOperator( InfluxOperator ):
    template_fields = ('experiment','measurement','tags','tags2','tags3','fields','dt')
    def __init__(self, experiment=None, measurement='database_name', dt=None, timezone='America/Los_Angeles', tags=None, tags2=None, tags3={}, fields={}, *args, **kwargs):
        self.experiment = experiment
        self.measurement = measurement
        self.dt = dt
        self.tags = tags
        self.tags2 = tags2
        self.tags3 = tags3
        self.fields = fields
        super( GenericInfluxOperator, self ).__init__( experiment=experiment, measurement=measurement, *args, **kwargs )
        self.timezone = timezone
        #LOG.info("TZ: %s" % (self.timezone,))
    
    def process(self,context):
        def lit_eval(a):
            #LOG.info("lit_eval %s: %s" % (type(a),a,))
            #if re.search( r"'\w+'\: inf,", a ):
            #    LOG.warn("FOUND invalid")
            if isinstance( a, str ):
                if "'resolution': inf, " in a:
                    a = a.replace( "'resolution': inf, ", "" )
                    LOG.warn("INFINITY %s" % (a,) )
                return literal_eval( a )
            elif a == None:
                return {}
            return a

        dt = self.dt
        if dt == None:
            dt = pytz.utc.localize(datetime.utcnow())
        elif isinstance( dt, str ):
            from_stat = False
            try:
                dt = parser.parse( self.dt )
            except Exception as e:
                # parse datetime from filename
                LOG.info('parsing datetime %s' % (self.dt,))
                dt = None
                for r, format in ( \
                        ('(?P<date_time>\d{8}_\d{4})', '%Y%m%d_%H%M'), \
                        ('\d{4}\_(?P<date_time>\w+\d+_\d\d\.\d\d\.\d\d)', '%b%d_%H.%M.%S'), \
                        #('\_(?P<date_time>\w+\d+_\d\d\.\d\d\.\d\d)', '%b%d_%H.%M.%S'), \
                    ):
                    LOG.info(" trying " + format )
                    m = re.findall( r, self.dt )
                    if len(m):
                        try:
                            dt = datetime.strptime( m[-1], format )
                        except Exception as e:
                            pass
                if dt == None:
                    if os.path.isfile(self.dt):
                        dt = datetime.fromtimestamp( os.stat( self.dt ).st_mtime ).astimezone( timezone('UTC') )
                        from_stat = True
                        LOG.info("parsed from file stat: %s" % (dt,))
                    else:
                        LOG.error('could not parse timestamp %s', ( self.dt) )
                        raise e
        #LOG.info("parsed dt - timezone: %s: %s" % (self.timezone, dt,))
        if dt.year == 1900:
            dt = dt.replace( year=datetime.utcnow().year ) # set timezone
        if not from_stat:
            dt = parse_dt_timezone( dt, tz=self.timezone )
        #LOG.info("final dt: %s" % (dt,))
        #LOG.info("tags: %s, tags2: %s, tags3: %s" % (self.tags,self.tags2,self.tags3))
        about = { **lit_eval( self.tags ), **lit_eval(self.tags2), **lit_eval(self.tags3) } 
        data = lit_eval( self.fields )
        LOG.info("SENDING: %s, %s, %s" % (dt, about, data ))
        return dt, about, data
    
class ParentStatsInfluxOperator(PythonOperator):
    ui_color = '#4bcf9a'
    template_fields = ('experiment',)
    def __init__(self,host='localhost',port=8086,user='root',password='root',db='cryoem',measurement='microscope_image',experiment=None,timezone='America/Los_Angeles',*args,**kwargs):
        super(ParentStatsInfluxOperator,self).__init__(python_callable=dummy,*args,**kwargs)
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.db = db
        self.measurement = measurement
        self.experiment = experiment
        self.timezone = timezone
    def execute(self, context):
        """Push the parameter key-value pairs to the elogbook"""
        about = {}
        data = {}
        client = influxdb.InfluxDBClient( self.host, self.port, self.user, self.password, self.db )
        client.create_database(self.measurement)
        failed = []
        for task in self.upstream_task_ids:
          LOG.info("parent: %s" % (task,))
          try:
              dt, about, data = self.get(context, task)
              client.write_points([{
                  "measurement": self.measurement,
                  "tags": about,
                  "fields": data,
                  "time": dt,
              }])
              LOG.info('  wrote datapoint at %s to %s: tag %s fields %s' % (dt, self.measurement, about, data))
              failed.append( False )
          except Exception as e:
              LOG.warn("%s" % (e,))
              failed.append( True )
        return False if True in failed else True
    def get(self, context, task, key='return_value' ):
        raise NotImplementedError('not implemented here')

class MultiLSFJob2InfluxOperator(ParentStatsInfluxOperator):
    def __init__(self, measurement='preprocessing', job_name='lsf', *args, **kwargs):
        super(MultiLSFJob2InfluxOperator,self).__init__(*args,**kwargs)
        self.measurement = measurement
        self.name_mapping = {
            'new_gainref': 'convert_gainref',
            'convert_summed_ctf_preview': 'summed_preview',
            'convert_aligned_preview': 'aligned_preview',
            'align': 'align_stack',
            'convert_aligned_ctf_preview': 'ctf_preview',
        }
    def get(self, context, task, key='return_value' ):
        d = context['ti'].xcom_pull( task_ids=task, key=key )
        LOG.info(" task: %s, key: %s, d: %s" % (task, key, d,))
        job_name = self.name_mapping[task] if task in self.name_mapping else task
        about = {
            'job_name': job_name,
            'experiment': self.experiment,
            'host': d['host'] if 'host' in d else 'influxdb01.slac.stanford.edu',
        }
        runtime = None
        if 'runtime' in d:
            try:
                runtime = d['runtime'].total_seconds()
            except:
                pass
        if runtime == None:
            m = re.search( '(?P<seconds>\d+\.\d+) seconds', d['duration'] )
            if m:
                this = m.groupdict()
                runtime = float(this['seconds'])
        data = {
            'runtime': runtime,
            # 'duration': d['duration'],
        }
        if 'inertia' in d:
            data['inertia'] = d['inertia'].total_seconds()
        #LOG.info("OUT: %s (%s)" % (data,d))
        dt = parse_dt_timezone( d['submitted_at'], tz=self.timezone )
        return dt, about, data

class PipelineInfluxOperator(PythonOperator):
    ui_color = '#4bcf9a'
    template_fields = ('experiment','run','microscope')
    def __init__(self,host='localhost',port=8086,user='root',password='root',db='cryoem',microscope=None,measurement='microscope_image',experiment=None,run=None,timezone='America/Los_Angeles',ssl=True,verify_ssl=False,*args,**kwargs):
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
        self.ssl = ssl
        self.verify_ssl = verify_ssl

class PipelineStatsOperator(PipelineInfluxOperator):
    def execute(self, context):
        """Push the parameter key-value pairs to the elogbook"""
        data = []
        client = influxdb.InfluxDBClient( self.host, self.port, self.user, self.password, self.db, ssl=self.ssl, verify_ssl=self.verify_ssl )
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
        client = influxdb.InfluxDBClient( self.host, self.port, self.user, self.password, self.db, ssl=self.ssl, verify_ssl=self.verify_ssl )
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


class InfluxPlugin(AirflowPlugin):
    name = 'influx_plugin'
    operators = [InfluxOperator,FeiEpu2InfluxOperator,LSFJob2InfluxOperator,GenericInfluxOperator,Xcom2InfluxOperator,ParentStatsInfluxOperator,MultiLSFJob2InfluxOperator,PipelineInfluxOperator,PipelineStatsOperator,PipelineCtfOperator]

