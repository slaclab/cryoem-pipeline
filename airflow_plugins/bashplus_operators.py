from airflow.plugins_manager import AirflowPlugin

from airflow.operators.bash_operator import BashOperator
from airflow.exceptions import AirflowException, AirflowSkipException

from tempfile import TemporaryDirectory, gettempdir
from airflow.utils.operator_helpers import context_to_airflow_vars
import os
import signal
from subprocess import PIPE, STDOUT, Popen

from datetime import datetime
import yaml
#import re

import logging
LOG = logging.getLogger(__name__)

class BashPlusOperator(BashOperator):
    ui_color = '#006699'
    template_fields = ('bash_command', 'dry_run' )
    def __init__(self, dry_run=False, *args,**kwargs):
        self.start_time = datetime.now()
        super( BashPlusOperator, self ).__init__( *args, **kwargs )
    def execute(self, context):
        env = self.env
        if env is None:
            env = os.environ.copy()

        airflow_context_vars = context_to_airflow_vars(context, in_env_var_format=True)
        env.update(airflow_context_vars)

        parse = {}
        with TemporaryDirectory(prefix='airflowtmp') as tmp_dir:

            def pre_exec():
                # Restore default signal disposition and invoke setsid
                for sig in ('SIGPIPE', 'SIGXFZ', 'SIGXFSZ'):
                    if hasattr(signal, sig):
                        signal.signal(getattr(signal, sig), signal.SIG_DFL)
                os.setsid()

            self.log.info('Running command:\n%s', self.bash_command)
            for l in self.bash_command.split('\n'):
                # self.log.info("L: %s" % (l,))
                if l.startswith('#'):
                    try:
                        k,v = l.split()
                        k = k[1:].lower()
                        parse[k] = v
                    except:
                        self.log.warn("Could not parse %s" % (l,))
            self.log.info('parse: %s' % (parse,))

            self.sub_process = Popen(  # pylint: disable=subprocess-popen-preexec-fn
                ['bash', "-c", self.bash_command],
                stdout=PIPE,
                stderr=STDOUT,
                cwd=tmp_dir,
                env=env,
                preexec_fn=pre_exec)

            # write stdout to parse['out'] also 
            self.log.info('Output:')
            out = None
            try:
                if 'out' in parse:
                    out = open( parse['out'], 'a' )
                    out.write('---\n')
                    out.write(self.bash_command)
                    out.write('---\n\n')
            except Exception as e:
                self.log.warn("Error writing out file %s: %s" % (parse['out'], e))
                     

            for raw_line in iter(self.sub_process.stdout.readline, b''):
                l = raw_line.decode(self.output_encoding).rstrip()
                self.log.info("%s", l)
                out.write('%s\n' % (l,))

            self.sub_process.wait()
            if out:
                out.close()

            # parse remaining
            for k,v in parse.items():
                if not k in ( 'out', ):
                    try:
                        with open( v, 'r' ) as f:
                            d = yaml.safe_load(f)
                            context['task_instance'].xcom_push( key=k, value=d )
                    except:
                        self.log.error('Error parsing "%s" output in file %s' % (k,v))
            self.log.info('Command exited with return code %s', self.sub_process.returncode)
            if self.sub_process.returncode != 0:
                raise AirflowException('Bash command failed. The command returned a non-zero exit code.')

        self.end_time = datetime.now()
        return {
            'started_at': self.start_time,
            'finished_at': self.end_time,
            'duration': (self.end_time - self.start_time).total_seconds(),
            'host': os.uname().nodename,
        }

class BashplusPlugin(AirflowPlugin):
    name = 'bashplus_plugin'
    operators = [BashPlusOperator,]
